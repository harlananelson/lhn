"""
lhn.bootstrap
=============

Single-call environment setup for pipeline notebooks on HealtheDataLab (HDL).

Replaces the ~50-line setup block (Cells 4--9) that every pipeline notebook
(053--067) used to carry. Puts the logic in one tested module so bug fixes
deploy once instead of requiring coordinated edits across ~15 notebooks.

Usage in a notebook
-------------------

.. code-block:: python

    from lhn.bootstrap import pipeline_setup
    ctx = pipeline_setup('000-control.yaml')
    r, e, d = ctx.r, ctx.e, ctx.d
    spark, F, Window = ctx.spark, ctx.F, ctx.Window
    resource = ctx.resource

The returned :class:`types.SimpleNamespace` carries every name the setup
block used to surface into locals via ``locals().update(...)`` -- plus
``user_path``, ``project_path``, and ``resource`` for direct access.

Prerequisite
------------

``lhn`` (and ideally ``spark_config_mapper`` and ``scd_phenotyping``) must
already be importable. With ``pip install --user -e .`` on each repo (run
by ``fetchupdate.sh``), they are. As a defensive fallback,
:func:`pipeline_setup` will also prepend the three repo roots to
``sys.path`` if they happen to be present under ``user_path`` -- that way
the bootstrap still works on machines where the editable installs haven't
been redone yet after a clean clone.

Design notes
------------

- ``Resources`` is initialized with an absolute ``base_path`` (the project
  directory). That eliminates the old ``os.chdir(project_path)`` global
  side effect: two notebooks sharing a kernel no longer fight over cwd.
- ``SICKLE_CELL_DATA_PATH`` is set for the downstream R target that reads
  it. If R launches as a separate job (not a subprocess of this kernel),
  the env var doesn't propagate; that's a separate concern.
- ``PYSPARK_PYTHON`` is set but may be a no-op if the SparkSession is
  already initialized at the cluster level. Kept for the (rare) case of a
  locally-created session.
- Cache clearing purges ``sys.modules`` entries for the three pipeline
  packages. Editable installs handle ``.pyc`` staleness via source-wins
  semantics, so we no longer rmtree ``__pycache__`` directories here; if
  ``fetchupdate.sh`` pulls new code mid-kernel, the sys.modules purge
  forces a re-import on the next ``from lhn...``.
"""

from __future__ import annotations

import getpass
import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

_PIPELINE_PKGS = ('scd_phenotyping', 'lhn', 'spark_config_mapper')


def discover_user_path() -> str:
    """Locate the HDL user data directory.

    Resolution order:

    1. ``$HDL_USER_PATH`` environment variable (if it points to an existing dir).
    2. ``~/.hdl_user_path`` breadcrumb file (fetchupdate.sh can write this).
    3. Filesystem probe: ``~/work/Users/<user>``, then ``~/work/IUH/<user>``.

    Raises
    ------
    RuntimeError
        If no candidate resolves to an existing directory.
    """
    env = os.environ.get('HDL_USER_PATH')
    if env and os.path.isdir(env):
        return env

    breadcrumb = Path.home() / '.hdl_user_path'
    if breadcrumb.exists():
        candidate = breadcrumb.read_text().strip()
        if candidate and os.path.isdir(candidate):
            return candidate

    user = getpass.getuser()
    candidates = [
        os.path.join(Path.home(), 'work', 'Users', user),
        os.path.join(Path.home(), 'work', 'IUH', user),
    ]
    for c in candidates:
        if os.path.isdir(c):
            return c

    raise RuntimeError(
        f"Could not locate HDL user data path for '{user}'. "
        f"Checked $HDL_USER_PATH, ~/.hdl_user_path, and {candidates}. "
        f"Run fetchupdate.sh at least once, or export $HDL_USER_PATH."
    )


def _prepend_sys_path(path: str) -> None:
    """Prepend ``path`` to ``sys.path`` if it exists and isn't already there."""
    if os.path.isdir(path) and path not in sys.path:
        sys.path.insert(0, path)


def configure_sys_path(
    user_path: str,
    pkgs: Iterable[str] = _PIPELINE_PKGS,
    extra_roots: Optional[Iterable[str]] = None,
) -> None:
    """Defensive ``sys.path`` setup for environments without editable installs.

    Idempotent. When the three pipeline packages are already installed via
    ``pip install -e .`` (the steady state), this is a no-op for them; the
    only effect is adding ``extra_roots`` (e.g., SCDCernerProject for
    shared_utils).
    """
    for pkg in pkgs:
        _prepend_sys_path(os.path.join(user_path, pkg))
    _prepend_sys_path(user_path)
    for root in (extra_roots or ()):
        _prepend_sys_path(root)


def clear_module_cache(pkgs: Iterable[str] = _PIPELINE_PKGS) -> int:
    """Purge ``sys.modules`` entries for the given top-level packages.

    Needed when ``fetchupdate.sh`` pulls new code *while a Jupyter kernel
    is running*: without this, cached modules in ``sys.modules`` shadow
    the freshly-pulled source until the kernel is restarted.

    Returns
    -------
    int
        Number of modules purged.
    """
    tops = set(pkgs)
    stale = [m for m in list(sys.modules) if m.split('.', 1)[0] in tops]
    for m in stale:
        sys.modules.pop(m, None)
    return len(stale)


def pipeline_setup(
    local_config: str = '000-control.yaml',
    global_config: str = 'configuration/config-global.yaml',
    schemaTag_config: str = 'configuration/config-RWD.yaml',
    project_dir: str = 'Projects/SickleCell_AI',
    shared_utils_dir: str = 'Projects/SCDCernerProject',
    data_path_env_var: str = 'SICKLE_CELL_DATA_PATH',
    debug: bool = True,
    widen_notebook: bool = True,
) -> SimpleNamespace:
    """One-call pipeline environment setup.

    Performs, in order:

    1. Set env vars (``PYTHONDONTWRITEBYTECODE``, ``PYSPARK_PYTHON``,
       ``<data_path_env_var>``).
    2. Discover ``user_path`` (see :func:`discover_user_path`).
    3. Defensive ``sys.path`` setup for pipeline repos.
    4. Purge ``sys.modules`` entries for pipeline packages so freshly-pulled
       code takes effect without a kernel restart.
    5. Import ``lhn.header`` (``spark``, ``F``, ``Window``) and ``Resources``.
    6. Initialize ``Resources`` with ``base_path=project_path`` (no chdir).
    7. Build a :class:`~types.SimpleNamespace` from ``resource.load_into_local()``.
    8. Assert all source Items loaded successfully
       (``status == 'PROCESSED'``); raise with a list of failures otherwise.

    Returns
    -------
    types.SimpleNamespace
        Attributes include ``user_path``, ``project_path``, ``resource``,
        ``spark``, ``F``, ``Window``, and every name from
        ``resource.load_into_local()`` (``r``, ``e``, ``d``, ``RWDSchema``,
        ``projectSchema``, ``dataLoc``, ...).

    Raises
    ------
    RuntimeError
        If the user path or project directory cannot be found, or if any
        source Item fails to load/process.
    """
    # 1. Env vars
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    os.environ.setdefault("PYSPARK_PYTHON", "python3")
    sys.dont_write_bytecode = True

    # 2. Path discovery
    user_path = discover_user_path()
    project_path = os.path.join(user_path, project_dir)
    if not os.path.isdir(project_path):
        raise RuntimeError(
            f"Project directory not found: {project_path}. "
            f"Has the project repo been cloned?"
        )
    os.environ[data_path_env_var] = user_path

    # 3. Defensive sys.path (no-op when editable installs are in place)
    configure_sys_path(
        user_path,
        extra_roots=[os.path.join(user_path, shared_utils_dir)],
    )

    # 4. Module cache purge (handles running-kernel + fresh-pull case)
    purged = clear_module_cache()
    if debug and purged:
        logger.info("Purged %d cached pipeline modules.", purged)

    # 5. Pipeline imports (after cache purge)
    from lhn.header import spark, F, Window  # noqa: E402
    from lhn import Resources                # noqa: E402

    # 6. Resources init -- base_path replaces os.chdir
    resource = Resources(
        local_config=local_config,
        global_config=global_config,
        schemaTag_config=schemaTag_config,
        base_path=project_path,
        debug=debug,
    )

    # 7. Build namespace from resource.load_into_local().
    # load_into_local() already returns 'resource' (plus r/e/d/schemas/...),
    # so start from that dict and let explicit kwargs override any overlap
    # (also avoids TypeError on duplicate keys).
    ns_dict = dict(resource.load_into_local())
    ns_dict.update(
        user_path=user_path,
        project_path=project_path,
        spark=spark,
        F=F,
        Window=Window,
    )
    ns = SimpleNamespace(**ns_dict)

    # 8. Status assertion -- fail loud on any silent ITEM_FAILED
    r = getattr(ns, 'r', None)
    if r is not None:
        print(r.report_str())
        failed = [
            item for item in r.values()
            if getattr(item, 'status', None) != 'PROCESSED'
        ]
        if failed:
            names = [getattr(item, 'name', repr(item)) for item in failed]
            raise RuntimeError(
                f"{len(failed)} source Item(s) failed to load/process: "
                f"{names}. See report above."
            )

    # 9. Notebook display
    if widen_notebook:
        try:
            from IPython.display import display, HTML
            display(HTML("<style>.container { width:99% !important; }</style>"))
        except ImportError:
            pass  # not in a notebook

    if debug:
        schema = getattr(resource, 'projectSchema', '?')
        try:
            import lhn as _lhn
            ver = getattr(_lhn, '__version__', 'unknown')
        except ImportError:
            ver = 'unknown'
        logger.info("pipeline_setup complete. lhn=%s, schema=%s", ver, schema)

    return ns
