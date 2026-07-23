"""
lhn.make_pipeline
=================

Make / targets-style runner for ``projectTables`` in ``000-control.yaml``.

Goals
-----
1. **YAML is the makefile** — nodes, ``inputs`` edges, join policy, entry order.
2. **Skip when fresh** — config fingerprint + upstream fingerprints + table exists.
3. **Optional notebook splice** — tagged markdown cell with the build report in the
   section's ``.ipynb`` so results appear without re-executing every Spark cell.

Usage
-----
.. code-block:: bash

    # From the project directory (Spark / HDL kernel available):
    python -m lhn.make --config 000-control.yaml --section 014
    python -m lhn.make --section 014 --force personYearPregnancyDx
    python -m lhn.make --section 014 --notebook 014-Dx-Med-Observability.ipynb

.. code-block:: python

    from lhn.bootstrap import pipeline_setup
    from lhn.make_pipeline import make_run
    ctx = pipeline_setup('000-control.yaml')
    report = make_run(ctx, section='014', notebook_path='014-….ipynb')

Freshness
---------
State: ``{project}/.lhn-make/manifest.json``. A node is **skipped** when:

* its Hive ``location`` loads successfully, and
* its fingerprint (hash of method + policy + inputs + upstream hashes)
  matches the manifest.

``--force NAME`` rebuilds that node. ``--force-all`` rebuilds every selected node.

Recipes automated
-----------------
* ``write_index_table`` — requires ``inputs.inTable``
* ``entityExtract`` — ``inputs.elementList`` + ``inputs.entitySource``, or
  multi-attach ``inputs.spine`` + ``inputs.features``
* ``create_extract`` — ``inputs.elementList`` + ``inputs.elementListSource``
* ``load_csv_as_df`` / ``dict2pyspark``

``manual`` and unknown methods are left to the notebook (action=unsupported).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import yaml

logger = logging.getLogger(__name__)

# Non-build metadata: excluded from fingerprints. Everything else on a node
# counts (blacklist > whitelist — wrong skip is worse than a false rebuild).
_FINGERPRINT_BLACKLIST = frozenset({
    'label', 'purpose', 'grain', 'stage', 'description', 'notes', 'comment',
})

_SECTION_RE = re.compile(
    r'^#\s*(?:NOTEBOOK\s+)?(\d{3})\b|^#\s*(\d{3})\s*[—\-–]',
    re.IGNORECASE,
)

_MAKEABLE = frozenset({
    'write_index_table', 'entityExtract', 'create_extract',
    'load_csv_as_df', 'dict2pyspark',
})

# Allowed inputs keys per method (typos fail loud).
_INPUTS_ALLOWED = {
    'write_index_table': frozenset({'inTable', 'source'}),
    'entityExtract': frozenset({
        'elementList', 'entitySource', 'spine', 'features', 'cohort',
    }),
    'create_extract': frozenset({'elementList', 'elementListSource'}),
    'load_csv_as_df': frozenset(),
    'dict2pyspark': frozenset(),
}

# ---------------------------------------------------------------------------
# YAML + sections
# ---------------------------------------------------------------------------

def load_project_tables(
    config_path: str,
) -> Tuple[Dict[str, dict], List[str], Dict[str, List[str]], dict]:
    """Load projectTables, key order, comment sections, and full YAML root.

    Returns
    -------
    tables, order, sections, root_config
    """
    path = Path(config_path)
    text = path.read_text(encoding='utf-8')
    sections: Dict[str, List[str]] = {}
    current_section: Optional[str] = None
    key_re = re.compile(r'^  ([A-Za-z_][A-Za-z0-9_]*)\s*:')

    in_project_tables = False
    for line in text.splitlines():
        if re.match(r'^projectTables\s*:', line):
            in_project_tables = True
            current_section = None
            continue
        if in_project_tables and re.match(r'^[A-Za-z_#]', line):
            if line.startswith('#') or line.startswith(' '):
                pass
            else:
                in_project_tables = False
                current_section = None
                continue
        if not in_project_tables:
            continue
        stripped = line.strip()
        if stripped.startswith('#'):
            sm = _SECTION_RE.match(stripped)
            if sm:
                current_section = sm.group(1) or sm.group(2)
                sections.setdefault(current_section, [])
            continue
        km = key_re.match(line)
        if km and current_section:
            sections[current_section].append(km.group(1))

    data = yaml.safe_load(text) or {}
    tables = data.get('projectTables') or {}
    if not isinstance(tables, dict):
        raise ValueError('projectTables must be a mapping')
    return tables, list(tables.keys()), sections, data


def _stable_hash(obj: Any) -> str:
    payload = json.dumps(obj, sort_keys=True, default=str, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]


def global_source_fingerprint(root_config: dict) -> str:
    """Hash schemas / data_version so warehouse refreshes invalidate all nodes.

    Include any of: top-level ``schemas``, ``analysis.data_version``,
    ``analysis.hist_start`` / ``hist_stop`` (study window changes matter).
    Document: bump ``analysis.data_version`` after a schema refresh if Hive
    mtimes are unavailable.
    """
    analysis = root_config.get('analysis') or {}
    token = {
        'schemas': root_config.get('schemas'),
        'data_version': analysis.get('data_version') or root_config.get('data_version'),
        'hist_start': analysis.get('hist_start'),
        'hist_stop': analysis.get('hist_stop'),
    }
    return _stable_hash(token)


def node_config_fingerprint(cfg: dict) -> str:
    """Hash entire node cfg minus non-build metadata (blacklist)."""
    if not isinstance(cfg, dict):
        return _stable_hash(cfg)
    subset = {k: v for k, v in cfg.items() if k not in _FINGERPRINT_BLACKLIST}
    return _stable_hash(subset)


def deps_from_inputs(cfg: dict) -> List[str]:
    """ExtractItem names under inputs (skip r./d. warehouse refs)."""
    inputs = cfg.get('inputs') or {}
    if not isinstance(inputs, dict):
        return []
    deps: List[str] = []

    def add(val: Any) -> None:
        if val is None:
            return
        if isinstance(val, list):
            for v in val:
                add(v)
            return
        if not isinstance(val, str):
            return
        if val.startswith(('r.', 'd.', 'spark.')):
            return
        name = val[2:] if val.startswith('e.') else val
        if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name):
            deps.append(name)

    for v in inputs.values():
        add(v)
    seen: Set[str] = set()
    out: List[str] = []
    for d in deps:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


# ---------------------------------------------------------------------------
# Report + manifest
# ---------------------------------------------------------------------------

@dataclass
class NodeResult:
    name: str
    action: str  # built | skipped | failed | unsupported
    reason: str = ''
    fingerprint: str = ''
    location: str = ''
    n_rows: Optional[int] = None
    elapsed_s: Optional[float] = None
    error: str = ''


@dataclass
class MakeReport:
    started: str
    finished: str = ''
    section: Optional[str] = None
    results: List[NodeResult] = field(default_factory=list)
    manifest_path: str = ''

    def to_markdown(self) -> str:
        lines = [
            '## lhn make report',
            '',
            f'- started: `{self.started}`',
            f'- finished: `{self.finished}`',
            f'- section: `{self.section or "all"}`',
            f'- manifest: `{self.manifest_path}`',
            '',
            '| node | action | rows | s | reason |',
            '|------|--------|------|---|--------|',
        ]
        for r in self.results:
            rows = '' if r.n_rows is None else f'{r.n_rows:,}'
            secs = '' if r.elapsed_s is None else f'{r.elapsed_s:.1f}'
            reason = (r.reason or r.error or '').replace('|', '\\|')
            lines.append(
                f'| `{r.name}` | **{r.action}** | {rows} | {secs} | {reason} |'
            )
        # actions: built | skipped | plan | failed | unsupported
        lines.append('')
        lines.append(
            '*Generated by `lhn.make_pipeline`. **skipped** = fresh '
            '(config fingerprint + upstream fingerprints + table exists).*'
        )
        return '\n'.join(lines) + '\n'


def _manifest_path(project_path: str) -> Path:
    return Path(project_path) / '.lhn-make' / 'manifest.json'


def load_manifest(project_path: str) -> dict:
    p = _manifest_path(project_path)
    if not p.is_file():
        return {'nodes': {}}
    try:
        return json.loads(p.read_text(encoding='utf-8'))
    except Exception as exc:
        logger.warning('Could not read manifest %s: %s', p, exc)
        return {'nodes': {}}


def save_manifest(project_path: str, manifest: dict) -> Path:
    p = _manifest_path(project_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    manifest['updated'] = datetime.now(timezone.utc).isoformat()
    p.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    return p


def table_exists(spark, location: Optional[str]) -> bool:
    if not location:
        return False
    try:
        spark.table(location).limit(0).collect()
        return True
    except Exception:
        return False


def table_row_count(spark, location: Optional[str]) -> Optional[int]:
    if not location:
        return None
    try:
        return int(spark.table(location).count())
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Resolve + recipes
# ---------------------------------------------------------------------------

def resolve_ref(ref: Any, r, e, d, spark=None):
    """Resolve an inputs value to ExtractItem or DataFrame.

    Accepts:
    * ``e.name`` / ``r.name`` / ``d.name`` prefixes
    * bare ExtractItem names on ``e`` / ``r`` / ``d``
    * schema-qualified Hive tables ``schema.table`` (via ``spark.table``)
    """
    if ref is None:
        return None
    if not isinstance(ref, str):
        return ref
    s = ref.strip()
    if s.startswith('r.'):
        obj = getattr(r, s[2:], None)
        return obj.df if obj is not None and hasattr(obj, 'df') else obj
    if s.startswith('e.'):
        return getattr(e, s[2:], None)
    if s.startswith('d.'):
        obj = getattr(d, s[2:], None)
        return obj.df if obj is not None and hasattr(obj, 'df') else obj
    if hasattr(e, s):
        return getattr(e, s)
    if hasattr(r, s):
        obj = getattr(r, s)
        return obj.df if hasattr(obj, 'df') else obj
    if hasattr(d, s):
        obj = getattr(d, s)
        return obj.df if hasattr(obj, 'df') else obj
    # schema.table Hive path (create_extract elementListSource sometimes)
    if '.' in s and spark is not None:
        try:
            return spark.table(s)
        except Exception as exc:
            raise KeyError(
                f'Cannot resolve input reference {ref!r} as spark.table: {exc}'
            ) from exc
    raise KeyError(f'Cannot resolve input reference {ref!r}')


def _as_df(obj):
    if obj is None:
        return None
    if hasattr(obj, 'df') and not hasattr(obj, 'columns'):
        return obj.df
    return obj


def validate_inputs(name: str, method: str, inputs: dict) -> None:
    """Raise if inputs has unknown keys for this method."""
    if not inputs:
        return
    allowed = _INPUTS_ALLOWED.get(method)
    if allowed is None:
        return
    unknown = sorted(set(inputs.keys()) - allowed)
    if unknown:
        raise ValueError(
            f'{name}: unknown inputs keys {unknown} for method={method!r}; '
            f'allowed={sorted(allowed)}'
        )


def run_recipe(name: str, item, cfg: dict, r, e, d, spark) -> Tuple[str, Optional[int]]:
    """Execute one makeable method. Returns (note, n_rows)."""
    method = (cfg.get('method') or '').strip()
    inputs = cfg.get('inputs') or {}
    if not isinstance(inputs, dict):
        inputs = {}
    validate_inputs(name, method, inputs)

    if method == 'write_index_table':
        in_ref = inputs.get('inTable') or inputs.get('source')
        if not in_ref:
            raise ValueError(
                f'{name}: write_index_table needs inputs.inTable'
            )
        item.write_index_table(inTable=resolve_ref(in_ref, r, e, d, spark))

    elif method == 'entityExtract':
        features = inputs.get('features')
        spine = inputs.get('spine')
        cohort = None
        if inputs.get('cohort') is not None:
            cohort = resolve_ref(inputs.get('cohort'), r, e, d, spark)
        if features is not None:
            if not spine:
                raise ValueError(f'{name}: inputs.features requires inputs.spine')
            spine_df = _as_df(resolve_ref(spine, r, e, d, spark))
            feat_list = features if isinstance(features, list) else [features]
            for i, feat in enumerate(feat_list):
                el = resolve_ref(feat, r, e, d, spark)
                src = spine_df if i == 0 else item.df
                item.entityExtract(el, src, cohort=cohort if i == 0 else None)
        else:
            el = resolve_ref(inputs.get('elementList'), r, e, d, spark)
            src = resolve_ref(inputs.get('entitySource'), r, e, d, spark)
            if el is None or src is None:
                raise ValueError(
                    f'{name}: entityExtract needs elementList+entitySource '
                    f'or spine+features'
                )
            item.entityExtract(el, _as_df(src), cohort=cohort)

    elif method == 'create_extract':
        el = resolve_ref(inputs.get('elementList'), r, e, d, spark)
        src = resolve_ref(inputs.get('elementListSource'), r, e, d, spark)
        item.create_extract(elementList=el, elementListSource=_as_df(src))

    elif method == 'load_csv_as_df':
        item.load_csv_as_df()

    elif method == 'dict2pyspark':
        item.dict2pyspark()

    else:
        raise ValueError(f'{name}: unsupported method {method!r}')

    n = None
    try:
        if item.df is not None:
            n = int(item.df.count())
    except Exception:
        n = table_row_count(spark, getattr(item, 'location', None))
    return f'built via {method}', n


# ---------------------------------------------------------------------------
# Fingerprint graph
# ---------------------------------------------------------------------------

def compute_fingerprints(
    tables: Dict[str, dict],
    names: Sequence[str],
    global_token: str = '',
) -> Dict[str, str]:
    """Fingerprint each node: global source token + self + upstream fps."""
    fps: Dict[str, str] = {}

    def fp(name: str, stack: Optional[Set[str]] = None) -> str:
        if name in fps:
            return fps[name]
        stack = stack or set()
        if name in stack:
            logger.warning('make: dependency cycle involving %s', name)
            return f'cycle:{name}'
        stack = set(stack)
        stack.add(name)
        cfg = tables.get(name) or {}
        base = node_config_fingerprint(cfg)
        ups = []
        for dep in deps_from_inputs(cfg):
            if dep in tables:
                ups.append(f'{dep}:{fp(dep, stack)}')
            else:
                # external / prior-section product: still pin the name
                ups.append(f'{dep}:external')
        full = _stable_hash({
            'global': global_token,
            'self': base,
            'up': ups,
        })
        fps[name] = full
        return full

    for n in names:
        if n in tables:
            fp(n)
    return fps


def upstream_fresh(
    name: str,
    tables: Dict[str, dict],
    fps: Dict[str, str],
    nodes_m: dict,
    spark,
    e,
    failed: Set[str],
    planned: Optional[Set[str]] = None,
) -> Tuple[bool, str]:
    """True if all in-graph deps are usable.

    Dep states:
    * failed this run → hard fail
    * planned/built this run (dry-run or earlier in loop) → ok
    * makeable with matching manifest fingerprint + table exists → ok
    * makeable with **no** manifest entry but table exists → ok (adopt
      out-of-make / notebook-built product; warn)
    * makeable with fingerprint **mismatch** → hard fail (true staleness)
    * non-makeable method → table-exists only (make never owns them)
    * table missing → hard fail

    Returns (ok, reason_if_not).
    """
    planned = planned or set()
    cfg = tables.get(name) or {}
    for dep in deps_from_inputs(cfg):
        if dep in failed:
            return False, f'upstream {dep} failed'
        if dep in planned:
            continue
        if dep not in tables:
            continue
        dep_cfg = tables.get(dep) or {}
        dep_method = (dep_cfg.get('method') or 'manual').strip()
        dep_item = getattr(e, dep, None)
        dep_loc = getattr(dep_item, 'location', None) if dep_item else None
        exists = table_exists(spark, dep_loc)
        prev = nodes_m.get(dep) or {}
        dep_fp = fps.get(dep)

        # Non-makeable deps: make never wrote a manifest entry; require table only
        if dep_method not in _MAKEABLE:
            if not exists:
                return False, (
                    f'upstream {dep} (method={dep_method!r}, notebook-owned) '
                    f'table missing at {dep_loc!r}'
                )
            continue

        if not exists:
            return False, (
                f'upstream {dep} table missing at {dep_loc!r} — '
                f'run its section first or --force-all'
            )

        if not prev:
            # Adopt out-of-make product so section 014 can run after notebook 011
            logger.warning(
                'make: adopting out-of-make upstream %s (table exists, '
                'no manifest entry) — subsequent config drift will be caught',
                dep,
            )
            # Record adoption so future config drift is detected
            # (caller must not persist nodes_m when dry_run)
            nodes_m[dep] = {
                'fingerprint': dep_fp,
                'location': dep_loc,
                'adopted': True,
                'built_at': datetime.now(timezone.utc).isoformat(),
            }
            continue

        if prev.get('fingerprint') != dep_fp:
            return False, (
                f'upstream {dep} stale (fingerprint mismatch) — '
                f'run its section first or --force-all'
            )
    return True, ''

# ---------------------------------------------------------------------------
# Notebook splice
# ---------------------------------------------------------------------------

_REPORT_TAG = 'lhn-make-report'


def splice_report_into_notebook(notebook_path: str, report: MakeReport) -> None:
    """Insert or replace a markdown cell tagged ``lhn-make-report``."""
    path = Path(notebook_path)
    if not path.is_file():
        raise FileNotFoundError(notebook_path)
    nb = json.loads(path.read_text(encoding='utf-8'))
    md = report.to_markdown()
    # nbformat-compatible source: list of lines
    source_lines = [ln + '\n' for ln in md.splitlines()]
    if source_lines and not source_lines[-1].endswith('\n'):
        source_lines[-1] += '\n'

    new_cell = {
        'cell_type': 'markdown',
        'metadata': {'tags': [_REPORT_TAG]},
        'source': source_lines,
    }

    cells = nb.get('cells') or []
    replaced = False
    for i, cell in enumerate(cells):
        tags = (cell.get('metadata') or {}).get('tags') or []
        if _REPORT_TAG in tags:
            cells[i] = new_cell
            replaced = True
            break
    if not replaced:
        # After first raw/markdown title if present, else prepend
        insert_at = 0
        for i, cell in enumerate(cells[:5]):
            if cell.get('cell_type') in ('raw', 'markdown'):
                insert_at = i + 1
        cells.insert(insert_at, new_cell)
    nb['cells'] = cells
    path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + '\n', encoding='utf-8')
    logger.info('Spliced make report into %s', path)


def find_notebook_for_section(
    project_path: str, section: str,
) -> Optional[str]:
    """Heuristic: first ``{section}-*.ipynb`` under project or notebooks/hdl."""
    root = Path(project_path)
    patterns = [
        f'{section}-*.ipynb',
        f'notebooks/hdl/{section}-*.ipynb',
        f'*/{section}-*.ipynb',
    ]
    for pat in patterns:
        hits = sorted(root.glob(pat))
        if hits:
            return str(hits[0])
    return None


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def make_run(
    ctx,
    config_path: Optional[str] = None,
    section: Optional[str] = None,
    only: Optional[Sequence[str]] = None,
    force: Optional[Sequence[str]] = None,
    force_all: bool = False,
    notebook_path: Optional[str] = None,
    auto_notebook: bool = False,
    dry_run: bool = False,
) -> MakeReport:
    """Run makeable projectTables nodes with skip-if-fresh.

    Parameters
    ----------
    ctx :
        Namespace from ``pipeline_setup`` (needs ``e``, ``r``, ``d``, ``spark``,
        ``project_path``).
    config_path :
        Path to ``000-control.yaml`` (default: ``{project_path}/000-control.yaml``).
    section :
        Comment section id (e.g. ``'014'``) — only nodes under that heading.
    only :
        Explicit node name list (overrides section filter if both set, intersect).
    force :
        Node names to rebuild even if fresh.
    force_all :
        Rebuild every selected node.
    notebook_path :
        If set, splice the report into this ``.ipynb``.
    auto_notebook :
        If True and ``section`` set, find ``{section}-*.ipynb`` and splice.
    dry_run :
        Plan only (no builds); still reports skip/build decisions.
    """
    project_path = getattr(ctx, 'project_path', None) or str(Path.cwd())
    config_path = config_path or str(Path(project_path) / '000-control.yaml')
    e, r, d = ctx.e, ctx.r, ctx.d
    spark = ctx.spark

    tables, order, sections, root_cfg = load_project_tables(config_path)
    gtoken = global_source_fingerprint(root_cfg)
    if section:
        section = str(section).zfill(3) if str(section).isdigit() else str(section)
        selected = sections.get(section) or sections.get(section.lstrip('0')) or []
        if not selected:
            raise ValueError(
                f'No projectTables found for section {section!r}. '
                f'Known sections: {sorted(sections)}'
            )
    else:
        selected = list(order)

    if only:
        only_set = set(only)
        selected = [n for n in selected if n in only_set]

    order_index = {n: i for i, n in enumerate(order)}
    selected = sorted(selected, key=lambda n: order_index.get(n, 10**9))

    force_set = set(force or [])
    if force_all:
        force_set |= set(selected)

    fps = compute_fingerprints(tables, order, global_token=gtoken)
    manifest = load_manifest(project_path)
    # Work on a copy so --dry-run never mutates on-disk state (Fable M2)
    nodes_m = dict(manifest.get('nodes') or {})
    if not dry_run:
        for stale in list(nodes_m.keys()):
            if stale not in tables:
                del nodes_m[stale]

    started = datetime.now(timezone.utc).isoformat()
    report = MakeReport(started=started, section=section)
    t0_all = time.time()
    failed: Set[str] = set()
    planned: Set[str] = set()  # dry-run plan or built this run

    for name in selected:
        cfg = tables.get(name) or {}
        method = (cfg.get('method') or 'manual').strip()
        item = getattr(e, name, None)
        location = getattr(item, 'location', None) if item is not None else None
        fp = fps.get(name, node_config_fingerprint(cfg))

        if method not in _MAKEABLE:
            report.results.append(NodeResult(
                name=name, action='unsupported',
                reason=f'method={method!r} (notebook-owned)',
                fingerprint=fp, location=location or '',
            ))
            # Treat as available if table exists (notebook may have built it)
            if location and table_exists(spark, location):
                planned.add(name)
            continue

        if item is None:
            failed.add(name)
            report.results.append(NodeResult(
                name=name, action='failed',
                reason='no ExtractItem on e', error='missing e.%s' % name,
                fingerprint=fp,
            ))
            continue

        if not getattr(item, 'location', None) or not getattr(item, 'label', None):
            logger.warning(
                'make: %s has no location/label — auto-write may not persist; '
                'skip-if-fresh will never fire', name,
            )

        ok_up, up_reason = upstream_fresh(
            name, tables, fps, nodes_m, spark, e, failed, planned=planned,
        )
        if not ok_up:
            # force rebuilds even if "fresh", but never build on failed-this-run
            # upstreams; for true staleness, force still needs upstream present
            if any(dep in failed for dep in deps_from_inputs(cfg)):
                failed.add(name)
                report.results.append(NodeResult(
                    name=name, action='failed',
                    reason=up_reason, fingerprint=fp, location=location or '',
                ))
                continue
            if name not in force_set:
                failed.add(name)
                report.results.append(NodeResult(
                    name=name, action='failed',
                    reason=up_reason, fingerprint=fp, location=location or '',
                ))
                continue
            logger.warning(
                'make: forcing %s despite upstream issue: %s', name, up_reason,
            )

        prev = nodes_m.get(name) or {}
        fresh = (
            name not in force_set
            and prev.get('fingerprint') == fp
            and table_exists(spark, location)
        )
        if fresh:
            n = prev.get('n_rows')
            if n is None:
                n = table_row_count(spark, location)
            planned.add(name)
            report.results.append(NodeResult(
                name=name, action='skipped',
                reason='fresh (fingerprint + table exists)',
                fingerprint=fp, location=location or '', n_rows=n,
            ))
            continue

        if dry_run:
            planned.add(name)
            report.results.append(NodeResult(
                name=name, action='plan',
                reason='dry-run would build',
                fingerprint=fp, location=location or '',
            ))
            continue

        t0 = time.time()
        try:
            note, n_rows = run_recipe(name, item, cfg, r, e, d, spark)
            elapsed = time.time() - t0
            location = getattr(item, 'location', None) or location
            planned.add(name)
            report.results.append(NodeResult(
                name=name, action='built', reason=note,
                fingerprint=fp, location=location or '',
                n_rows=n_rows, elapsed_s=elapsed,
            ))
            nodes_m[name] = {
                'fingerprint': fp,
                'global_token': gtoken,
                'location': location,
                'n_rows': n_rows,
                'method': method,
                'built_at': datetime.now(timezone.utc).isoformat(),
                'deps': deps_from_inputs(cfg),
            }
        except Exception as exc:
            elapsed = time.time() - t0
            failed.add(name)
            logger.exception('make failed on %s', name)
            report.results.append(NodeResult(
                name=name, action='failed',
                reason='error', error=str(exc),
                fingerprint=fp, location=location or '',
                elapsed_s=elapsed,
            ))

    report.finished = datetime.now(timezone.utc).isoformat()
    if dry_run:
        report.manifest_path = str(_manifest_path(project_path)) + ' (not written; dry-run)'
    else:
        manifest['nodes'] = nodes_m
        mpath = save_manifest(project_path, manifest)
        report.manifest_path = str(mpath)

    nb = notebook_path
    if auto_notebook and not nb and section:
        nb = find_notebook_for_section(project_path, section)
    if nb:
        try:
            splice_report_into_notebook(nb, report)
        except Exception as exc:
            logger.warning('Notebook splice failed: %s', exc)

    logger.info(
        'make done in %.1fs: %s',
        time.time() - t0_all,
        {a: sum(1 for r in report.results if r.action == a)
         for a in ('built', 'skipped', 'plan', 'failed', 'unsupported')},
    )
    return report


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog='python -m lhn.make',
        description='Make/targets-style runner for lhn projectTables',
    )
    parser.add_argument(
        '--config', default='000-control.yaml',
        help='Path to 000-control.yaml (default: ./000-control.yaml)',
    )
    parser.add_argument(
        '--section', default=None,
        help='Comment section id to run (e.g. 014)',
    )
    parser.add_argument(
        '--only', nargs='*', default=None,
        help='Only these node names',
    )
    parser.add_argument(
        '--force', nargs='*', default=None,
        help='Force rebuild of these nodes',
    )
    parser.add_argument(
        '--force-all', action='store_true',
        help='Force rebuild of all selected nodes',
    )
    parser.add_argument(
        '--notebook', default=None,
        help='ipynb path to splice the make report into',
    )
    parser.add_argument(
        '--auto-notebook', action='store_true',
        help='Find {section}-*.ipynb and splice report',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Plan only; do not build',
    )
    parser.add_argument(
        '--quiet', action='store_true',
        help='Do not print the markdown report (still splices if requested)',
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    # Late import so --help works without Spark
    from lhn.bootstrap import pipeline_setup

    ctx = pipeline_setup(args.config)
    report = make_run(
        ctx,
        config_path=args.config,
        section=args.section,
        only=args.only,
        force=args.force,
        force_all=args.force_all,
        notebook_path=args.notebook,
        auto_notebook=args.auto_notebook,
        dry_run=args.dry_run,
    )
    if not args.quiet:
        print(report.to_markdown())
    failed = sum(1 for r in report.results if r.action == 'failed')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
