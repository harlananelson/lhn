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

_FINGERPRINT_KEYS = (
    'method', 'indexFields', 'datefieldPrimary', 'datefieldStop', 'code',
    'histStart', 'histEnd', 'histStop', 'howjoin', 'broadcast_flag',
    'on_collision', 'datefieldSource', 'datefieldElement', 'fieldList',
    'masterList', 'sourceField', 'listIndex', 'find_method', 'dictionary',
    'retained_fields', 'max_gap', 'csv', 'inputs', 'groupName',
    'conditionCodefield', 'concept_flags',
)

_SECTION_RE = re.compile(
    r'^#\s*(?:NOTEBOOK\s+)?(\d{3})\b|^#\s*(\d{3})\s*[—\-–]',
    re.IGNORECASE,
)

_MAKEABLE = frozenset({
    'write_index_table', 'entityExtract', 'create_extract',
    'load_csv_as_df', 'dict2pyspark',
})


# ---------------------------------------------------------------------------
# YAML + sections
# ---------------------------------------------------------------------------

def load_project_tables(
    config_path: str,
) -> Tuple[Dict[str, dict], List[str], Dict[str, List[str]]]:
    """Load projectTables, key order, and comment-section → names."""
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
    return tables, list(tables.keys()), sections


def _stable_hash(obj: Any) -> str:
    payload = json.dumps(obj, sort_keys=True, default=str, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]


def node_config_fingerprint(cfg: dict) -> str:
    subset = {k: cfg.get(k) for k in _FINGERPRINT_KEYS if k in cfg}
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

def resolve_ref(ref: Any, r, e, d):
    """Resolve an inputs value to ExtractItem or DataFrame."""
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
    raise KeyError(f'Cannot resolve input reference {ref!r}')


def _as_df(obj):
    if obj is None:
        return None
    if hasattr(obj, 'df') and not hasattr(obj, 'columns'):
        return obj.df
    return obj


def run_recipe(name: str, item, cfg: dict, r, e, d, spark) -> Tuple[str, Optional[int]]:
    """Execute one makeable method. Returns (note, n_rows)."""
    method = (cfg.get('method') or '').strip()
    inputs = cfg.get('inputs') or {}

    if method == 'write_index_table':
        in_ref = inputs.get('inTable') or inputs.get('source')
        if not in_ref:
            raise ValueError(
                f'{name}: write_index_table needs inputs.inTable'
            )
        item.write_index_table(inTable=resolve_ref(in_ref, r, e, d))

    elif method == 'entityExtract':
        features = inputs.get('features')
        spine = inputs.get('spine')
        if features is not None:
            if not spine:
                raise ValueError(f'{name}: inputs.features requires inputs.spine')
            spine_df = _as_df(resolve_ref(spine, r, e, d))
            feat_list = features if isinstance(features, list) else [features]
            for i, feat in enumerate(feat_list):
                el = resolve_ref(feat, r, e, d)
                src = spine_df if i == 0 else item.df
                item.entityExtract(el, src)
        else:
            el = resolve_ref(inputs.get('elementList'), r, e, d)
            src = resolve_ref(inputs.get('entitySource'), r, e, d)
            if el is None or src is None:
                raise ValueError(
                    f'{name}: entityExtract needs elementList+entitySource '
                    f'or spine+features'
                )
            item.entityExtract(el, _as_df(src))

    elif method == 'create_extract':
        el = resolve_ref(inputs.get('elementList'), r, e, d)
        src = resolve_ref(inputs.get('elementListSource'), r, e, d)
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
) -> Dict[str, str]:
    """Fingerprint each node including sorted upstream fingerprints."""
    fps: Dict[str, str] = {}

    def fp(name: str, stack: Optional[Set[str]] = None) -> str:
        if name in fps:
            return fps[name]
        stack = stack or set()
        if name in stack:
            return 'cycle'
        stack = set(stack)
        stack.add(name)
        cfg = tables.get(name) or {}
        base = node_config_fingerprint(cfg)
        ups = []
        for dep in deps_from_inputs(cfg):
            if dep in tables:
                ups.append(f'{dep}:{fp(dep, stack)}')
        full = _stable_hash({'self': base, 'up': ups})
        fps[name] = full
        return full

    for n in names:
        if n in tables:
            fp(n)
    return fps


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

    tables, order, sections = load_project_tables(config_path)
    if section:
        section = str(section).zfill(3) if str(section).isdigit() else str(section)
        selected = sections.get(section) or sections.get(section.lstrip('0')) or []
        if not selected:
            # fallback: names that appear in order after matching comment parse miss
            raise ValueError(
                f'No projectTables found for section {section!r}. '
                f'Known sections: {sorted(sections)}'
            )
    else:
        selected = list(order)

    if only:
        only_set = set(only)
        selected = [n for n in selected if n in only_set]

    # preserve YAML order
    order_index = {n: i for i, n in enumerate(order)}
    selected = sorted(selected, key=lambda n: order_index.get(n, 10**9))

    force_set = set(force or [])
    if force_all:
        force_set |= set(selected)

    fps = compute_fingerprints(tables, order)
    manifest = load_manifest(project_path)
    nodes_m = manifest.setdefault('nodes', {})

    started = datetime.now(timezone.utc).isoformat()
    report = MakeReport(started=started, section=section)
    t0_all = time.time()

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
            continue

        if item is None:
            report.results.append(NodeResult(
                name=name, action='failed',
                reason='no ExtractItem on e', error='missing e.%s' % name,
                fingerprint=fp,
            ))
            continue

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
            report.results.append(NodeResult(
                name=name, action='skipped',
                reason='fresh (fingerprint + table exists)',
                fingerprint=fp, location=location or '', n_rows=n,
            ))
            continue

        if dry_run:
            report.results.append(NodeResult(
                name=name, action='built',
                reason='dry-run would build',
                fingerprint=fp, location=location or '',
            ))
            continue

        t0 = time.time()
        try:
            note, n_rows = run_recipe(name, item, cfg, r, e, d, spark)
            elapsed = time.time() - t0
            location = getattr(item, 'location', None) or location
            report.results.append(NodeResult(
                name=name, action='built', reason=note,
                fingerprint=fp, location=location or '',
                n_rows=n_rows, elapsed_s=elapsed,
            ))
            nodes_m[name] = {
                'fingerprint': fp,
                'location': location,
                'n_rows': n_rows,
                'method': method,
                'built_at': datetime.now(timezone.utc).isoformat(),
                'deps': deps_from_inputs(cfg),
            }
        except Exception as exc:
            elapsed = time.time() - t0
            logger.exception('make failed on %s', name)
            report.results.append(NodeResult(
                name=name, action='failed',
                reason='error', error=str(exc),
                fingerprint=fp, location=location or '',
                elapsed_s=elapsed,
            ))

    report.finished = datetime.now(timezone.utc).isoformat()
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
         for a in ('built', 'skipped', 'failed', 'unsupported')},
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
        '--print-report', action='store_true',
        help='Print markdown report to stdout',
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
    print(report.to_markdown())
    failed = sum(1 for r in report.results if r.action == 'failed')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
