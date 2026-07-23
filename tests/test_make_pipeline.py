"""Unit tests for make_pipeline (no Spark)."""

import json
from pathlib import Path

from lhn.make_pipeline import (
    load_project_tables,
    node_config_fingerprint,
    deps_from_inputs,
    compute_fingerprints,
    MakeReport,
    NodeResult,
    splice_report_into_notebook,
    _stable_hash,
)


SAMPLE_YAML = """
projectTables:
  # 011 — extract
  codes:
    method: load_csv_as_df
    csv: x.csv
  # 014 — observability
  personYearA:
    method: write_index_table
    inputs:
      inTable: codes
    indexFields: [personid, year]
    code: a
  personYearB:
    method: write_index_table
    inputs:
      inTable: codes
    code: b
  overlap:
    method: entityExtract
    inputs:
      elementList: personYearA
      entitySource: personYearB
    howjoin: inner
    broadcast_flag: false
  panel:
    method: entityExtract
    inputs:
      spine: personYearA
      features: [personYearB, overlap]
    howjoin: left
    on_collision: raise
"""


def test_load_sections_and_order(tmp_path: Path):
    p = tmp_path / '000-control.yaml'
    p.write_text(SAMPLE_YAML, encoding='utf-8')
    tables, order, sections = load_project_tables(str(p))
    assert order == ['codes', 'personYearA', 'personYearB', 'overlap', 'panel']
    assert '011' in sections
    assert '014' in sections
    assert sections['014'] == ['personYearA', 'personYearB', 'overlap', 'panel']
    assert tables['overlap']['howjoin'] == 'inner'


def test_deps_and_fingerprint_changes():
    cfg_a = {
        'method': 'write_index_table',
        'inputs': {'inTable': 'codes'},
        'code': 'a',
    }
    cfg_b = dict(cfg_a)
    cfg_b['code'] = 'b'
    assert node_config_fingerprint(cfg_a) != node_config_fingerprint(cfg_b)
    assert deps_from_inputs(cfg_a) == ['codes']
    assert deps_from_inputs({
        'inputs': {
            'elementList': 'personYearA',
            'entitySource': 'r.conditionSource',
            'features': ['glp1IndexAny', 'e.personObservabilityStudy'],
        }
    }) == ['personYearA', 'glp1IndexAny', 'personObservabilityStudy']


def test_upstream_fingerprint_invalidates(tmp_path: Path):
    p = tmp_path / '000-control.yaml'
    p.write_text(SAMPLE_YAML, encoding='utf-8')
    tables, order, _ = load_project_tables(str(p))
    fps1 = compute_fingerprints(tables, order)
    tables['personYearA']['code'] = 'CHANGED'
    fps2 = compute_fingerprints(tables, order)
    assert fps1['personYearA'] != fps2['personYearA']
    # downstream should change too
    assert fps1['overlap'] != fps2['overlap']


def test_splice_notebook(tmp_path: Path):
    nb = {
        'nbformat': 4,
        'nbformat_minor': 5,
        'metadata': {},
        'cells': [
            {'cell_type': 'markdown', 'metadata': {}, 'source': ['# Title\n']},
            {'cell_type': 'code', 'metadata': {}, 'source': ['print(1)\n'],
             'outputs': [], 'execution_count': None},
        ],
    }
    path = tmp_path / '014-Demo.ipynb'
    path.write_text(json.dumps(nb), encoding='utf-8')
    report = MakeReport(
        started='t0', finished='t1', section='014',
        results=[NodeResult(name='a', action='skipped', reason='fresh', n_rows=10)],
        manifest_path='/tmp/m.json',
    )
    splice_report_into_notebook(str(path), report)
    out = json.loads(path.read_text(encoding='utf-8'))
    tags = [c.get('metadata', {}).get('tags') for c in out['cells']]
    assert any(tags and 'lhn-make-report' in (t or []) for t in tags)
    # second splice replaces
    report.results.append(NodeResult(name='b', action='built', n_rows=3))
    splice_report_into_notebook(str(path), report)
    out2 = json.loads(path.read_text(encoding='utf-8'))
    report_cells = [
        c for c in out2['cells']
        if 'lhn-make-report' in (c.get('metadata') or {}).get('tags', [])
    ]
    assert len(report_cells) == 1
    text = ''.join(report_cells[0]['source'])
    assert 'person' not in text or 'a' in text
    assert '**built**' in text or 'built' in text
