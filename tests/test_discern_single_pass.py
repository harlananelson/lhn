"""Unit tests for single-pass Discern extract helpers and contracts.

Pure helpers run without Spark. Integration tests (stub UDF) skip when a local
SparkSession is unavailable (e.g. Databricks-Connect-only environments).
"""

from __future__ import annotations

import types
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Pure helpers — import without going through lhn.header / Spark session
# ---------------------------------------------------------------------------

def _load_extract_module_pure():
    """Load just the pure helper source by exec'ing a stripped slice.

    Full ``import lhn`` creates a SparkSession at import time (spark_config_mapper
    header). These helpers are pure Python and do not need that.
    """
    # Prefer real import if environment supports it.
    try:
        from lhn.core.extract import (
            _discern_sql_lit,
            _discern_context_concept_subsets,
        )
        return _discern_sql_lit, _discern_context_concept_subsets
    except Exception:
        pass

    # Fallback: evaluate the two pure functions from source text.
    path = __file__.replace("tests/test_discern_single_pass.py",
                            "lhn/core/extract.py")
    # also try absolute
    import pathlib
    path = pathlib.Path(__file__).resolve().parents[1] / "lhn" / "core" / "extract.py"
    text = path.read_text()
    # extract function bodies via simple exec of the helper section
    start = text.index("def _discern_sql_lit")
    end = text.index("def _derive_date_parts")
    ns = {}
    # _discern_match_tag_exprs needs F — skip; only pure ones:
    chunk = text[start:end]
    # only exec pure functions that don't reference F
    pure = []
    for name in ("_discern_sql_lit", "_discern_context_concept_subsets"):
        i = chunk.index("def {}".format(name))
        # next def after this
        j = chunk.find("\ndef ", i + 1)
        pure.append(chunk[i:j] if j != -1 else chunk[i:])
    exec("\n\n".join(pure), ns)
    return ns["_discern_sql_lit"], ns["_discern_context_concept_subsets"]


_discern_sql_lit, _discern_context_concept_subsets = _load_extract_module_pure()


class TestDiscernSqlLit:
    def test_plain(self):
        assert _discern_sql_lit("ECHO_PROC") == "ECHO_PROC"

    def test_backslash_escape_apostrophe(self):
        # Spark 2.4: backslash, not '' doubling
        assert _discern_sql_lit("O'Brien") == "O\\'Brien"

    def test_backslash_itself(self):
        assert _discern_sql_lit("a\\b") == "a\\\\b"


class TestContextConceptSubsets:
    def test_groups_and_dedups(self):
        flags = [
            {"flag": "echo", "concept": "ECHO_PROC", "context": "CTX_A"},
            {"flag": "echo", "concept": "ECHO_OBSTYPE", "context": "CTX_A"},
            {"flag": "lvef", "concept": "LV_EF", "context": "CTX_B"},
            {"flag": "echo2", "concept": "ECHO_PROC", "context": "CTX_A"},  # dup concept
        ]
        got = _discern_context_concept_subsets(flags)
        assert list(got.keys()) == ["CTX_A", "CTX_B"]
        assert got["CTX_A"] == ["ECHO_PROC", "ECHO_OBSTYPE"]
        assert got["CTX_B"] == ["LV_EF"]


# ---------------------------------------------------------------------------
# Source-structure guards (no execution): single-pass, no filter+union loop
# ---------------------------------------------------------------------------

class TestSourceShape:
    def test_events_no_union_loop(self):
        path = __file__.replace("tests/test_discern_single_pass.py",
                                "lhn/core/extract.py")
        import pathlib
        path = pathlib.Path(__file__).resolve().parents[1] / "lhn" / "core" / "extract.py"
        text = path.read_text()
        # slice extract_concept_events method
        i = text.index("def extract_concept_events")
        j = text.index("def build_datadict", i)
        body = text[i:j]
        assert "F.explode" in body
        assert "F.array(*tag_exprs)" in body or "F.array(*" in body
        assert "unionByName" not in body
        assert "for row in flags:\n            matched" not in body
        assert "single-pass" in body or "Single pass" in body

    def test_counts_no_union_loop(self):
        import pathlib
        path = pathlib.Path(__file__).resolve().parents[1] / "lhn" / "core" / "extract.py"
        text = path.read_text()
        i = text.index("def build_ontology_counts")
        j = text.index("def build_ontology_coverage", i)
        body = text[i:j]
        assert "F.explode" in body
        assert "_union_aligned" not in body
        assert "one filter+tag per concept, unioned" not in body
        assert "single-pass" in body or "Single pass" in body

    def test_push_subsets_concepts(self):
        import pathlib
        path = pathlib.Path(__file__).resolve().parents[1] / "lhn" / "core" / "extract.py"
        text = path.read_text()
        i = text.index("def push_discern")
        j = text.index("def extract_concept_flags", i)
        body = text[i:j]
        assert "lst.append(row['concept'])" in body
        assert "to_push.setdefault(row['context'], None)" not in body


# ---------------------------------------------------------------------------
# Integration (optional): stub UDF + local Spark
# ---------------------------------------------------------------------------

def _try_local_spark():
    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("lhn-discern-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        # Databricks Connect raises on real local plans sometimes
        spark.range(1).count()
        return spark
    except Exception as ex:
        pytest.skip("local SparkSession unavailable: {}".format(ex))


@pytest.fixture(scope="module")
def spark():
    return _try_local_spark()


def _make_item(**kwargs):
    """Minimal ExtractItem without full package bootstrap when possible."""
    try:
        from lhn.core.extract import ExtractItem
    except Exception as ex:
        pytest.skip("cannot import ExtractItem: {}".format(ex))

    class Cfg:
        pass
    cfg = Cfg()
    cfg.name = kwargs.pop("name", "testEvents")
    for k, v in kwargs.items():
        setattr(cfg, k, v)
    return ExtractItem(cfg)


class TestExtractConceptEventsIntegration:
    def test_multi_match_and_same_flag_duplicates(self, spark):
        from pyspark.sql import Row
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType
        )

        # Register stub: match when code.standard.id equals concept suffix heuristic
        def has_concept_in_context(code, concept, context):
            # code is a Row/struct
            if code is None:
                return False
            cid = code["id"] if isinstance(code, dict) else getattr(code, "id", None)
            # simple: concept name contains the id token
            return cid is not None and cid in concept

        spark.udf.register("has_concept_in_context", has_concept_in_context, "boolean")

        schema = StructType([
            StructField("personid", StringType()),
            StructField("tenant", StringType()),
            StructField("servicedate", StringType()),
            StructField("labcode", StructType([
                StructField("id", StringType()),
            ])),
        ])
        # row0 matches ECHO_PROC and ECHO_OBSTYPE (both flag echo) if we use ids in names
        # Use concepts that contain the id string
        rows = [
            Row(personid="p1", tenant="t1", servicedate="2020-01-01",
                labcode=Row(id="ECHO")),
            Row(personid="p2", tenant="t1", servicedate="2020-01-02",
                labcode=Row(id="LVEF")),
            Row(personid="p3", tenant="t1", servicedate="2020-01-03",
                labcode=Row(id="NONE")),
        ]
        src = spark.createDataFrame(rows, schema=schema)

        flags = [
            {"flag": "echo", "concept": "X_ECHO_PROC", "context": "CTX1"},
            {"flag": "echo", "concept": "X_ECHO_OBSTYPE", "context": "CTX1"},
            {"flag": "lvef", "concept": "X_LVEF_OBS", "context": "CTX2"},
        ]
        item = _make_item(
            concept_flags=flags,
            conditionCodefield="labcode",
            datefieldPrimary="servicedate",
            indexFields=["personid", "tenant"],
            retained_fields=[],
        )

        # push_discern would call foresight — disable
        out = item.extract_concept_events(
            source=src, push=False, set_self_df=False
        )
        got = {(r.personid, r.flag) for r in out.collect()}
        # p1 matches both echo concepts → two rows both flag=echo
        assert ("p1", "echo") in got
        assert sum(1 for r in out.collect() if r.personid == "p1") == 2
        assert ("p2", "lvef") in got
        assert "p3" not in {r.personid for r in out.collect()}

    def test_flag_name_collision_raises(self, spark):
        item = _make_item(
            concept_flags=[
                {"flag": "x", "concept": "C", "context": "G"},
            ],
            conditionCodefield="labcode",
            indexFields=["personid", "flag"],
        )
        from pyspark.sql import Row
        src = spark.createDataFrame([Row(personid="p", flag="bad", labcode=Row(id="a"))])
        with pytest.raises(ValueError, match="reserved"):
            item.extract_concept_events(source=src, push=False, set_self_df=False)


class TestPushDiscernSubsets:
    def test_push_records_concept_lists(self):
        try:
            from lhn.core.extract import ExtractItem
        except Exception as ex:
            pytest.skip("cannot import ExtractItem: {}".format(ex))

        class Cfg:
            pass
        cfg = Cfg()
        cfg.name = "t"
        cfg.discern_root = "s3://x/v1/"
        item = ExtractItem(cfg)
        flags = [
            {"flag": "a", "concept": "C1", "context": "G1"},
            {"flag": "b", "concept": "C2", "context": "G1"},
            {"flag": "c", "concept": "C3", "context": "G2"},
            {"flag": "d", "concept": "C1", "context": "G1"},
        ]
        calls = []

        def fake_push(spark, discern_context=None, version=None,
                      discern_root=None, concepts=None):
            calls.append({
                "context": discern_context,
                "concepts": list(concepts) if concepts is not None else None,
                "root": discern_root,
            })

        fake_mod = types.ModuleType("foresight.discern")
        fake_mod.push_discern = fake_push
        with patch.dict("sys.modules", {
            "foresight": types.ModuleType("foresight"),
            "foresight.discern": fake_mod,
        }):
            item.push_discern(concept_flags=flags)

        by_ctx = {c["context"]: c["concepts"] for c in calls}
        assert by_ctx["G1"] == ["C1", "C2"]
        assert by_ctx["G2"] == ["C3"]
