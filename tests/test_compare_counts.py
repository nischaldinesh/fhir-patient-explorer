import importlib.util
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("compare_counts", ROOT / "orchestration/airflow/scripts/compare_counts.py")
cc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cc)


def make_warehouse(data_dir, rows):
    db = cc.warehouse_path(data_dir)
    db.parent.mkdir(parents=True)
    con = duckdb.connect(str(db))
    for t, n in rows.items():
        con.execute(f"CREATE TABLE {t} AS SELECT range AS id FROM range({n})")
    con.close()
    return db


def test_warehouse_path():
    assert cc.warehouse_path("data/airflow") == Path("data/airflow/analytics/warehouse.duckdb")


def test_mart_counts(tmp_path):
    con = duckdb.connect(str(make_warehouse(tmp_path, {"dim_date": 3, "fact_encounter": 0})), read_only=True)
    assert cc.mart_counts(con, ["dim_date", "fact_encounter", "dim_lab"]) == {"dim_date": 3, "fact_encounter": 0,
                                                                             "dim_lab": None}
    con.close()


def test_diff_counts():
    assert cc.diff_counts({"a": 1, "b": 2}, {"a": 1, "b": 2}) == []
    assert cc.diff_counts({"a": 1, "b": 2}, {"b": 3, "c": 0}) == [("a", 1, None), ("b", 2, 3), ("c", None, 0)]


def test_main_save_and_against(tmp_path, capsys):
    make_warehouse(tmp_path, {t: 2 for t in cc.MART_TABLES})
    saved = tmp_path / "first.json"
    assert cc.main(["--data-dir", str(tmp_path), "--save", str(saved)]) == 0
    assert json.loads(saved.read_text()) == {t: 2 for t in cc.MART_TABLES}
    assert cc.main(["--data-dir", str(tmp_path), "--against", str(saved)]) == 0
    assert "Row counts identical" in capsys.readouterr().out
    saved.write_text(json.dumps({**{t: 2 for t in cc.MART_TABLES}, "dim_date": 5}))
    assert cc.main(["--data-dir", str(tmp_path), "--against", str(saved)]) == 1
    assert "DIFFERENT dim_date: 5 then 2" in capsys.readouterr().out
