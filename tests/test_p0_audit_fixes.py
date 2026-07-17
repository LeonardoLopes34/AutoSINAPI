"""
P0 audit fixes for the ETL toolkit (AutoSINAPI).

Covers:
  - H5: ETL status enum aligned with Celery ("success"/"no_data"/"failure").
  - H6: Alembic migration adds origem_preco / percentual_mo missing columns.
  - Robustness: all-or-nothing transactional load via Database.save_all().
"""

import logging
import os
import sys

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

# Make the AutoSINAPI package importable regardless of CWD.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from autosinapi.config import Config  # noqa: E402
from autosinapi.core.database import Database  # noqa: E402
from autosinapi.exceptions import DatabaseError  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))


@pytest.fixture
def config():
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
    }
    sinapi_config = {"state": "SP", "month": "01", "year": "2023", "type": "REFERENCIA"}
    return Config(db_config, sinapi_config, "server")


def test_status_enum_aligned_with_celery(config):
    """H5: status strings must match what the API/Celery layer expects."""
    assert config.STATUS_SUCCESS == "success"
    assert config.STATUS_SUCCESS_NO_DATA == "no_data"
    assert config.STATUS_FAILURE == "failure"


def test_alembic_005_migration_adds_missing_columns():
    """H6: migration 005 must add origem_preco and percentual_mo columns."""
    migration_path = os.path.join(
        REPO_ROOT, "alembic", "versions", "005_add_etl_missing_columns.py"
    )
    assert os.path.exists(migration_path), "migration 005 file must exist"

    content = open(migration_path, encoding="utf-8").read()
    assert 'revision' in content and '"005"' in content, "revision must be 005"
    assert 'down_revision' in content and '"004"' in content, "down_revision must be 004"
    assert "origem_preco" in content, "must add origem_preco to precos_insumos_mensal"
    assert "percentual_mo" in content, "must add percentual_mo to custos_composicoes_mensal"
    # Idempotent / safe against re-runs
    assert "IF NOT EXISTS" in content


class _FakeConfig:
    DB_POLICY_REPLACE = "replace"
    DB_POLICY_APPEND = "append"
    DB_POLICY_UPSERT = "upsert"
    DB_TEMP_TABLE_PREFIX = "tmp_"


def _make_sqlite_db():
    engine = create_engine("sqlite://")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, sinapi_versao TEXT, etl_run_id TEXT)"))
        conn.execute(text("CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT, sinapi_versao TEXT, etl_run_id TEXT)"))
        conn.commit()
    db = Database.__new__(Database)
    db._engine = engine
    db.config = _FakeConfig()
    db.logger = logging.getLogger("test.save_all")
    return db


def test_save_all_commits_all_tables():
    db = _make_sqlite_db()
    df_items = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    df_logs = pd.DataFrame({"id": [1, 2], "msg": ["x", "y"]})
    loads = [
        ("k1", "items", "append", ["id"]),
        ("k2", "logs", "append", ["id"]),
    ]
    data = {"k1": df_items, "k2": df_logs}
    records, tables = db.save_all(loads, data, {}, "run-1", "2023.01")

    assert records == 4
    assert set(tables) == {"items", "logs"}
    with db._engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM items")).scalar() == 2
        assert conn.execute(text("SELECT count(*) FROM logs")).scalar() == 2


def test_save_all_rolls_back_on_failure():
    db = _make_sqlite_db()
    df_items = pd.DataFrame({"id": [1], "name": ["a"]})
    df_logs = pd.DataFrame({"id": [1], "msg": ["x"]})
    loads = [
        ("k1", "items", "append", ["id"]),
        ("k2", "logs", "append", ["id"]),
        ("k3", "fail_table", "append", ["id"]),
    ]
    data = {"k1": df_items, "k2": df_logs, "k3": df_items.copy()}

    # Inject a failure only on the last load to validate the all-or-nothing
    # rollback of the single outer transaction opened by save_all.
    orig_save = db.save_data

    def fake_save(data, table_name, policy, conn=None, **kw):
        if table_name == "fail_table":
            raise DatabaseError("forced failure")
        return orig_save(data, table_name, policy, conn=conn, **kw)

    with pytest.raises(DatabaseError):
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(db, "save_data", fake_save)
            db.save_all(loads, data, {}, "run-2", "2023.01")

    # Nothing should have been committed after the rollback.
    with db._engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM items")).scalar() == 0
        assert conn.execute(text("SELECT count(*) FROM logs")).scalar() == 0
