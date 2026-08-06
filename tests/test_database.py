from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from python_stdx.database import Database


def _database(tmp_path: Path) -> Database:
    return Database(
        f"sqlite:///{tmp_path / 'test.db'}",
        pool_size=2,
        max_overflow=1,
        pool_timeout=1,
        connect_args={"check_same_thread": False},
    )


def test_transaction_commits_and_session_reports_dialect(tmp_path: Path) -> None:
    database = _database(tmp_path)
    try:
        with database.transaction() as session:
            assert session.get_dialect_name() == "sqlite"
            session.execute(text("CREATE TABLE item (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"))
            session.execute(text("INSERT INTO item (id, value) VALUES (1, 'saved')"))

        with database.session() as session:
            assert session.execute(text("SELECT value FROM item WHERE id = 1")).scalar_one() == "saved"
    finally:
        database.close()


def test_transaction_rolls_back_and_preserves_sqlalchemy_errors(tmp_path: Path) -> None:
    database = _database(tmp_path)
    try:
        with database.transaction() as session:
            session.execute(text("CREATE TABLE item (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"))

        with pytest.raises(RuntimeError, match="abort"):
            with database.transaction() as session:
                session.execute(text("INSERT INTO item (id, value) VALUES (1, 'rolled-back')"))
                raise RuntimeError("abort")

        with database.session() as session:
            assert session.execute(text("SELECT COUNT(*) FROM item")).scalar_one() == 0

        with database.transaction() as session:
            session.execute(text("INSERT INTO item (id, value) VALUES (1, 'saved')"))

        with pytest.raises(IntegrityError):
            with database.transaction() as session:
                session.execute(text("INSERT INTO item (id, value) VALUES (1, 'duplicate')"))
    finally:
        database.close()
