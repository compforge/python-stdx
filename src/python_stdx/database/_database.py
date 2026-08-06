"""Explicit SQLAlchemy engine, session, and transaction lifecycle management."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager

from sqlalchemy import URL, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import Pool, QueuePool

from python_stdx.database._session import Session


class Database:
    """Own a synchronous SQLAlchemy engine and its session factory.

    ``session()`` never commits automatically. ``transaction()`` commits on
    success and rolls back on every exception. SQLAlchemy exceptions are kept
    intact so callers can handle ``IntegrityError`` and other precise failures.
    """

    def __init__(
        self,
        url: str | URL,
        *,
        pool_size: int = 20,
        max_overflow: int = 10,
        pool_timeout: float = 30.0,
        pool_recycle: int = 1800,
        pool_pre_ping: bool = True,
        pool_class: type[Pool] = QueuePool,
        echo: bool = False,
        connect_args: Mapping[str, object] | None = None,
    ) -> None:
        """Create an engine with explicit capacity and timeout settings."""
        self.engine: Engine = create_engine(
            url,
            poolclass=pool_class,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=pool_pre_ping,
            echo=echo,
            connect_args=dict(connect_args or {}),
        )
        self.session_factory: sessionmaker[Session] = sessionmaker(
            bind=self.engine,
            class_=Session,
            autoflush=False,
            expire_on_commit=False,
        )

    def close(self) -> None:
        """Dispose the engine and every pooled connection it owns."""
        self.engine.dispose()

    @contextmanager
    def session(self) -> Iterator[Session]:
        """Yield a session that rolls back on failure and never auto-commits."""
        session = self.session_factory()
        try:
            yield session
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def transaction(self) -> Iterator[Session]:
        """Yield a transaction that commits on success and rolls back on failure."""
        session = self.session_factory()
        try:
            with session.begin():
                yield session
        finally:
            session.close()
