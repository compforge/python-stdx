"""SQLAlchemy session with a small dialect-introspection helper."""

from sqlalchemy.orm import Session as SQLAlchemySession


class Session(SQLAlchemySession):
    """A regular synchronous session that can report its bound dialect."""

    def get_dialect_name(self) -> str:
        """Return the SQLAlchemy dialect name of the current bind."""
        return self.get_bind().dialect.name
