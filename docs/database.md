# Database lifecycle

`python_stdx.database` provides a small synchronous SQLAlchemy lifecycle boundary. It owns one engine and session factory, applies explicit connection-pool capacity and timeouts, and exposes two context managers:

- `session()` rolls back on failure and never commits automatically.
- `transaction()` commits on success and rolls back on failure.

SQLAlchemy exceptions are preserved instead of being translated into HTTP or product-specific exceptions. Callers can therefore handle `IntegrityError`, `OperationalError`, and other precise failure types at the correct layer.

`Session.get_dialect_name()` is available for the small number of cases that genuinely need a dialect branch. Portable application code should otherwise continue using common SQLAlchemy constructs.

The package deliberately does not maintain an enum of database products, hard-coded driver URLs, or bundled customer-specific dialects. Applications pass a SQLAlchemy URL and install the driver they need. SQLAlchemy's dialect and pool extension points remain directly available through the owned engine.
