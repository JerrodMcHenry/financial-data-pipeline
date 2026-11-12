"""PostgreSQL connection handling.

Usage:
    with connect() as conn:
        conn.execute("SELECT 1")

The connection context manager commits on success, rolls back on error,
and always closes the connection.
"""

import os

import psycopg


class DatabaseConfigError(RuntimeError):
    """Raised when required database configuration is missing."""


def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise DatabaseConfigError(
            "DATABASE_URL is not set. Copy .env.example to .env, fill in values, "
            "and export it (e.g. `set -a; source .env; set +a`)."
        )
    return url


def connect() -> psycopg.Connection:
    """Open a new PostgreSQL connection. Use it as a context manager."""
    return psycopg.connect(get_database_url())


if __name__ == "__main__":
    with connect() as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
    print(f"Connected to PostgreSQL {version}")
