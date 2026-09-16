import pytest

from shopee_quality import database


def test_postgres_connection_kwargs_from_environment(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "postgres")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "shopee_analytics")
    monkeypatch.setenv("POSTGRES_USER", "shopee_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-password")

    actual = database.postgres_connection_kwargs()

    assert actual == {
        "host": "postgres",
        "port": 5432,
        "dbname": "shopee_analytics",
        "user": "shopee_user",
        "password": "test-password",
        "connect_timeout": 10,
    }


REQUIRED_POSTGRES_VARIABLES = (
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
)


def set_valid_postgres_environment(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "shopee_quality")
    monkeypatch.setenv("POSTGRES_USER", "shopee_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-password")


@pytest.mark.parametrize(
    "missing_variable",
    REQUIRED_POSTGRES_VARIABLES,
)
def test_postgres_connection_kwargs_rejects_missing_variable(
    monkeypatch,
    missing_variable: str,
) -> None:
    set_valid_postgres_environment(monkeypatch)
    monkeypatch.delenv(missing_variable)

    with pytest.raises(KeyError, match=missing_variable):
        database.postgres_connection_kwargs()


def test_postgres_connection_kwargs_rejects_invalid_port(
    monkeypatch,
) -> None:
    set_valid_postgres_environment(monkeypatch)
    monkeypatch.setenv("POSTGRES_PORT", "not-a-port")

    with pytest.raises(ValueError):
        database.postgres_connection_kwargs()
