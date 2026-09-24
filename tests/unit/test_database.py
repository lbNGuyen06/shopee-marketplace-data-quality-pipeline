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


REQUIRED_SOURCE_VARIABLES = (
    "SOURCE_DB_DRIVER",
    "SOURCE_DB_HOST",
    "SOURCE_DB_PORT",
    "SOURCE_DB_NAME",
    "SOURCE_DB_USER",
    "SOURCE_DB_PASSWORD",
)


def set_valid_source_environment(monkeypatch) -> None:
    monkeypatch.setenv("SOURCE_DB_DRIVER", "ODBC Driver 17 for SQL Server")
    monkeypatch.setenv("SOURCE_DB_HOST", "sqlserver.example.test")
    monkeypatch.setenv("SOURCE_DB_PORT", "1433")
    monkeypatch.setenv("SOURCE_DB_NAME", "xomdata_dataset")
    monkeypatch.setenv("SOURCE_DB_USER", "source_user")
    monkeypatch.setenv("SOURCE_DB_PASSWORD", "test-password")


def test_sqlserver_connection_string_from_environment(monkeypatch) -> None:
    set_valid_source_environment(monkeypatch)

    actual = database.sqlserver_connection_string()

    assert actual == (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=sqlserver.example.test,1433;"
        "DATABASE=xomdata_dataset;"
        "UID=source_user;"
        "PWD=test-password;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=10;"
    )


@pytest.mark.parametrize("missing_variable", REQUIRED_SOURCE_VARIABLES)
def test_sqlserver_connection_string_rejects_missing_variable(
    monkeypatch,
    missing_variable: str,
) -> None:
    set_valid_source_environment(monkeypatch)
    monkeypatch.delenv(missing_variable)

    with pytest.raises(KeyError, match=missing_variable):
        database.sqlserver_connection_string()


def test_sqlserver_connection_string_rejects_invalid_port(monkeypatch) -> None:
    set_valid_source_environment(monkeypatch)
    monkeypatch.setenv("SOURCE_DB_PORT", "not-a-port")

    with pytest.raises(ValueError):
        database.sqlserver_connection_string()
