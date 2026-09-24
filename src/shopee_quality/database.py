import os


def postgres_connection_kwargs() -> dict:
    return {
        "host": os.environ["POSTGRES_HOST"],
        "port": int(os.environ["POSTGRES_PORT"]),
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "connect_timeout": 10,
    }


def sqlserver_connection_string() -> str:
    port = int(os.environ["SOURCE_DB_PORT"])

    return (
        f"DRIVER={{{os.environ['SOURCE_DB_DRIVER']}}};"
        f"SERVER={os.environ['SOURCE_DB_HOST']},{port};"
        f"DATABASE={os.environ['SOURCE_DB_NAME']};"
        f"UID={os.environ['SOURCE_DB_USER']};"
        f"PWD={os.environ['SOURCE_DB_PASSWORD']};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=10;"
    )
