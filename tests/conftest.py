import pytest
import oracledb
from typing import Iterator

# from embed_test_utils import CharacterEmbeddings

DEFAULT_ORACLE_CONFIG = {
    "host": "localhost",
    "port": 1521,
    "service_name": "FREE",
    "user": "system",
    "password": "root"
}

@pytest.fixture(scope="function")
def conn() -> Iterator[oracledb.Connection]:
    connection = oracledb.connect(
        user=DEFAULT_ORACLE_CONFIG["user"],
        password=DEFAULT_ORACLE_CONFIG["password"],
        dsn=f"{DEFAULT_ORACLE_CONFIG['host']}:{DEFAULT_ORACLE_CONFIG['port']}/{DEFAULT_ORACLE_CONFIG['service_name']}"
    )
    connection.autocommit = True
    yield connection
    connection.close()

@pytest.fixture(scope="function", autouse=True)
def clear_test_db(conn: oracledb.Connection) -> None:
    """Delete all tables before each test."""
    cursor = conn.cursor()
    try:
        cursor.execute("TRUNCATE TABLE checkpoints")
        cursor.execute("TRUNCATE TABLE checkpoint_blobs")
        cursor.execute("TRUNCATE TABLE checkpoint_writes")
        cursor.execute("TRUNCATE TABLE checkpoint_migrations")
    except oracledb.DatabaseError:
        pass
    try:
        cursor.execute("TRUNCATE TABLE store_migrations")
        cursor.execute("TRUNCATE TABLE store")
    except oracledb.DatabaseError:
        pass
    cursor.close()

# @pytest.fixture
# def fake_embeddings() -> CharacterEmbeddings:
#     return CharacterEmbeddings(dims=500)


VECTOR_TYPES = ["vector", "halfvec"]
