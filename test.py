
import oracledb
from . import OracleSaver
oracle_config = {
    "host": "localhost",
    "port": 1521,
    "service_name": "FREE",
    "user": "system",
    "password": "root",
}
with oracledb.connect(
    user=oracle_config["user"],
    password=oracle_config["password"],
    dsn=f"{oracle_config['host']}:{oracle_config['port']}/{oracle_config['service_name']}",
) as conn:
    conn.autocommit = True
    checkpointer = OracleSaver(conn)