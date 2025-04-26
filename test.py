
import oracledb
oracle_config = {
    "host": "localhost",
    "port": 1521,
    "service_name": "FREE",
    "user": "system",
    "password": "root",
}
UPSERT_CHECKPOINT_BLOBS_SQL = """MERGE INTO checkpoint_blobs dst
USING (
    SELECT
        :1      AS thread_id,
        :2  AS checkpoint_ns,
        :3        AS channel,
        :4        AS version,
        :5           AS type,
        :6           AS bl
    FROM DUAL
) src
ON (
    dst.thread_id       = src.thread_id
    AND dst.checkpoint_ns = src.checkpoint_ns
    AND dst.channel        = src.channel
    AND dst.version        = src.version
)
WHEN NOT MATCHED THEN
  INSERT (
    thread_id,
    checkpoint_ns,
    channel,
    version,
    type,
    blob
  )
  VALUES (
    src.thread_id,
    src.checkpoint_ns,
    src.channel,
    src.version,
    src.type,
    src.bl
  )"""

with oracledb.connect(
    user=oracle_config["user"],
    password=oracle_config["password"],
    dsn=f"{oracle_config['host']}:{oracle_config['port']}/{oracle_config['service_name']}",
) as conn:
    cur = conn.cursor()
    cur.executemany(UPSERT_CHECKPOINT_BLOBS_SQL, [('thread-pool', 'oracle', 'empty', 'empty', 'msgpack', b'\x92\xa5empty\xc0')])
    print("Checkpoint inserted successfully.")