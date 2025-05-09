import ast
import random
from collections.abc import Sequence
from typing import Any, Optional, cast

from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    get_checkpoint_id,
)
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.serde.types import TASKS, ChannelProtocol

MetadataInput = Optional[dict[str, Any]]

"""
To add a new migration, add a new string to the MIGRATIONS list.
The position of the migration in the list is the version number.
"""
# """DROP TABLE CHECKPOINTS""",
# """DROP TABLE CHECKPOINT_BLOBS""",
# """DROP TABLE CHECKPOINT_WRITES""",   

MIGRATIONS = [
    """CREATE TABLE IF NOT EXISTS checkpoint_migrations (
        v NUMBER PRIMARY KEY
    )""",
 
"""CREATE TABLE IF NOT EXISTS checkpoints (
        thread_id VARCHAR2(200) NOT NULL,
        checkpoint_ns VARCHAR2(200) DEFAULT '',
        checkpoint_id VARCHAR2(200) NOT NULL,
        parent_checkpoint_id VARCHAR2(200),
        type VARCHAR2(200),
        checkpoint CLOB NOT NULL,
        metadata CLOB DEFAULT '{}',
        PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
    )""",
    """CREATE TABLE IF NOT EXISTS checkpoint_blobs (
        thread_id VARCHAR2(200) NOT NULL, 
        checkpoint_ns VARCHAR2(200) DEFAULT '',
        channel VARCHAR2(200) NOT NULL,
        version VARCHAR2(200) NOT NULL,
        type VARCHAR2(200) NOT NULL,
        blob BLOB,
        PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
    )""",
    """CREATE TABLE IF NOT EXISTS checkpoint_writes (
    thread_id VARCHAR2(255) NOT NULL,
    checkpoint_ns VARCHAR2(255) DEFAULT '',
    checkpoint_id VARCHAR2(255) NOT NULL,
    task_id VARCHAR2(255) NOT NULL,
    task_path VARCHAR2(255) DEFAULT '',
    idx NUMBER NOT NULL,
    channel VARCHAR2(255) NOT NULL,
    type VARCHAR2(255),
    blob BLOB NOT NULL,
    CONSTRAINT pk_checkpoint_writes PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
)""",
    """CREATE INDEX IF NOT EXISTS checkpoints_thread_id_idx ON checkpoints(thread_id)""",
    """CREATE INDEX IF NOT EXISTS checkpoint_blobs_thread_id_idx ON checkpoint_blobs(thread_id)""",
    """CREATE INDEX IF NOT EXISTS checkpoint_writes_thread_id_idx ON checkpoint_writes(thread_id)""",
    """ALTER TABLE IF NOT EXISTS checkpoint_writes ADD task_path VARCHAR2(1000) DEFAULT ''"""
]

SELECT_SQL = f"""
SELECT 
    c.thread_id,
    c.checkpoint,
    c.checkpoint_ns,
    c.checkpoint_id,
    c.parent_checkpoint_id,
    c.metadata,
    (
        SELECT JSON_ARRAYAGG(
            JSON_ARRAY(bl.channel, bl.type, bl.blob)
        )
        FROM JSON_TABLE(c.checkpoint, '$.channel_versions[*]' 
            COLUMNS (
                channel PATH '$.key',
                version PATH '$.value'
            )
        ) jt
        JOIN checkpoint_blobs bl 
            ON bl.thread_id = c.thread_id
            AND bl.checkpoint_ns = c.checkpoint_ns
            AND bl.channel = jt.channel
            AND bl.version = jt.version
    ) as channel_values,
    (
        SELECT JSON_ARRAYAGG(
            JSON_ARRAY(cw.task_id, cw.channel, cw.type, cw.blob) 
            ORDER BY cw.task_id, cw.idx
        )
        FROM checkpoint_writes cw
        WHERE cw.thread_id = c.thread_id
            AND cw.checkpoint_ns = c.checkpoint_ns
            AND cw.checkpoint_id = c.checkpoint_id
    ) as pending_writes,
    (
        SELECT JSON_ARRAYAGG(
            JSON_ARRAY(cw.type, cw.blob)
            ORDER BY cw.task_path, cw.task_id, cw.idx
        )
        FROM checkpoint_writes cw
        WHERE cw.thread_id = c.thread_id
            AND cw.checkpoint_ns = c.checkpoint_ns
            AND cw.checkpoint_id = c.parent_checkpoint_id
            AND cw.channel = '{TASKS}'
    ) as pending_sends
FROM checkpoints c
"""


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


UPSERT_CHECKPOINTS_SQL = """
    MERGE INTO checkpoints c
    USING (SELECT :1 AS thread_id, :2 AS checkpoint_ns, :3 AS checkpoint_id, :4 AS parent_checkpoint_id, :5 AS checkpoint, :6 AS metadata FROM dual) src
    ON (c.thread_id = src.thread_id AND c.checkpoint_ns = src.checkpoint_ns AND c.checkpoint_id = src.checkpoint_id)
    WHEN MATCHED THEN
        UPDATE SET checkpoint = src.checkpoint, metadata = src.metadata
    WHEN NOT MATCHED THEN
        INSERT (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
        VALUES (src.thread_id, src.checkpoint_ns, src.checkpoint_id, src.parent_checkpoint_id, src.checkpoint, src.metadata)
    """ 

UPSERT_CHECKPOINT_WRITES_SQL =  """
    MERGE INTO checkpoint_writes w
    USING (SELECT :1 AS thread_id, :2 AS checkpoint_ns, :3 AS checkpoint_id, :4 AS task_id, :5 AS task_path, :6 AS idx, :7 AS channel, :8 AS type, :9 AS blob FROM dual) src
    ON (w.thread_id = src.thread_id AND w.checkpoint_ns = src.checkpoint_ns AND w.checkpoint_id = src.checkpoint_id AND w.task_id = src.task_id AND w.idx = src.idx)
    WHEN MATCHED THEN
        UPDATE SET channel = src.channel, type = src.type, blob = src.blob, task_path = src.task_path
    WHEN NOT MATCHED THEN
        INSERT (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob)
        VALUES (src.thread_id, src.checkpoint_ns, src.checkpoint_id, src.task_id, src.task_path, src.idx, src.channel, src.type, src.blob)
    """


INSERT_CHECKPOINT_WRITES_SQL ="""
    INSERT INTO checkpoint_writes 
    (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob)
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
    """



class BaseOracleSaver(BaseCheckpointSaver[str]):
    SELECT_SQL = SELECT_SQL
    MIGRATIONS = MIGRATIONS
    UPSERT_CHECKPOINT_BLOBS_SQL = UPSERT_CHECKPOINT_BLOBS_SQL
    UPSERT_CHECKPOINTS_SQL = UPSERT_CHECKPOINTS_SQL
    UPSERT_CHECKPOINT_WRITES_SQL = UPSERT_CHECKPOINT_WRITES_SQL
    INSERT_CHECKPOINT_WRITES_SQL = INSERT_CHECKPOINT_WRITES_SQL

    jsonplus_serde = JsonPlusSerializer()
    supports_pipeline: bool

    def _load_checkpoint(
        self,
        checkpoint: dict[str, Any],
        channel_values: list[tuple[bytes, bytes, bytes]],
        pending_sends: list[tuple[bytes, bytes]],
    ) -> Checkpoint:
        return {
            **checkpoint,
            "pending_sends": [
                self.serde.loads_typed((c.decode(), b)) for c, b in pending_sends or []
            ],
            "channel_values": self._load_blobs(channel_values),
        }

    def _dump_checkpoint(self, checkpoint: Checkpoint) -> dict[str, Any]:
        return {**checkpoint, "pending_sends": []}

    def _load_blobs(
        self, blob_values: list[tuple[bytes, bytes, bytes]]
    ) -> dict[str, Any]:
        if not blob_values:
            return {}
        print("blob_values", blob_values)
        return {
            k: self.serde.loads_typed((t, v.encode()))
            for k, t, v in [(blob_values.split(','))]
            if t != "empty"
        }

    def _dump_blobs(
        self,
        thread_id: str,
        checkpoint_ns: str,
        values: dict[str, Any],
        versions: ChannelVersions,
    ) -> list[tuple[str, str, str, str, str, Optional[bytes]]]:
        
        if not versions:
            value = ("empty", None)
            print("value", [(thread_id,checkpoint_ns, 'empty', 'empty',  *self.serde.dumps_typed(value))])
            return [(thread_id,checkpoint_ns, 'empty', 'empty',  *self.serde.dumps_typed(value))]

        if not checkpoint_ns:
            checkpoint_ns = "default"

        return [
            (
                thread_id,
                checkpoint_ns,
                k,
                cast(str, ver),
                *(
                    self.serde.dumps_typed(values[k])
                    if k in values
                    else ("empty", None)
                ),
            )
            for k, ver in versions.items()
        ]

    def _load_writes(
        self, writes: list[tuple[bytes, bytes, bytes, bytes]]
    ) -> list[tuple[str, str, Any]]:
        return (
            [
                (
                    tid,
                    channel,
                    self.serde.loads_typed((t, v.encode())),
                )
                for tid, channel, t, v in [tuple(v)for v in ast.literal_eval(writes)]
            ]
            if writes
            else []
        )

    def _dump_writes(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        task_path: str,
        writes: Sequence[tuple[str, Any]],
    ) -> list[tuple[str, str, str, str, str, int, str, str, bytes]]:
        data = [
           
        ]
        for idx, (channel, value) in enumerate(writes):
            value = value if value else ("empty", None),
            data.append((
                thread_id,
                checkpoint_ns,
                checkpoint_id,
                task_id,
                task_path,
                WRITES_IDX_MAP.get(channel, idx),
                channel,
                *self.serde.dumps_typed(value)
            ))
        return data

    def _load_metadata(self, metadata: dict[str, Any]) -> CheckpointMetadata:
        return self.jsonplus_serde.loads(self.jsonplus_serde.dumps(metadata))

    def _dump_metadata(self, metadata: CheckpointMetadata) -> str:
        serialized_metadata = self.jsonplus_serde.dumps(metadata)
        # NOTE: we're using JSON serializer (not msgpack), so we need to remove null characters before writing
        return serialized_metadata.decode().replace("\\u0000", "")

    def get_next_version(self, current: Optional[str], channel: ChannelProtocol) -> str:
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"

    def _search_where(
        self,
        config: Optional[RunnableConfig],
        filter: MetadataInput,
        before: Optional[RunnableConfig] = None,
    ) -> tuple[str, list[Any]]:
        wheres = []
        param_values = []

        # Handle config filters
        if config:
            wheres.append(f"thread_id = :{len(param_values) + 1}")
            param_values.append(config["configurable"]["thread_id"])
            
            checkpoint_ns = config["configurable"].get("checkpoint_ns")
            if checkpoint_ns is not None:
                wheres.append(f"checkpoint_ns = :{len(param_values) + 1}")
                param_values.append(checkpoint_ns)
            
            if checkpoint_id := get_checkpoint_id(config):
                wheres.append(f"checkpoint_id = :{len(param_values) + 1}")
                param_values.append(checkpoint_id)

        # Handle metadata filters
        if filter:
            for key, value in flatten_metadata_filter(filter).items():
                wheres.append(f"JSON_VALUE(metadata, '$.{key}') = :{len(param_values) + 1}")
                param_values.append(str(value))  # always bind as string

        # Handle 'before'
        if before is not None:
            wheres.append(f"checkpoint_id < :{len(param_values) + 1}")
            param_values.append(get_checkpoint_id(before))

        return (
            "WHERE " + " AND ".join(wheres) if wheres else "",
            param_values,
        )

def flatten_metadata_filter(d, parent_key='', sep='.'):
    """Flattens nested dictionaries into JSON paths for Oracle JSON_VALUE queries."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_metadata_filter(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
