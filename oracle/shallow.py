import asyncio
import threading
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Optional

from langchain_core.runnables import RunnableConfig
from oracledb import (
    AsyncConnectionPool,
    Connection,
    Cursor, 
    SessionPool as ConnectionPool,
    Pipeline,
    AsyncConnection,
    AsyncCursor,
    create_pipeline as AsyncPipeline  # Fix: Import AsyncPipeline
)

from . import _ainternal, _internal
from .oracle_types import DictRow, dict_row, Capabilities, Jsonb

from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_metadata,
)
from .base import BaseOracleSaver
from langgraph.checkpoint.serde.base import SerializerProtocol
from langgraph.checkpoint.serde.types import TASKS

"""
To add a new migration, add a new string to the MIGRATIONS list.
The position of the migration in the list is the version number.
"""
MIGRATIONS = [
    """CREATE TABLE checkpoint_migrations (
    v NUMBER PRIMARY KEY
)""",
    """CREATE TABLE checkpoints (
    thread_id VARCHAR2(255) NOT NULL,
    checkpoint_ns VARCHAR2(255) DEFAULT '',
    checkpoint_id VARCHAR2(255) NOT NULL,
    checkpoint CLOB NOT NULL,
    metadata CLOB DEFAULT '{}',
    CONSTRAINT pk_checkpoints PRIMARY KEY (thread_id, checkpoint_ns)
)""",
    """CREATE TABLE checkpoint_blobs (
    thread_id VARCHAR2(255) NOT NULL,
    checkpoint_ns VARCHAR2(255) DEFAULT '',
    channel VARCHAR2(255) NOT NULL,
    type VARCHAR2(255) NOT NULL,
    blob BLOB,
    CONSTRAINT pk_checkpoint_blobs PRIMARY KEY (thread_id, checkpoint_ns, channel)
)""",
    """CREATE TABLE checkpoint_writes (
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
    """CREATE INDEX idx_checkpoints_thread_id ON checkpoints(thread_id)""",
    """CREATE INDEX idx_checkpoint_blobs_thread_id ON checkpoint_blobs(thread_id)""",
    """CREATE INDEX idx_checkpoint_writes_thread_id ON checkpoint_writes(thread_id)"""
]

SELECT_SQL = """
SELECT 
    c.thread_id,
    c.checkpoint,
    c.checkpoint_ns,
    c.metadata,
    (
        SELECT LISTAGG(channel || ',' || type || ',' || 
               UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_ENCODE(blob)), '|') 
        WITHIN GROUP (ORDER BY channel)
        FROM checkpoint_blobs b
        WHERE b.thread_id = c.thread_id 
        AND b.checkpoint_ns = c.checkpoint_ns
    ) as channel_values,
    (
        SELECT LISTAGG(task_id || ',' || channel || ',' || type || ',' || 
               UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_ENCODE(blob)), '|')
        WITHIN GROUP (ORDER BY task_id, idx)
        FROM checkpoint_writes w
        WHERE w.thread_id = c.thread_id 
        AND w.checkpoint_ns = c.checkpoint_ns
        AND w.checkpoint_id = c.checkpoint_id
    ) as pending_writes,
    (
        SELECT LISTAGG(type || ',' || 
               UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_ENCODE(blob)), '|')
        WITHIN GROUP (ORDER BY task_path, task_id, idx)
        FROM checkpoint_writes w
        WHERE w.thread_id = c.thread_id 
        AND w.checkpoint_ns = c.checkpoint_ns
        AND w.channel = '{TASKS}'
    ) as pending_sends
FROM checkpoints c
"""

UPSERT_CHECKPOINT_BLOBS_SQL = """
MERGE INTO checkpoint_blobs dst
USING (SELECT :1 as thread_id, :2 as checkpoint_ns, :3 as channel, :4 as type, :5 as blob FROM dual) src
ON (dst.thread_id = src.thread_id AND dst.checkpoint_ns = src.checkpoint_ns AND dst.channel = src.channel)
WHEN MATCHED THEN
    UPDATE SET type = src.type, blob = src.blob
WHEN NOT MATCHED THEN
    INSERT (thread_id, checkpoint_ns, channel, type, blob)
    VALUES (src.thread_id, src.checkpoint_ns, src.channel, src.type, src.blob)
"""

UPSERT_CHECKPOINTS_SQL = """
MERGE INTO checkpoints dst
USING (SELECT :1 as thread_id, :2 as checkpoint_ns, :3 as checkpoint_id, :4 as checkpoint, :5 as metadata FROM dual) src
ON (dst.thread_id = src.thread_id AND dst.checkpoint_ns = src.checkpoint_ns)
WHEN MATCHED THEN
    UPDATE SET checkpoint_id = src.checkpoint_id, checkpoint = src.checkpoint, metadata = src.metadata
WHEN NOT MATCHED THEN
    INSERT (thread_id, checkpoint_ns, checkpoint_id, checkpoint, metadata)
    VALUES (src.thread_id, src.checkpoint_ns, src.checkpoint_id, src.checkpoint, src.metadata)
"""

UPSERT_CHECKPOINT_WRITES_SQL = """
MERGE INTO checkpoint_writes dst
USING (SELECT :1 as thread_id, :2 as checkpoint_ns, :3 as checkpoint_id, :4 as task_id, 
              :5 as task_path, :6 as idx, :7 as channel, :8 as type, :9 as blob FROM dual) src
ON (dst.thread_id = src.thread_id AND dst.checkpoint_ns = src.checkpoint_ns 
    AND dst.checkpoint_id = src.checkpoint_id AND dst.task_id = src.task_id AND dst.idx = src.idx)
WHEN MATCHED THEN
    UPDATE SET channel = src.channel, type = src.type, blob = src.blob
WHEN NOT MATCHED THEN
    INSERT (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob)
    VALUES (src.thread_id, src.checkpoint_ns, src.checkpoint_id, src.task_id, src.task_path, src.idx, src.channel, src.type, src.blob)
"""

INSERT_CHECKPOINT_WRITES_SQL = """
INSERT INTO checkpoint_writes 
    (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob)
VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
"""

def _dump_blobs(
    serde: SerializerProtocol,
    thread_id: str,
    checkpoint_ns: str,
    values: dict[str, Any],
    versions: ChannelVersions,
) -> list[tuple[str, str, str, str, str, Optional[bytes]]]:
    if not versions:
        return []
    if not checkpoint_ns:
        checkpoint_ns = ""
    return [
        (
            thread_id,
            checkpoint_ns,
            k,
            *(serde.dumps_typed(values[k]) if k in values else ("empty", None)),
        )
        for k in versions
    ]



class ShallowOracleSaver(BaseOracleSaver):
    """A checkpoint saver that uses Postgres to store checkpoints.

    This checkpointer ONLY stores the most recent checkpoint and does NOT retain any history.
    It is meant to be a light-weight drop-in replacement for the PostgresSaver that
    supports most of the LangGraph persistence functionality with the exception of time travel.
    """

    SELECT_SQL = SELECT_SQL
    MIGRATIONS = MIGRATIONS
    UPSERT_CHECKPOINT_BLOBS_SQL = UPSERT_CHECKPOINT_BLOBS_SQL
    UPSERT_CHECKPOINTS_SQL = UPSERT_CHECKPOINTS_SQL
    UPSERT_CHECKPOINT_WRITES_SQL = UPSERT_CHECKPOINT_WRITES_SQL
    INSERT_CHECKPOINT_WRITES_SQL = INSERT_CHECKPOINT_WRITES_SQL

    lock: threading.Lock

    def __init__(
        self,
        conn: _internal.Conn,
        pipe: Optional[Pipeline] = None,
        serde: Optional[SerializerProtocol] = None,
    ) -> None:
        super().__init__(serde=serde)
        if isinstance(conn, ConnectionPool) and pipe is not None:
            raise ValueError(
                "Pipeline should be used only with a single Connection, not ConnectionPool."
            )

        self.conn = conn
        self.pipe = pipe
        self.lock = threading.Lock()
        self.supports_pipeline = Capabilities().has_pipeline()

    @classmethod
    @contextmanager
    def from_conn_string(
        cls, conn_string: str, *, pipeline: bool = False
    ) -> Iterator["ShallowOracleSaver"]:
        """Create a new ShallowOracleSaver instance from a connection string.

        Args:
            conn_string (str): The Postgres connection info string.
            pipeline (bool): whether to use Pipeline

        Returns:
            ShallowOracleSaver: A new ShallowOracleSaver instance.
        """
        with Connection.connect(
            conn_string, autocommit=True, prepare_threshold=0, row_factory=dict_row
        ) as conn:
            if pipeline:
                with conn.pipeline() as pipe:
                    yield cls(conn, pipe)
            else:
                yield cls(conn)

    def setup(self) -> None:
        with self._cursor() as cur:
            cur.execute(self.MIGRATIONS[0])
            results = cur.execute(
                "SELECT v FROM checkpoint_migrations ORDER BY v DESC FETCH FIRST 1 ROW ONLY"
            )
            row = results.fetchone()
            if row is None:
                version = -1
            else:
                version = row[0]  # Access first column by index instead of name
            for v, migration in zip(
                range(version + 1, len(self.MIGRATIONS)),
                self.MIGRATIONS[version + 1 :],
            ):
                cur.execute(migration)
                cur.execute(f"INSERT INTO checkpoint_migrations (v) VALUES ({v})")
        if self.pipe:
            self.pipe.sync()

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints from the database.

        This method retrieves a list of checkpoint tuples from the Postgres database based
        on the provided config. For ShallowOracleSaver, this method returns a list with
        ONLY the most recent checkpoint.
        """
        where, args = self._search_where(config, filter, before)
        query = self.SELECT_SQL + where
        if limit:
            query += f" LIMIT {limit}"
        with self._cursor() as cur:
            cur.execute(self.SELECT_SQL + where, args, binary=True)
            for value in cur:
                checkpoint = self._load_checkpoint(
                    value["checkpoint"],
                    value["channel_values"],
                    value["pending_sends"],
                )
                yield CheckpointTuple(
                    config={
                        "configurable": {
                            "thread_id": value["thread_id"],
                            "checkpoint_ns": value["checkpoint_ns"],
                            "checkpoint_id": checkpoint["id"],
                        }
                    },
                    checkpoint=checkpoint,
                    metadata=self._load_metadata(value["metadata"]),
                    pending_writes=self._load_writes(value["pending_writes"]),
                )

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """Get a checkpoint tuple from the database.

        This method retrieves a checkpoint tuple from the Postgres database based on the
        provided config (matching the thread ID in the config).

        Args:
            config (RunnableConfig): The config to use for retrieving the checkpoint.

        Returns:
            Optional[CheckpointTuple]: The retrieved checkpoint tuple, or None if no matching checkpoint was found.

        Examples:

            Basic:
            >>> config = {"configurable": {"thread_id": "1"}}
            >>> checkpoint_tuple = memory.get_tuple(config)
            >>> print(checkpoint_tuple)
            CheckpointTuple(...)

            With timestamp:

            >>> config = {
            ...    "configurable": {
            ...        "thread_id": "1",
            ...        "checkpoint_ns": "",
            ...        "checkpoint_id": "1ef4f797-8335-6428-8001-8a1503f9b875",
            ...    }
            ... }
            >>> checkpoint_tuple = memory.get_tuple(config)
            >>> print(checkpoint_tuple)
            CheckpointTuple(...)
        """  # noqa
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        args = (thread_id, checkpoint_ns)
        where = "WHERE thread_id = %s AND checkpoint_ns = %s"

        with self._cursor() as cur:
            cur.execute(
                self.SELECT_SQL + where,
                args,
                binary=True,
            )

            for value in cur:
                checkpoint = self._load_checkpoint(
                    value["checkpoint"],
                    value["channel_values"],
                    value["pending_sends"],
                )
                return CheckpointTuple(
                    config={
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": checkpoint["id"],
                        }
                    },
                    checkpoint=checkpoint,
                    metadata=self._load_metadata(value["metadata"]),
                    pending_writes=self._load_writes(value["pending_writes"]),
                )

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Save a checkpoint to the database.

        This method saves a checkpoint to the Postgres database. The checkpoint is associated
        with the provided config. For ShallowOracleSaver, this method saves ONLY the most recent
        checkpoint and overwrites a previous checkpoint, if it exists.

        Args:
            config (RunnableConfig): The config to associate with the checkpoint.
            checkpoint (Checkpoint): The checkpoint to save.
            metadata (CheckpointMetadata): Additional metadata to save with the checkpoint.
            new_versions (ChannelVersions): New channel versions as of this write.

        Returns:
            RunnableConfig: Updated configuration after storing the checkpoint.

        Examples:

            >>> from langgraph.checkpoint.postgres import ShallowOracleSaver
            >>> DB_URI = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
            >>> with ShallowOracleSaver.from_conn_string(DB_URI) as memory:
            >>>     config = {"configurable": {"thread_id": "1", "checkpoint_ns": ""}}
            >>>     checkpoint = {"ts": "2024-05-04T06:32:42.235444+00:00", "id": "1ef4f797-8335-6428-8001-8a1503f9b875", "channel_values": {"key": "value"}}
            >>>     saved_config = memory.put(config, checkpoint, {"source": "input", "step": 1, "writes": {"key": "value"}}, {})
            >>> print(saved_config)
            {'configurable': {'thread_id': '1', 'checkpoint_ns': '', 'checkpoint_id': '1ef4f797-8335-6428-8001-8a1503f9b875'}}
        """
        configurable = config["configurable"].copy()
        thread_id = configurable.pop("thread_id")
        checkpoint_ns = configurable.pop("checkpoint_ns")

        copy = checkpoint.copy()
        next_config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

        with self._cursor(pipeline=True) as cur:
            cur.execute(
                """DELETE FROM checkpoint_writes
                WHERE thread_id = %s AND checkpoint_ns = %s AND checkpoint_id NOT IN (%s, %s)""",
                (
                    thread_id,
                    checkpoint_ns,
                    checkpoint["id"],
                    configurable.get("checkpoint_id", ""),
                ),
            )
            cur.executemany(
                self.UPSERT_CHECKPOINT_BLOBS_SQL,
                _dump_blobs(
                    self.serde,
                    thread_id,
                    checkpoint_ns,
                    copy.pop("channel_values"),  # type: ignore[misc]
                    new_versions,
                ),
            )
            cur.execute(
                self.UPSERT_CHECKPOINTS_SQL,
                (
                    thread_id,
                    checkpoint_ns,
                    Jsonb(self._dump_checkpoint(copy)),
                    self._dump_metadata(get_checkpoint_metadata(config, metadata)),
                ),
            )
        return next_config

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store intermediate writes linked to a checkpoint.

        This method saves intermediate writes associated with a checkpoint to the Postgres database.

        Args:
            config (RunnableConfig): Configuration of the related checkpoint.
            writes (List[Tuple[str, Any]]): List of writes to store.
            task_id (str): Identifier for the task creating the writes.
        """
        query = (
            self.UPSERT_CHECKPOINT_WRITES_SQL
            if all(w[0] in WRITES_IDX_MAP for w in writes)
            else self.INSERT_CHECKPOINT_WRITES_SQL
        )
        with self._cursor(pipeline=True) as cur:
            cur.executemany(
                query,
                self._dump_writes(
                    config["configurable"]["thread_id"],
                    config["configurable"]["checkpoint_ns"],
                    config["configurable"]["checkpoint_id"],
                    task_id,
                    task_path,
                    writes,
                ),
            )

    @contextmanager
    def _cursor(self, *, pipeline: bool = False) -> Iterator[Cursor]:
        """Create a database cursor as a context manager.

        Args:
            pipeline (bool): whether to use pipeline for the DB operations inside the context manager.
                Will be applied regardless of whether the ShallowOracleSaver instance was initialized with a pipeline.
                If pipeline mode is not supported, will fall back to using transaction context manager.
        """
        with _internal.get_connection(self.conn) as conn:
            if self.pipe:
                # a connection in pipeline mode can be used concurrently
                # in multiple threads/coroutines, but only one cursor can be
                # used at a time
                try:
                    with conn.cursor(binary=True, row_factory=dict_row) as cur:
                        yield cur
                finally:
                    if pipeline:
                        self.pipe.sync()
            elif pipeline:
                # a connection not in pipeline mode can only be used by one
                # thread/coroutine at a time, so we acquire a lock
                if self.supports_pipeline:
                    with (
                        self.lock,
                        conn.pipeline(),
                        conn.cursor(binary=True, row_factory=dict_row) as cur,
                    ):
                        yield cur
                else:
                    # Use connection's transaction context manager when pipeline mode not supported
                    with (
                        self.lock,
                        conn.transaction(),
                        conn.cursor(binary=True, row_factory=dict_row) as cur,
                    ):
                        yield cur
            else:
                with self.lock, conn.cursor(binary=True, row_factory=dict_row) as cur:
                    yield cur


class AsyncShallowOracleSaver(BaseOracleSaver):
    """A checkpoint saver that uses Postgres to store checkpoints asynchronously.

    This checkpointer ONLY stores the most recent checkpoint and does NOT retain any history.
    It is meant to be a light-weight drop-in replacement for the AsyncPostgresSaver that
    supports most of the LangGraph persistence functionality with the exception of time travel.
    """

    SELECT_SQL = SELECT_SQL
    MIGRATIONS = MIGRATIONS
    UPSERT_CHECKPOINT_BLOBS_SQL = UPSERT_CHECKPOINT_BLOBS_SQL
    UPSERT_CHECKPOINTS_SQL = UPSERT_CHECKPOINTS_SQL
    UPSERT_CHECKPOINT_WRITES_SQL = UPSERT_CHECKPOINT_WRITES_SQL
    INSERT_CHECKPOINT_WRITES_SQL = INSERT_CHECKPOINT_WRITES_SQL
    lock: asyncio.Lock

    def __init__(
        self,
        conn: _ainternal.Conn,
        pipe: Optional[AsyncPipeline] = None,
        serde: Optional[SerializerProtocol] = None,
    ) -> None:
        super().__init__(serde=serde)
        if isinstance(conn, AsyncConnectionPool) and pipe is not None:
            raise ValueError(
                "Pipeline should be used only with a single AsyncConnection, not AsyncConnectionPool."
            )

        self.conn = conn
        self.pipe = pipe
        self.lock = asyncio.Lock()
        self.loop = asyncio.get_running_loop()
        self.supports_pipeline = Capabilities().has_pipeline()

    @classmethod
    @asynccontextmanager
    async def from_conn_string(
        cls,
        conn_string: str,
        *,
        pipeline: bool = False,
        serde: Optional[SerializerProtocol] = None,
    ) -> AsyncIterator["AsyncShallowOracleSaver"]:
        """Create a new AsyncShallowOracleSaver instance from a connection string.

        Args:
            conn_string (str): The Postgres connection info string.
            pipeline (bool): whether to use AsyncPipeline

        Returns:
            AsyncShallowOracleSaver: A new AsyncShallowOracleSaver instance.
        """
        async with await AsyncConnection.connect(
            conn_string, autocommit=True, prepare_threshold=0, row_factory=dict_row
        ) as conn:
            if pipeline:
                async with conn.pipeline() as pipe:
                    yield cls(conn=conn, pipe=pipe, serde=serde)
            else:
                yield cls(conn=conn, serde=serde)

    async def setup(self) -> None:
        """Set up the checkpoint database asynchronously.

        This method creates the necessary tables in the Postgres database if they don't
        already exist and runs database migrations. It MUST be called directly by the user
        the first time checkpointer is used.
        """
        async with self._cursor() as cur:
            await cur.execute(self.MIGRATIONS[0])
            results = await cur.execute(
                "SELECT v FROM checkpoint_migrations ORDER BY v DESC LIMIT 1"
            )
            row = await results.fetchone()
            if row is None:
                version = -1
            else:
                version = row["v"]
            for v, migration in zip(
                range(version + 1, len(self.MIGRATIONS)),
                self.MIGRATIONS[version + 1 :],
            ):
                await cur.execute(migration)
                await cur.execute(f"INSERT INTO checkpoint_migrations (v) VALUES ({v})")
        if self.pipe:
            await self.pipe.sync()

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """List checkpoints from the database asynchronously.

        This method retrieves a list of checkpoint tuples from the Postgres database based
        on the provided config. For ShallowOracleSaver, this method returns a list with
        ONLY the most recent checkpoint.
        """
        where, args = self._search_where(config, filter, before)
        query = self.SELECT_SQL + where
        if limit:
            query += f" LIMIT {limit}"
        async with self._cursor() as cur:
            await cur.execute(self.SELECT_SQL + where, args, binary=True)
            async for value in cur:
                checkpoint = await asyncio.to_thread(
                    self._load_checkpoint,
                    value["checkpoint"],
                    value["channel_values"],
                    value["pending_sends"],
                )
                yield CheckpointTuple(
                    config={
                        "configurable": {
                            "thread_id": value["thread_id"],
                            "checkpoint_ns": value["checkpoint_ns"],
                            "checkpoint_id": checkpoint["id"],
                        }
                    },
                    checkpoint=checkpoint,
                    metadata=self._load_metadata(value["metadata"]),
                    pending_writes=await asyncio.to_thread(
                        self._load_writes, value["pending_writes"]
                    ),
                )

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """Get a checkpoint tuple from the database asynchronously.

        This method retrieves a checkpoint tuple from the Postgres database based on the
        provided config (matching the thread ID in the config).

        Args:
            config (RunnableConfig): The config to use for retrieving the checkpoint.

        Returns:
            Optional[CheckpointTuple]: The retrieved checkpoint tuple, or None if no matching checkpoint was found.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        args = (thread_id, checkpoint_ns)
        where = "WHERE thread_id = %s AND checkpoint_ns = %s"

        async with self._cursor() as cur:
            await cur.execute(
                self.SELECT_SQL + where,
                args,
                binary=True,
            )

            async for value in cur:
                checkpoint = await asyncio.to_thread(
                    self._load_checkpoint,
                    value["checkpoint"],
                    value["channel_values"],
                    value["pending_sends"],
                )
                return CheckpointTuple(
                    config={
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": checkpoint["id"],
                        }
                    },
                    checkpoint=checkpoint,
                    metadata=self._load_metadata(value["metadata"]),
                    pending_writes=await asyncio.to_thread(
                        self._load_writes, value["pending_writes"]
                    ),
                )

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Save a checkpoint to the database asynchronously.

        This method saves a checkpoint to the Postgres database. The checkpoint is associated
        with the provided config. For AsyncShallowOracleSaver, this method saves ONLY the most recent
        checkpoint and overwrites a previous checkpoint, if it exists.

        Args:
            config (RunnableConfig): The config to associate with the checkpoint.
            checkpoint (Checkpoint): The checkpoint to save.
            metadata (CheckpointMetadata): Additional metadata to save with the checkpoint.
            new_versions (ChannelVersions): New channel versions as of this write.

        Returns:
            RunnableConfig: Updated configuration after storing the checkpoint.
        """
        configurable = config["configurable"].copy()
        thread_id = configurable.pop("thread_id")
        checkpoint_ns = configurable.pop("checkpoint_ns")

        copy = checkpoint.copy()
        next_config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

        async with self._cursor(pipeline=True) as cur:
            await cur.execute(
                """DELETE FROM checkpoint_writes
                WHERE thread_id = %s AND checkpoint_ns = %s AND checkpoint_id NOT IN (%s, %s)""",
                (
                    thread_id,
                    checkpoint_ns,
                    checkpoint["id"],
                    configurable.get("checkpoint_id", ""),
                ),
            )
            await cur.executemany(
                self.UPSERT_CHECKPOINT_BLOBS_SQL,
                _dump_blobs(
                    self.serde,
                    thread_id,
                    checkpoint_ns,
                    copy.pop("channel_values"),  # type: ignore[misc]
                    new_versions,
                ),
            )
            await cur.execute(
                self.UPSERT_CHECKPOINTS_SQL,
                (
                    thread_id,
                    checkpoint_ns,
                    Jsonb(self._dump_checkpoint(copy)),
                    self._dump_metadata(get_checkpoint_metadata(config, metadata)),
                ),
            )
        return next_config

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store intermediate writes linked to a checkpoint asynchronously.

        This method saves intermediate writes associated with a checkpoint to the database.

        Args:
            config (RunnableConfig): Configuration of the related checkpoint.
            writes (Sequence[Tuple[str, Any]]): List of writes to store, each as (channel, value) pair.
            task_id (str): Identifier for the task creating the writes.
        """
        query = (
            self.UPSERT_CHECKPOINT_WRITES_SQL
            if all(w[0] in WRITES_IDX_MAP for w in writes)
            else self.INSERT_CHECKPOINT_WRITES_SQL
        )
        params = await asyncio.to_thread(
            self._dump_writes,
            config["configurable"]["thread_id"],
            config["configurable"]["checkpoint_ns"],
            config["configurable"]["checkpoint_id"],
            task_id,
            task_path,
            writes,
        )
        async with self._cursor(pipeline=True) as cur:
            await cur.executemany(query, params)

    @asynccontextmanager
    async def _cursor(
        self, *, pipeline: bool = False
    ) -> AsyncIterator[AsyncCursor]:
        """Create a database cursor as a context manager.

        Args:
            pipeline (bool): whether to use pipeline for the DB operations inside the context manager.
                Will be applied regardless of whether the AsyncShallowOracleSaver instance was initialized with a pipeline.
                If pipeline mode is not supported, will fall back to using transaction context manager.
        """
        async with _ainternal.get_connection(self.conn) as conn:
            if self.pipe:
                # a connection in pipeline mode can be used concurrently
                # in multiple threads/coroutines, but only one cursor can be
                # used at a time
                try:
                    async with conn.cursor(binary=True, row_factory=dict_row) as cur:
                        yield cur
                finally:
                    if pipeline:
                        await self.pipe.sync()
            elif pipeline:
                # a connection not in pipeline mode can only be used by one
                # thread/coroutine at a time, so we acquire a lock
                if self.supports_pipeline:
                    async with (
                        self.lock,
                        conn.pipeline(),
                        conn.cursor(binary=True, row_factory=dict_row) as cur,
                    ):
                        yield cur
                else:
                    # Use connection's transaction context manager when pipeline mode not supported
                    async with (
                        self.lock,
                        conn.transaction(),
                        conn.cursor(binary=True, row_factory=dict_row) as cur,
                    ):
                        yield cur
            else:
                async with (
                    self.lock,
                    conn.cursor(binary=True, row_factory=dict_row),
                ):
                    yield cur

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints from the database.

        This method retrieves a list of checkpoint tuples from the Postgres database based
        on the provided config. For ShallowOracleSaver, this method returns a list with
        ONLY the most recent checkpoint.
        """
        aiter_ = self.alist(config, filter=filter, before=before, limit=limit)
        while True:
            try:
                yield asyncio.run_coroutine_threadsafe(
                    anext(aiter_),  # noqa: F821
                    self.loop,
                ).result()
            except StopAsyncIteration:
                break

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """Get a checkpoint tuple from the database.

        This method retrieves a checkpoint tuple from the Postgres database based on the
        provided config (matching the thread ID in the config).

        Args:
            config (RunnableConfig): The config to use for retrieving the checkpoint.

        Returns:
            Optional[CheckpointTuple]: The retrieved checkpoint tuple, or None if no matching checkpoint was found.
        """
        try:
            # check if we are in the main thread, only bg threads can block
            # we don't check in other methods to avoid the overhead
            if asyncio.get_running_loop() is self.loop:
                raise asyncio.InvalidStateError(
                    "Synchronous calls to AsyncShallowOracleSaver are only allowed from a "
                    "different thread. From the main thread, use the async interface."
                    "For example, use `await checkpointer.aget_tuple(...)` or `await "
                    "graph.ainvoke(...)`."
                )
        except RuntimeError:
            pass
        return asyncio.run_coroutine_threadsafe(
            self.aget_tuple(config), self.loop
        ).result()

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Save a checkpoint to the database.

        This method saves a checkpoint to the Postgres database. The checkpoint is associated
        with the provided config. For AsyncShallowOracleSaver, this method saves ONLY the most recent
        checkpoint and overwrites a previous checkpoint, if it exists.

        Args:
            config (RunnableConfig): The config to associate with the checkpoint.
            checkpoint (Checkpoint): The checkpoint to save.
            metadata (CheckpointMetadata): Additional metadata to save with the checkpoint.
            new_versions (ChannelVersions): New channel versions as of this write.

        Returns:
            RunnableConfig: Updated configuration after storing the checkpoint.
        """
        return asyncio.run_coroutine_threadsafe(
            self.aput(config, checkpoint, metadata, new_versions), self.loop
        ).result()

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store intermediate writes linked to a checkpoint.

        This method saves intermediate writes associated with a checkpoint to the database.

        Args:
            config (RunnableConfig): Configuration of the related checkpoint.
            writes (Sequence[Tuple[str, Any]]): List of writes to store, each as (channel, value) pair.
            task_id (str): Identifier for the task creating the writes.
            task_path (str): Path of the task creating the writes.
        """
        return asyncio.run_coroutine_threadsafe(
            self.aput_writes(config, writes, task_id, task_path), self.loop
        ).result()
