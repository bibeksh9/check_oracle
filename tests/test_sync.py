import re
from contextlib import contextmanager
from typing import Any
from uuid import uuid4

import pytest
from langchain_core.runnables import RunnableConfig
import oracledb
from oracledb import Connection, SessionPool

from langgraph.checkpoint.base import (
    EXCLUDED_METADATA_KEYS,
    Checkpoint,
    CheckpointMetadata,
    create_checkpoint,
    empty_checkpoint,
)
from oracle import OracleSaver, ShallowOracleSaver
from .conftest import DEFAULT_ORACLE_CONFIG

def _exclude_keys(config: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in config.items() if k not in EXCLUDED_METADATA_KEYS}

@contextmanager 
def _pool_saver():
    """Fixture for pool mode testing."""
    pool = oracledb.connect(
        user=DEFAULT_ORACLE_CONFIG["user"],
        password=DEFAULT_ORACLE_CONFIG["password"],
        dsn=f"{DEFAULT_ORACLE_CONFIG['host']}:{DEFAULT_ORACLE_CONFIG['port']}/{DEFAULT_ORACLE_CONFIG['service_name']}"
    )
    pool.autocommit = True
    try:
        checkpointer = OracleSaver(pool)
        checkpointer.setup()
        yield checkpointer
    finally:
        pool.close()

@contextmanager
def _base_saver():
    """Fixture for regular connection mode testing."""
    conn = oracledb.connect(
        user=DEFAULT_ORACLE_CONFIG["user"],
        password=DEFAULT_ORACLE_CONFIG["password"],
        dsn=f"{DEFAULT_ORACLE_CONFIG['host']}:{DEFAULT_ORACLE_CONFIG['port']}/{DEFAULT_ORACLE_CONFIG['service_name']}"
    )
    conn.autocommit = True
    try:
        checkpointer = OracleSaver(conn)
        checkpointer.setup()
        yield checkpointer
    finally:
        conn.close()

@contextmanager
def _shallow_saver():
    """Fixture for regular connection mode testing with a shallow checkpointer."""
    conn = oracledb.connect(
        user=DEFAULT_ORACLE_CONFIG["user"],
        password=DEFAULT_ORACLE_CONFIG["password"],
        dsn=f"{DEFAULT_ORACLE_CONFIG['host']}:{DEFAULT_ORACLE_CONFIG['port']}/{DEFAULT_ORACLE_CONFIG['service_name']}"
    )
    conn.autocommit = True
    try:
        checkpointer = ShallowOracleSaver(conn)
        checkpointer.setup()
        yield checkpointer
    finally:
        conn.close()

@contextmanager
def _saver(name: str):
    if name == "base":
        with _base_saver() as saver:
            yield saver
    elif name == "shallow":
        with _shallow_saver() as saver:
            yield saver
    elif name == "pool":
        with _pool_saver() as saver:
            yield saver

@pytest.fixture
def test_data():
    """Fixture providing test data for checkpoint tests."""
    config_1: RunnableConfig = {
        "configurable": {
            "thread_id": "thread-1",
            "thread_ts": "1",
            "checkpoint_ns": "oracle",
        }
    }
    config_2: RunnableConfig = {
        "configurable": {
            "thread_id": "thread-2", 
            "checkpoint_id": "2",
            "checkpoint_ns": "oracle",
        }
    }
    config_3: RunnableConfig = {
        "configurable": {
            "thread_id": "thread-2",
            "checkpoint_id": "2-inner",
            "checkpoint_ns": "inner",
        }
    }

    chkpnt_1: Checkpoint = empty_checkpoint()
    chkpnt_2: Checkpoint = create_checkpoint(chkpnt_1, {}, 1)
    chkpnt_3: Checkpoint = empty_checkpoint()

    metadata_1: CheckpointMetadata = {
        "source": "input",
        "step": 2,
        "writes": {},
        "score": 1,
    }
    metadata_2: CheckpointMetadata = {
        "source": "loop",
        "step": 1,
        "writes": {"foo": "bar"},
        "score": None,
    }
    metadata_3: CheckpointMetadata = {}

    return {
        "configs": [config_1, config_2, config_3],
        "checkpoints": [chkpnt_1, chkpnt_2, chkpnt_3],
        "metadata": [metadata_1, metadata_2, metadata_3],
    }

@pytest.mark.parametrize("saver_name", ["base", "pool", "shallow"])
def test_combined_metadata(saver_name: str, test_data) -> None:
    with _saver(saver_name) as saver:
        config = {
            "configurable": {
                "thread_id": "thread-pool",
                "checkpoint_ns": "oracle",
                "__super_private_key": "super_private_value",
            },
            "metadata": {"run_id": "my_run_id"},
        }
        chkpnt: Checkpoint = create_checkpoint(empty_checkpoint(), {}, 1)
        metadata: CheckpointMetadata = {
            "source": "loop",
            "step": 1,
            "writes": {"foo": "bar"},
            "score": None,
        }
        saver.put(config, chkpnt, metadata, {})
        checkpoint = saver.get_tuple(config)
        assert checkpoint.metadata == {
            **metadata,
            "thread_id": "thread-pool",
            "run_id": "my_run_id",
        }

@pytest.mark.parametrize("saver_name", ["base", "pool", "shallow"]) 
def test_search(saver_name: str, test_data) -> None:
    with _saver(saver_name) as saver:
        configs = test_data["configs"]
        checkpoints = test_data["checkpoints"]
        metadata = test_data["metadata"]

        saver.put(configs[0], checkpoints[0], metadata[0], {})
        saver.put(configs[1], checkpoints[1], metadata[1], {})
        saver.put(configs[2], checkpoints[2], metadata[2], {})

        query_1 = {"source": "input"}
        query_2 = {
            "step": 1,
            "writes": {"foo": "bar"},
        }
        query_3: dict[str, Any] = {}
        query_4 = {"source": "update", "step": 1}

        search_results_1 = list(saver.list(None, filter=query_1))
        assert len(search_results_1) == 1
        assert search_results_1[0].metadata == {
            **_exclude_keys(configs[0]["configurable"]),
            **metadata[0],
        }

        search_results_2 = list(saver.list(None, filter=query_2))
        assert len(search_results_2) == 1
        assert search_results_2[0].metadata == {
            **_exclude_keys(configs[1]["configurable"]),
            **metadata[1],
        }

        search_results_3 = list(saver.list(None, filter=query_3))
        assert len(search_results_3) == 3

        search_results_4 = list(saver.list(None, filter=query_4))
        assert len(search_results_4) == 0

        search_results_5 = list(saver.list({"configurable": {"thread_id": "thread-2"}}))
        assert len(search_results_5) == 2
        assert {
            search_results_5[0].config["configurable"]["checkpoint_ns"],
            search_results_5[1].config["configurable"]["checkpoint_ns"],
        } == {"oracle", "inner"}

@pytest.mark.parametrize("saver_name", ["base", "pool", "shallow"])
def test_null_chars(saver_name: str, test_data) -> None:
    with _saver(saver_name) as saver:
        config = saver.put(
            test_data["configs"][0],
            test_data["checkpoints"][0],
            {"my_key": "\x00abc"},
            {},
        )
        assert saver.get_tuple(config).metadata["my_key"] == "abc"
        assert (
            list(saver.list(None, filter={"my_key": "abc"}))[0].metadata["my_key"]
            == "abc"
        )

def test_nonnull_migrations() -> None:
    _leading_comment_remover = re.compile(r"^/\*.*?\*/")
    for migration in OracleSaver.MIGRATIONS:
        statement = _leading_comment_remover.sub("", migration).split()[0]
        assert statement.strip()