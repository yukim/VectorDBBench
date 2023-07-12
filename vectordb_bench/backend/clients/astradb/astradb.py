import logging
from contextlib import contextmanager
from typing import Type
from ..api import DBCaseConfig, VectorDB, DBConfig, EmptyDBCaseConfig, IndexType
from .config import AstraDBConfig

CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS %s (
    id INT,
    embedding VECTOR<FLOAT, %s>,
    PRIMARY KEY (id)
);
"""
CREATE_INDEX_TEMPLATE = """
CREATE CUSTOM INDEX IF NOT EXISTS %s_index
ON %s(embedding) USING 'StorageAttachedIndex';
"""
DROP_TABLE_TEMPLATE = """
DROP TABLE IF EXISTS %s;
"""
DROP_INDEX_TEMPLATE = """
DROP INDEX IF EXISTS %s_index;
"""
INSERT_PREPARED = """
INSERT INTO %s (id, embedding) VALUES (?, ?);
"""
SELECT_PREPARED = """
SELECT id FROM %s ORDER BY embedding ANN OF ? LIMIT ?;
"""

log = logging.getLogger(__name__) 

class AstraDB(VectorDB):
    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: DBCaseConfig | None,
        collection_name: str = "astradb_collection",
        drop_old: bool = False,
        **kwargs,
    ) -> None:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider

        self.keyspace = db_config['keyspace']
        self.table = collection_name
        self.cloud_config= {
            'secure_connect_bundle': db_config['cloud_config']
        }
        self.auth_provider = PlainTextAuthProvider(db_config['client_id'], db_config['client_secret'])

        cluster = Cluster(cloud=self.cloud_config, auth_provider=self.auth_provider)
        session = cluster.connect()
        session.set_keyspace(self.keyspace)

        if drop_old:
            log.info(f"Dropping table: {collection_name}")
            session.execute(DROP_TABLE_TEMPLATE%collection_name)
            session.execute(DROP_INDEX_TEMPLATE%collection_name)
        session.execute(CREATE_TABLE_TEMPLATE%(collection_name, dim))
        session.execute(CREATE_INDEX_TEMPLATE%(collection_name, collection_name))
        session.shutdown()

    @classmethod
    def config_cls(cls) -> Type[DBConfig]:
        return AstraDBConfig

    @classmethod
    def case_config_cls(cls, index_type: IndexType | None = None) -> Type[DBCaseConfig]:
        return EmptyDBCaseConfig

    @contextmanager
    def init(self):
        from cassandra.cluster import Cluster
        cluster = Cluster(cloud=self.cloud_config, auth_provider=self.auth_provider)
        self.session = cluster.connect()
        try:
            self.session.set_keyspace(self.keyspace)
            self._insert = self.session.prepare(INSERT_PREPARED%self.table)
            self._select = self.session.prepare(SELECT_PREPARED%self.table)
            log.info("prepare completed")
            yield
        finally:
            log.info("shutting down")
            self.session.shutdown()

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        **kwargs,
    ) -> (int, Exception):
        from cassandra.concurrent import execute_concurrent_with_args

        params = [(metadata[i], embeddings[i]) for i in range(len(metadata))]
        results = execute_concurrent_with_args(self.session, self._insert, params, concurrency=16, raise_on_first_error=False)
        successful_results = list(filter(lambda result: result.success, results))
        if len(successful_results) == len(results):
            return len(successful_results), None
        else:
            failed_results = filter(lambda result: not result.success, results)
            error = next(failed_results).result_or_exc
            log.warning(f"Failed to insert data, error: {error}")   
            return len(successful_results), error

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        filters: dict | None = None,
    ) -> list[int]:
        # filtering search not supported yet
        from cassandra.concurrent import execute_concurrent

        results = execute_concurrent(self.session, (self._select, (query, k)), concurrency=16, raise_on_first_error=True)
        successful_results = filter(lambda result: result.success, results)
        return [row.id for result in successful_results for row in result[1]]

    def optimize(self):
        pass

    def ready_to_load(self):
        pass