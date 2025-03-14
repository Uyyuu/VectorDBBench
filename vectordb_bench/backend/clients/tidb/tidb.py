import json
import uuid
import logging
from collections.abc import Generator
from contextlib import contextmanager

import mysql.connector
from mysql.connector.connection import MySQLConnection, MySQLCursor

from ..api import VectorDB
from .config import TiDBConfig, TiDBIndexConfig

log = logging.getLogger(__name__)


class TiDB(VectorDB):
    conn: MySQLConnection | None = None
    cursor: MySQLCursor | None = None

    def __init__(
        self,
        dim: int,
        db_config: TiDBConfig,
        db_case_config: TiDBIndexConfig | None,
        collection_name: str = "tidb_vector_collection",
        drop_old: bool = False,
        **kwargs,
    ) -> None:
        self.name = "vectors"
        self.db_config = db_config
        self.case_config = db_case_config
        self.table_name = collection_name
        self.dim = dim
        self._index_name = "tidb_vector_index"
        self._primary_field = "uuid"
        self._id_field = "id" 
        self._vector_field = "embedding"

        # Create connection
        self.conn, self.cursor = self._create_connection(**self.db_config)

        # Drop old table and index if required
        if drop_old:
            self._drop_table()

        self._create_table()

        # Close connection
        self.cursor.close()
        self.conn.close()
        self.cursor = None
        self.conn = None

    @staticmethod
    def _create_connection(autocommit: bool = False, **kwargs) -> tuple[MySQLConnection, MySQLCursor]:
        conn = mysql.connector.connect(autocommit=autocommit, **kwargs)
        cursor = conn.cursor(prepared=True)

        return conn, cursor

    def _drop_table(self):
        drop_table_sql = f"DROP TABLE IF EXISTS `{self.table_name}`"
        self.cursor.execute(drop_table_sql)
        self.conn.commit()
        log.info(f"Table `{self.table_name}` dropped.")

    def _create_table(self):
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.table_name}` (
            `{self._primary_field}` BINARY(16) PRIMARY KEY,
            `{self._id_field}` INT NOT NULL,
            `{self._vector_field}` VECTOR({self.dim}) NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        log.info(f"Table `{self.table_name}` created with dimension {self.dim}.")

    def _create_index(self):
        assert self.conn is not None, "Connection is not initialized"
        assert self.cursor is not None, "Cursor is not initialized"

        self.cursor.execute(f"ALTER TABLE `{self.table_name}` SET TIFLASH REPLICA 1;")
        self.cursor.execute(f"CREATE VECTOR INDEX `{self._index_name}` ON `{self.table_name}` ((VEC_COSINE_DISTANCE({self._vector_field}))) USING HNSW;")
        self.conn.commit()
        log.info(f"Index `{self._index_name}` created on `{self._vector_field}`.")

    def _drop_index(self):
        assert self.conn is not None, "Connection is not initialized"
        assert self.cursor is not None, "Cursor is not initialized"

        drop_index_sql = f"DROP INDEX IF EXISTS `{self._index_name}` ON `{self.table_name}`"
        self.cursor.execute(drop_index_sql)
        self.conn.commit()
        log.info(f"Index `{self._index_name}` dropped from table `{self.table_name}`.")

    @contextmanager
    def init(self) -> Generator[None, None, None]:
        """create and destory connections to database.
        Examples:
            >>> with self.init():
            >>>     self.insert_embeddings()
        """
        self.conn, self.cursor = self._create_connection(**self.db_config)
        try:
            yield
        finally:
            self.cursor.close()
            self.conn.close()
            self.cursor = None
            self.conn = None

        return

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        **kwargs,
    ) -> tuple[int, Exception | None]:
        assert self.conn is not None, "Connection is not initialized"
        assert self.cursor is not None, "Cursor is not initialized"

        values_placeholders = [] 
        data_tuple_list = []    

        for m, e in zip(metadata, embeddings, strict=False):
            values_placeholders.append("(%s, %s, %s)") 
            # Convert UUID to binary for performance reasons
            m_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, str(m))
            data_tuple_list.append(m_uuid.bytes)
            data_tuple_list.append(m)            
            data_tuple_list.append(json.dumps(e))   

        values_clause = ",\n    ".join(values_placeholders) 

        insert_sql = f"""
        INSERT INTO `{self.table_name}` (`{self._primary_field}`, `{self._id_field}`, `{self._vector_field}`)
        VALUES
            {values_clause}
        """

        try:
            # use cursor.execute() NOT cursor.executemany()
            self.cursor.execute(insert_sql, tuple(data_tuple_list))
            self.conn.commit()

            return len(metadata), None
        except Exception as e:
            log.exception(f"Failed to insert embeddings: {e}")
            self.conn.rollback()

            return 0, e

    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        filters: dict | None = None,
        timeout: int | None = None,
    ) -> list[int]:
        assert self.conn is not None, "Connection is not initialized"
        assert self.cursor is not None, "Cursor is not initialized"

        # row embedding can not use in SQL query
        query_vector = json.dumps(query)

        search_sql = f"""
        SELECT id, vec_cosine_distance({self._vector_field}, %s) AS distance
        FROM `{self.table_name}`
        ORDER BY distance
        LIMIT %s;
        """

        self.cursor.execute(search_sql, (query_vector, k))
        results = self.cursor.fetchall()

        return [int(r[0]) for r in results]

    #  TODO: Implement this method
    def optimize(self, data_size: int | None = None):
        log.info(f"{self.name} post insert before optimize")
        self._drop_index()
        self._create_index()

        # Warm up
        analyze_sql = f"ANALYZE TABLE `{self.table_name}`"
        self.cursor.execute(analyze_sql)
        self.conn.commit()

        log.info(f"{self.name} post insert after optimize")