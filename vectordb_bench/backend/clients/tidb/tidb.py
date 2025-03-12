import json
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
        self._primary_field = "id"
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
    def _create_connection(**kwargs) -> tuple[MySQLConnection, MySQLCursor]:
        conn = mysql.connector.connect(**kwargs)
        cursor = conn.cursor()

        return conn, cursor

    def _drop_table(self):
        drop_table_sql = f"DROP TABLE IF EXISTS `{self.table_name}`"
        self.cursor.execute(drop_table_sql)
        self.conn.commit()
        log.info(f"Table `{self.table_name}` dropped.")

    def _create_table(self):
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.table_name}` (
            `{self._primary_field}` INT PRIMARY KEY,
            `{self._vector_field}` VECTOR({self.dim})
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        log.info(f"Table `{self.table_name}` created with dimension {self.dim}.")

    def _create_index(self):
        create_index_sql = f"""
        CREATE INDEX `{self._index_name}` ON `{self.table_name}` USING HNSW (`{self._vector_field}`) ALGORITHM HNSW;
        """
        self.cursor.execute(create_index_sql)
        self.conn.commit()
        log.info(f"Index `{self._index_name}` created on `{self._vector_field}`.")

    def _drop_index(self):
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

        insert_sql = f"""
        INSERT INTO `{self.table_name}` (`{self._primary_field}`, `{self._vector_field}`)
        VALUES (%s, %s)
        """

        insert_datas: list[tuple] = []
        for m, e in zip(metadata, embeddings, strict=False):
            insert_datas.append((m, json.dumps(e)))
        try:
            # self.cursor.executemany(insert_sql, [(m, e) for m, e in zip(metadata, embeddings, strict=False)])
            self.cursor.executemany(insert_sql, insert_datas)
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

        # エンベディングベクトルをJSON形式の文字列に変換
        query_vector = json.dumps(query)

        # パラメータ化クエリを使用
        search_sql = f"""
        SELECT id, vec_cosine_distance({self._vector_field}, %s) AS distance
        FROM `{self.table_name}`
        ORDER BY distance
        LIMIT %s;
        """
        # パラメータをクエリに渡す
        self.cursor.execute(search_sql, (query_vector, k))
        results = self.cursor.fetchall()

        return [int(r[0]) for r in results]

    #  TODO: Implement this method
    def optimize(self, data_size: int | None = None):
        pass
