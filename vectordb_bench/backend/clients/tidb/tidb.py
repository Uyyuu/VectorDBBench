import concurrent.futures
import json
import uuid
import io
import logging
import time
from contextlib import contextmanager
from typing import Any, Optional, Tuple

import pymysql

from ..api import VectorDB
from .config import TiDBIndexConfig

log = logging.getLogger(__name__)


class TiDB(VectorDB):
    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: TiDBIndexConfig,
        collection_name: str = "vector_bench_test",
        drop_old: bool = False,
        **kwargs,
    ):
        self.name = "TiDB"
        self.db_config = db_config
        self.case_config = db_case_config
        self.table_name = collection_name
        self.dim = dim
        self._vector_field = "embedding"
        self._index_name = "tidb_vector_index"
        self.conn = None  # To be inited by init()
        self.cursor = None  # To be inited by init()

        self.search_fn = db_case_config.search_param()["metric_fn"]

        if drop_old:
            self._drop_table()
            self._create_table()

    @contextmanager
    def init(self):
        with self._get_connection() as (conn, cursor):
            self.conn = conn
            self.cursor = cursor
            try:
                yield
            finally:
                self.conn = None
                self.cursor = None

    @contextmanager
    def _get_connection(self):
        with pymysql.connect(**self.db_config) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                yield conn, cursor

    def _drop_table(self):
        try:
            with self._get_connection() as (conn, cursor):
                cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                conn.commit()
        except Exception as e:
            log.warning("Failed to drop table: %s error: %s", self.table_name, e)
            raise e

    def _create_table(self):
        try:
            index_param = self.case_config.index_param()
            with self._get_connection() as (conn, cursor):
                cursor.execute(
                    f"""
                    CREATE TABLE {self.table_name} (
                        uuid BINARY(16) PRIMARY KEY,
                        id BIGINT NOT NULL,
                        embedding VECTOR({self.dim}) NOT NULL,
                        VECTOR INDEX {self._index_name} (({index_param["metric_fn"]}(embedding)))
                    );
                    """
                )
                conn.commit()
        except Exception as e:
            log.warning("Failed to create table: %s error: %s", self.table_name, e)
            raise e
    
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

    def ready_to_load(self) -> bool:
        pass

    def optimize(self, data_size: int | None = None) -> None:
        assert self.conn is not None, "Connection is not initialized"
        assert self.cursor is not None, "Cursor is not initialized"

        log.info("Starting TiFlash replica optimization...")
        log.info("Dropping existing index...")
        self._drop_index()
        self._create_index()
        
        log.info("Waiting TiFlash replica to catch up...")
        while True:
            progress = self._optimize_check_tiflash_replica_progress()
            if progress != 1:
                log.info("Data replication not ready, progress: %d", progress)
                time.sleep(2)
            else:
                break

        log.info("Waiting TiFlash to catch up...")
        self._optimize_wait_tiflash_catch_up()

        log.info("Start compacting TiFlash replica...")
        self._optimize_compact_tiflash()

        log.info("Waiting index build to finish...")
        log_reduce_seq = 0
        while True:
            pending_rows = self._optimize_get_tiflash_index_pending_rows()
            if pending_rows > 0:
                if log_reduce_seq % 15 == 0:
                    log.info("Index not fully built, pending rows: %d", pending_rows)
                log_reduce_seq += 1
                time.sleep(2)
            else:
                break

        log.info("Index build finished successfully.")

    def _optimize_check_tiflash_replica_progress(self):
        try:
            database = self.db_config["database"]
            with self._get_connection() as (_, cursor):
                cursor.execute(
                    f"""
                    SELECT PROGRESS FROM information_schema.tiflash_replica
                    WHERE TABLE_SCHEMA = "{database}" AND TABLE_NAME = "{self.table_name}"
                    """
                )
                result = cursor.fetchone()
                return result[0]
        except Exception as e:
            log.warning("Failed to check TiFlash replica progress: %s", e)
            raise e

    def _optimize_wait_tiflash_catch_up(self):
        try:
            with self._get_connection() as (conn, cursor):
                cursor.execute('SET @@TIDB_ISOLATION_READ_ENGINES="tidb,tiflash"')
                conn.commit()
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                result = cursor.fetchone()
                return result[0]
        except Exception as e:
            log.warning("Failed to wait TiFlash to catch up: %s", e)
            raise e

    def _optimize_compact_tiflash(self):
        try:
            with self._get_connection() as (conn, cursor):
                cursor.execute(f"ALTER TABLE {self.table_name} COMPACT")
                conn.commit()
        except Exception as e:
            log.warning("Failed to compact table: %s", e)
            raise e

    def _optimize_get_tiflash_index_pending_rows(self):
        try:
            database = self.db_config["database"]
            with self._get_connection() as (_, cursor):
                cursor.execute(
                    f"""
                    SELECT SUM(ROWS_STABLE_NOT_INDEXED)
                    FROM information_schema.tiflash_indexes
                    WHERE TIDB_DATABASE = "{database}" AND TIDB_TABLE = "{self.table_name}"
                    """
                )
                result = cursor.fetchone()
                return result[0]
        except Exception as e:
            log.warning("Failed to read TiFlash index pending rows: %s", e)
            raise e
        
    def bulk_insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        **kwargs,
    ) -> Exception:
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
        INSERT INTO `{self.table_name}` (uuid, id, embedding)
        VALUES
            {values_clause}
        """

        try:
            # use cursor.execute() NOT cursor.executemany()
            with self._get_connection() as (conn, cursor):
                cursor.execute(insert_sql, tuple(data_tuple_list))
                conn.commit()

        except Exception as e:
            log.warning(f"Failed to insert embeddings: {e}")
            self.conn.rollback()

            raise e

    def _insert_embeddings_serial(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
    ) -> Exception:
        try:
            self.bulk_insert_embeddings(embeddings, metadata)  
        except Exception as e:
            log.warning("Failed to insert data into table: %s", e)
            raise e

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        **kwargs: Any,
    ) -> Tuple[int, Optional[Exception]]:
        workers = 10
        # Avoid exceeding MAX_ALLOWED_PACKET (default=64MB)
        total_len = len(embeddings)
        if total_len == 0:
            return 0, None

        # バッチサイズの計算 (total_len < workers の場合や割り切れない場合を考慮)
        batch_size = max(1, (total_len + workers - 1) // workers) # 最低1は保証
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for i in range(0, total_len, batch_size):
                # ここでデータをスライスする
                batch_embeddings = embeddings[i:min(i + batch_size, total_len)]
                batch_metadata = metadata[i:min(i + batch_size, total_len)]

                # スライスしたバッチデータのみを _insert_embeddings_serial に渡す
                if batch_embeddings: # 空のバッチは送らない
                    future = executor.submit(self._insert_embeddings_serial, batch_embeddings, batch_metadata)
                    futures.append(future)

            done, pending = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_EXCEPTION)
            executor.shutdown(wait=False)
            for future in done:
                future.result()
            for future in pending:
                future.cancel()
        return len(metadata), None
    
    # Not implemented filter
    def search_embedding(
        self,
        query: list[float],
        k: int = 100,
        filters: dict | None = None,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> list[int]:
        self.cursor.execute(
            f"""
            SELECT id FROM {self.table_name}
            ORDER BY {self.search_fn}(embedding, "{str(query)}") LIMIT {k};
            """
        )
        result = self.cursor.fetchall()
        return [int(i[0]) for i in result]
