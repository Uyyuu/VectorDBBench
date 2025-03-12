from typing import TypedDict

from pydantic import BaseModel

from ..api import DBCaseConfig, DBConfig, MetricType


class TiDBConfigDict(TypedDict):
    host: str
    port: int
    user: str
    password: str
    database: str


class TiDBConfig(DBConfig):
    host: str = "localhost"
    port: int = 4000
    user: str = "root"
    password: str = ""
    database: str = "test"

    def to_dict(self) -> TiDBConfigDict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }


class TiDBIndexConfig(BaseModel, DBCaseConfig):
    metric_type: MetricType | None = None

    def parse_metric(self) -> str:
        if self.metric_type == MetricType.L2:
            return "Euclid"

        if self.metric_type == MetricType.IP:
            return "Dot"

        return "Cosine"

    def index_param(self) -> dict:
        return {"distance": self.parse_metric()}

    def search_param(self) -> dict:
        return {}
