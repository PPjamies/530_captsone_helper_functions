from enum import Enum


class SourceType(Enum):
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    SQL = "sql"
