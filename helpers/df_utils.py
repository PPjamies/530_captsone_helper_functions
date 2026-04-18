from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType

from .models import SourceType


def tag_source_type(df: DataFrame, source_type: SourceType, column_name: str = "source_type") -> DataFrame:
    """
    Tags a DataFrame with a constant value to indicate the data origin
    """
    res = df.withColumn(column_name, lit(source_type.value))
    return res


def cast_df_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Forces a DataFrame to match a specific schema by casting columns
    """
    res = df.select([
        col(field.name).cast(field.dataType) for field in schema
    ])
    return res
