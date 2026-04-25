from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit, trim, lower, when, coalesce, try_to_date, row_number
from pyspark.sql.types import StructType

from .models import SourceType


def tag_source_type(df: DataFrame, source_type: SourceType, column_name: str = "source_type") -> DataFrame:
    """
    Tags a DataFrame with a constant value to indicate the data origin
    """
    return df.withColumn(column_name, lit(source_type.value))


def cast_df_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Forces a DataFrame to match a specific schema by casting columns
    """

    existing_columns = set(df.columns)

    return df.select([
        col(field.name).cast(field.dataType)
        if field.name in existing_columns
        else lit(None).cast(field.dataType).alias(field.name)
        for field in schema
    ])


def check_no_null_sk(df: DataFrame, sk_col: str):
    """
    Checks a suspected surrogate key in a dimension table is not null
    """
    nulls = df.filter(col(sk_col).isNull())

    if nulls.count() > 0:
        nulls.show()
        raise AssertionError(f"{nulls.count()} Nulls found in {sk_col} column")

    print(f"No nulls found in {sk_col} column")


def check_unique_sk(df: DataFrame, sk_col: str):
    """
    Checks a suspected surrogate key in a dimension table is unique
    """
    duplicates = df.groupBy(sk_col).count().filter(col("count") > 1)

    if duplicates.count() > 0:
        duplicates.show()
        raise AssertionError(f"{duplicates.count()} duplicate keys found in {sk_col} column")

    print(f"Unique keys found in {sk_col} column")


def clean_strings(df: DataFrame, col_names: list[str]) -> DataFrame:
    """
    Cleans string columns
    """
    for col_name in col_names:
        # remove whitespaces
        df = df.withColumn(col_name, trim(col(col_name)))

        # turn all strings to lowercase
        df = df.withColumn(col_name, lower(col(col_name)))

        # standardize null values
        df = df.withColumn(
            col_name,
            when(col(col_name).isin("", "null", "none", "na", "n/a", "nan", "unknown", "undefined"), None)
            .otherwise(col(col_name))
        )

    return df


def clean_booleans(df: DataFrame, col_names: list[str]) -> DataFrame:
    """
    Cleans string boolean columns
    """
    for col_name in col_names:
        # clean strings
        df = clean_strings(df, [col_name])

        # standardize boolean values
        df = df.withColumn(
            col_name,
            when(col(col_name).isin("y", "yes", "true", "1"), True)
            .when(col(col_name).isin("n", "no", "false", "0"), False)
            .otherwise(None)
        )

    return df


def clean_dates(df: DataFrame, col_names: list[str]) -> DataFrame:
    """
    Cleans string date columns
    """
    for col_name in col_names:
        # clean strings
        df = clean_strings(df, [col_name])

        # standardize date values
        df = df.withColumn(
            col_name,
            coalesce(
                try_to_date(col(col_name), "yyyy-MM-dd"),
                try_to_date(col(col_name), "dd/MM/yyyy"),
                try_to_date(col(col_name), "MM/dd/yyyy")
            )
        )

    return df


def check_duplicates(df: DataFrame):
    """
    Checks the data frame has no duplicates (excludes the source_type column)
    """
    # create a list of columns to compare on (exclude source_type)
    cmpr_cols = [c for c in df.columns if c != "source_type"]

    duplicates = df.groupBy(cmpr_cols).count().filter(col("count") > 1)
    print(f"Found {duplicates.count()} duplicate records")


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Remove duplicates from a dataframe based on source_type ranking
    """
    # create a list of columns to compare on (exclude source_type)
    cmpr_cols = [c for c in df.columns if c != "source_type"]

    # rank each row
    window = Window.partitionBy(cmpr_cols).orderBy(
        when(col("source_type") == "parquet", 1)
        .when(col("source_type") == "json", 2)
        .when(col("source_type") == "csv", 3)
        .when(col("source_type") == "sql", 4)  # stalest data
    )

    df = df.withColumn("rank", row_number().over(window))

    # keep the duplicate row with highest priority
    df = df.filter(col("rank") == 1).drop("rank")

    return df
