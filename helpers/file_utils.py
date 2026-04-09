from collections import Counter, defaultdict

SPARK_TYPE_ALIASES = {
    "int": "integer",
    "bigint": "long"
}


def test(message: str):
    print("hello from python package!!!")
    print(message)


def report_column_counts(dfs: list):
    all_cols = []

    # flatten cols
    for df in dfs:
        all_cols.extend(df.columns)

    # map = {col_name: count}
    col_count = Counter(all_cols)

    print(f"Expected count for each column is {len(dfs)}")
    for col_name, count in sorted(col_count.items()):
        print(f"{col_name}: {count}")


def report_column_type_counts(dfs: list):
    # map = {col_name: {type1: count, type2: count, etc...}}
    col_type_count = defaultdict(Counter)

    # store col type frequency of in map
    for df in dfs:
        for field in df.schema.fields:
            col_type_count[field.name][field.dataType.simpleString()] += 1

    for col_name, type_counts in sorted(col_type_count.items()):
        print(col_name)
        for dtype, count in type_counts.items():
            print(f"   |--{SPARK_TYPE_ALIASES.get(dtype, dtype)}: {count}")