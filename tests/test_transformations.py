import pytest

from transformations import DataTransformer as T
from transformations import clean_and_prepare


def rows(df):
    return [r.asDict() for r in df.collect()]


def test_clean_column_names(spark):
    df = spark.createDataFrame([(1, 2)], ["Full Name", "Age-(Y)"])
    assert T.clean_column_names(df).columns == ["full_name", "age_y"]


def test_trim_all_strings_only_touches_strings(spark):
    df = spark.createDataFrame([("  a  ", 1)], ["s", "n"])
    assert rows(T.trim_all_strings(df)) == [{"s": "a", "n": 1}]


def test_lowercase_all_strings(spark):
    df = spark.createDataFrame([("ABC", 1)], ["s", "n"])
    assert rows(T.lowercase_all_strings(df)) == [{"s": "abc", "n": 1}]


def test_remove_nulls_with_subset(spark):
    df = spark.createDataFrame([(1, "a"), (None, "b"), (3, None)], "id int, name string")
    assert [r["id"] for r in T.remove_nulls(df, subset=["id"]).collect()] == [1, 3]


def test_fill_nulls(spark):
    df = spark.createDataFrame([(None, "a")], "age int, name string")
    assert rows(T.fill_nulls(df, {"age": 0})) == [{"age": 0, "name": "a"}]


def test_remove_duplicates_subset(spark):
    df = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], ["id", "v"])
    assert T.remove_duplicates(df, subset=["id"]).count() == 2


def test_cast_columns(spark):
    df = spark.createDataFrame([("1", "2.5")], ["a", "b"])
    out = T.cast_columns(df, {"a": "integer", "b": "double"})
    assert dict(out.dtypes) == {"a": "int", "b": "double"}


def test_split_column(spark):
    df = spark.createDataFrame([("Alice Johnson",), ("Bob",)], ["full_name"])
    out = T.split_column(df, "full_name", " ", ["first", "last"])
    assert rows(out) == [
        {"full_name": "Alice Johnson", "first": "Alice", "last": "Johnson"},
        {"full_name": "Bob", "first": "Bob", "last": None},
    ]


def test_split_column_treats_delimiter_literally(spark):
    df = spark.createDataFrame([("a.b",)], ["x"])
    out = T.split_column(df, "x", ".", ["p", "q"])
    assert rows(out)[0]["p"] == "a" and rows(out)[0]["q"] == "b"


def test_concat_columns(spark):
    df = spark.createDataFrame([("a", "b")], ["x", "y"])
    assert rows(T.concat_columns(df, ["x", "y"], "xy", "-"))[0]["xy"] == "a-b"


def test_add_timestamp_column(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    assert "load_timestamp" in T.add_timestamp_column(df).columns


def test_extract_date_parts(spark):
    df = spark.createDataFrame([("2024-03-15",)], ["d"])
    r = rows(T.extract_date_parts(df, "d"))[0]
    assert (r["d_year"], r["d_month"], r["d_day"]) == (2024, 3, 15)


def test_add_conditional_column(spark):
    df = spark.createDataFrame([(10,), (30,), (70,)], ["age"])
    conds = {"age < 18": "Minor", "age >= 18 and age < 65": "Adult", "age >= 65": "Senior"}
    out = T.add_conditional_column(df, "grp", conds, "Unknown")
    assert [r["grp"] for r in out.orderBy("age").collect()] == ["Minor", "Adult", "Senior"]


def test_add_conditional_column_default(spark):
    df = spark.createDataFrame([(5,)], ["age"])
    out = T.add_conditional_column(df, "grp", {"age > 10": "big"}, "Unknown")
    assert rows(out)[0]["grp"] == "Unknown"


def test_add_conditional_column_rejects_empty(spark):
    df = spark.createDataFrame([(5,)], ["age"])
    with pytest.raises(ValueError):
        T.add_conditional_column(df, "grp", {})


def test_add_row_number_partitioned(spark):
    df = spark.createDataFrame([("a", 1), ("a", 2), ("b", 5)], ["dept", "sal"])
    out = T.add_row_number(df, partition_by=["dept"], order_by=["sal"])
    assert sorted((r["dept"], r["sal"], r["row_num"]) for r in out.collect()) == [
        ("a", 1, 1), ("a", 2, 2), ("b", 5, 1),
    ]


def test_add_row_number_without_partition(spark):
    df = spark.createDataFrame([(3,), (1,), (2,)], ["v"])
    out = T.add_row_number(df, order_by=["v"])
    assert [(r["v"], r["row_num"]) for r in out.orderBy("v").collect()] == [(1, 1), (2, 2), (3, 3)]


def test_add_row_number_requires_order_by(spark):
    df = spark.createDataFrame([(1,)], ["v"])
    with pytest.raises(ValueError):
        T.add_row_number(df, partition_by=["v"])


def test_filter_by_condition(spark):
    df = spark.createDataFrame([(1,), (30,)], ["age"])
    assert T.filter_by_condition(df, "age > 25").count() == 1


def test_apply_transformations(spark):
    df = spark.createDataFrame([(" A ", 1)], ["Name", "Id"])
    out = T.apply_transformations(
        df, [(T.clean_column_names, {}), (T.trim_all_strings, {})]
    )
    assert rows(out) == [{"name": "A", "id": 1}]


def test_clean_and_prepare_end_to_end(spark):
    df = spark.createDataFrame(
        [("  ALICE ", 1), ("alice", 1), (None, 2)], ["Full Name", "Id"]
    )
    out = clean_and_prepare(df)
    assert out.columns == ["full_name", "id"]
    assert rows(out) == [{"full_name": "alice", "id": 1}]
