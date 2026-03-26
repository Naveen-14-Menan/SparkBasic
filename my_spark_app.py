#!/usr/bin/env python3
from pyspark.sql import SparkSession


def main():
    from pyspark.sql import SparkSession

    # Create session
    spark = SparkSession.builder \
        .appName("MyFirstPySparkApp") \
        .master("local[*]") \
        .getOrCreate()

    # Test it works
    print(f"Spark version: {spark.version}")
    print(f"App name: {spark.appName}")

    # Create a simple DataFrame
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])
    df.show()

    # Cleanup
    spark.stop()


if __name__ == "__main__":
    main()