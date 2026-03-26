#!/usr/bin/env python3
"""
Data Transformation Utilities
Common transformation and cleaning functions for PySpark DataFrames
"""

import logging
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, lower, upper, concat_ws, 
    when, coalesce, current_timestamp,
    row_number, datediff, to_date, year, month, day
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class DataTransformer:
    """Utility class for common DataFrame transformations"""
    
    # ========================================================================
    # Column Cleaning
    # ========================================================================
    
    @staticmethod
    def clean_column_names(df: DataFrame) -> DataFrame:
        """
        Clean column names: lowercase, remove spaces, replace special chars
        
        Args:
            df (DataFrame): Input DataFrame
        
        Returns:
            DataFrame: DataFrame with cleaned column names
        
        Example:
            >>> df = transformer.clean_column_names(df)
        """
        logger.info("Cleaning column names")
        
        for column in df.columns:
            new_column = (
                column
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
                .replace(".", "_")
                .replace("(", "")
                .replace(")", "")
            )
            df = df.withColumnRenamed(column, new_column)
        
        return df
    
    @staticmethod
    def trim_all_strings(df: DataFrame) -> DataFrame:
        """
        Trim leading/trailing whitespace from all string columns
        
        Args:
            df (DataFrame): Input DataFrame
        
        Returns:
            DataFrame: DataFrame with trimmed strings
        
        Example:
            >>> df = transformer.trim_all_strings(df)
        """
        logger.info("Trimming whitespace from string columns")
        
        for col_name in df.columns:
            if col_name in df.dtypes and df.schema[col_name].dataType.typeName() == 'string':
                df = df.withColumn(col_name, trim(col(col_name)))
        
        return df
    
    @staticmethod
    def lowercase_all_strings(df: DataFrame) -> DataFrame:
        """
        Convert all string columns to lowercase
        
        Args:
            df (DataFrame): Input DataFrame
        
        Returns:
            DataFrame: DataFrame with lowercase strings
        """
        logger.info("Converting string columns to lowercase")
        
        for col_name in df.columns:
            if col_name in df.dtypes:
                field = next((f for f in df.schema.fields if f.name == col_name), None)
                if field and "string" in str(field.dataType):
                    df = df.withColumn(col_name, lower(col(col_name)))
        
        return df
    
    # ========================================================================
    # Null/Missing Data Handling
    # ========================================================================
    
    @staticmethod
    def remove_nulls(df: DataFrame, subset: List[str] = None) -> DataFrame:
        """
        Remove rows with null values
        
        Args:
            df (DataFrame): Input DataFrame
            subset (list): Specific columns to check (default: all columns)
        
        Returns:
            DataFrame: DataFrame without null rows
        
        Example:
            >>> df = transformer.remove_nulls(df, subset=['id', 'name'])
        """
        logger.info(f"Removing null rows. Subset: {subset}")
        
        before_count = df.count()
        df = df.dropna(subset=subset)
        after_count = df.count()
        
        logger.info(f"Removed {before_count - after_count} rows with nulls")
        return df
    
    @staticmethod
    def fill_nulls(df: DataFrame, fill_values: Dict[str, Any]) -> DataFrame:
        """
        Fill null values with specified values
        
        Args:
            df (DataFrame): Input DataFrame
            fill_values (dict): Dictionary of column names and fill values
        
        Returns:
            DataFrame: DataFrame with filled nulls
        
        Example:
            >>> df = transformer.fill_nulls(df, {'age': 0, 'name': 'Unknown'})
        """
        logger.info(f"Filling null values: {fill_values}")
        df = df.fillna(fill_values)
        return df
    
    # ========================================================================
    # Deduplication
    # ========================================================================
    
    @staticmethod
    def remove_duplicates(df: DataFrame, subset: List[str] = None) -> DataFrame:
        """
        Remove duplicate rows
        
        Args:
            df (DataFrame): Input DataFrame
            subset (list): Specific columns to consider (default: all columns)
        
        Returns:
            DataFrame: DataFrame without duplicates
        
        Example:
            >>> df = transformer.remove_duplicates(df, subset=['id'])
        """
        logger.info(f"Removing duplicates. Subset: {subset}")
        
        before_count = df.count()
        df = df.dropDuplicates(subset=subset)
        after_count = df.count()
        
        logger.info(f"Removed {before_count - after_count} duplicate rows")
        return df
    
    # ========================================================================
    # Type Conversion
    # ========================================================================
    
    @staticmethod
    def cast_column(df: DataFrame, column: str, target_type: str) -> DataFrame:
        """
        Cast column to different data type
        
        Args:
            df (DataFrame): Input DataFrame
            column (str): Column name
            target_type (str): Target data type (e.g., 'integer', 'double', 'string')
        
        Returns:
            DataFrame: DataFrame with casted column
        
        Example:
            >>> df = transformer.cast_column(df, 'age', 'integer')
        """
        logger.info(f"Casting {column} to {target_type}")
        df = df.withColumn(column, col(column).cast(target_type))
        return df
    
    @staticmethod
    def cast_columns(df: DataFrame, cast_map: Dict[str, str]) -> DataFrame:
        """
        Cast multiple columns to different data types
        
        Args:
            df (DataFrame): Input DataFrame
            cast_map (dict): Dictionary of column names and target types
        
        Returns:
            DataFrame: DataFrame with casted columns
        
        Example:
            >>> df = transformer.cast_columns(df, {'age': 'integer', 'salary': 'double'})
        """
        logger.info(f"Casting columns: {cast_map}")
        
        for column, target_type in cast_map.items():
            df = df.withColumn(column, col(column).cast(target_type))
        
        return df
    
    # ========================================================================
    # Text Processing
    # ========================================================================
    
    @staticmethod
    def split_column(df: DataFrame, 
                     column: str, 
                     delimiter: str,
                     new_columns: List[str]) -> DataFrame:
        """
        Split column by delimiter into multiple columns
        
        Args:
            df (DataFrame): Input DataFrame
            column (str): Column to split
            delimiter (str): Delimiter string
            new_columns (list): Names for new columns
        
        Returns:
            DataFrame: DataFrame with split columns
        
        Example:
            >>> df = transformer.split_column(df, 'full_name', ' ', ['first_name', 'last_name'])
        """
        logger.info(f"Splitting {column} by '{delimiter}'")
        
        split_cols = df[column].str.split(delimiter, expand=True)
        for i, new_col in enumerate(new_columns):
            if i < len(split_cols.columns):
                df = df.withColumn(new_col, split_cols[i])
        
        return df
    
    @staticmethod
    def concat_columns(df: DataFrame,
                       columns: List[str],
                       new_column: str,
                       delimiter: str = " ") -> DataFrame:
        """
        Concatenate multiple columns into one
        
        Args:
            df (DataFrame): Input DataFrame
            columns (list): Columns to concatenate
            new_column (str): Name of new column
            delimiter (str): Delimiter to use (default: space)
        
        Returns:
            DataFrame: DataFrame with concatenated column
        
        Example:
            >>> df = transformer.concat_columns(df, ['first_name', 'last_name'], 'full_name')
        """
        logger.info(f"Concatenating {columns} into {new_column}")
        df = df.withColumn(new_column, concat_ws(delimiter, *[col(c) for c in columns]))
        return df
    
    # ========================================================================
    # Date/Time Operations
    # ========================================================================
    
    @staticmethod
    def add_timestamp_column(df: DataFrame, column_name: str = "load_timestamp") -> DataFrame:
        """
        Add current timestamp column
        
        Args:
            df (DataFrame): Input DataFrame
            column_name (str): Name of timestamp column
        
        Returns:
            DataFrame: DataFrame with timestamp column
        
        Example:
            >>> df = transformer.add_timestamp_column(df)
        """
        logger.info(f"Adding timestamp column '{column_name}'")
        df = df.withColumn(column_name, current_timestamp())
        return df
    
    @staticmethod
    def extract_date_parts(df: DataFrame, 
                          date_column: str,
                          date_format: str = "yyyy-MM-dd") -> DataFrame:
        """
        Extract year, month, day from date column
        
        Args:
            df (DataFrame): Input DataFrame
            date_column (str): Name of date column
            date_format (str): Date format
        
        Returns:
            DataFrame: DataFrame with extracted date parts
        
        Example:
            >>> df = transformer.extract_date_parts(df, 'order_date')
        """
        logger.info(f"Extracting date parts from {date_column}")
        
        date_col = to_date(col(date_column), date_format)
        df = df.withColumn(f"{date_column}_year", year(date_col))
        df = df.withColumn(f"{date_column}_month", month(date_col))
        df = df.withColumn(f"{date_column}_day", day(date_col))
        
        return df
    
    # ========================================================================
    # Conditional Operations
    # ========================================================================
    
    @staticmethod
    def add_conditional_column(df: DataFrame,
                              new_column: str,
                              conditions: Dict[str, str],
                              default: Any = None) -> DataFrame:
        """
        Add column based on conditions
        
        Args:
            df (DataFrame): Input DataFrame
            new_column (str): Name of new column
            conditions (dict): Dictionary of {condition: value}
            default: Default value if no conditions match
        
        Returns:
            DataFrame: DataFrame with conditional column
        
        Example:
            >>> conditions = {
            ...     "age < 18": "Minor",
            ...     "age >= 18 and age < 65": "Adult",
            ...     "age >= 65": "Senior"
            ... }
            >>> df = transformer.add_conditional_column(df, 'age_group', conditions, 'Unknown')
        """
        logger.info(f"Adding conditional column '{new_column}'")
        
        condition_col = when(conditions[list(conditions.keys())[0]], list(conditions.values())[0])
        
        for cond, val in list(conditions.items())[1:]:
            condition_col = condition_col.when(col(cond), val)
        
        df = df.withColumn(new_column, condition_col.otherwise(default))
        return df
    
    # ========================================================================
    # Ranking/Windowing
    # ========================================================================
    
    @staticmethod
    def add_row_number(df: DataFrame,
                      partition_by: List[str] = None,
                      order_by: List[str] = None) -> DataFrame:
        """
        Add row number using window function
        
        Args:
            df (DataFrame): Input DataFrame
            partition_by (list): Columns to partition by
            order_by (list): Columns to order by
        
        Returns:
            DataFrame: DataFrame with row_number column
        
        Example:
            >>> df = transformer.add_row_number(df, partition_by=['department'], order_by=['salary'])
        """
        logger.info(f"Adding row number. Partition: {partition_by}, Order: {order_by}")
        
        window_spec = Window \
            .partitionBy(*partition_by) if partition_by else Window.partitionBy() \
            .orderBy(*order_by) if order_by else Window.orderBy()
        
        df = df.withColumn("row_num", row_number().over(window_spec))
        return df
    
    # ========================================================================
    # Data Validation
    # ========================================================================
    
    @staticmethod
    def filter_by_condition(df: DataFrame, condition: str) -> DataFrame:
        """
        Filter DataFrame by condition
        
        Args:
            df (DataFrame): Input DataFrame
            condition (str): Filter condition as SQL expression
        
        Returns:
            DataFrame: Filtered DataFrame
        
        Example:
            >>> df = transformer.filter_by_condition(df, "age > 25 and department = 'Engineering'")
        """
        logger.info(f"Filtering by condition: {condition}")
        df = df.filter(condition)
        return df
    
    # ========================================================================
    # Pipeline
    # ========================================================================
    
    @staticmethod
    def apply_transformations(df: DataFrame, transformations: List[tuple]) -> DataFrame:
        """
        Apply multiple transformations in sequence
        
        Args:
            df (DataFrame): Input DataFrame
            transformations (list): List of (function, kwargs) tuples
        
        Returns:
            DataFrame: Transformed DataFrame
        
        Example:
            >>> transforms = [
            ...     (DataTransformer.clean_column_names, {}),
            ...     (DataTransformer.trim_all_strings, {}),
            ...     (DataTransformer.remove_nulls, {'subset': ['id']}),
            ... ]
            >>> df = DataTransformer.apply_transformations(df, transforms)
        """
        logger.info(f"Applying {len(transformations)} transformations")
        
        for func, kwargs in transformations:
            logger.info(f"Applying {func.__name__}")
            df = func(df, **kwargs)
        
        return df


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def clean_and_prepare(df: DataFrame) -> DataFrame:
    """
    Apply standard cleaning and preparation transformations
    
    Args:
        df (DataFrame): Input DataFrame
    
    Returns:
        DataFrame: Cleaned and prepared DataFrame
    """
    logger.info("Applying standard clean and prepare pipeline")
    
    df = DataTransformer.clean_column_names(df)
    df = DataTransformer.trim_all_strings(df)
    df = DataTransformer.lowercase_all_strings(df)
    df = DataTransformer.remove_duplicates(df)
    df = DataTransformer.remove_nulls(df)
    
    return df


if __name__ == "__main__":
    # Example usage
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("TransformationExample") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create sample data
    data = [
        (1, "  Alice Johnson  ", 25, "Engineering", 80000.0),
        (2, "  Bob Smith  ", 30, "Sales", 75000.0),
        (3, "  Charlie Brown  ", 35, "Engineering", 95000.0),
    ]
    
    df = spark.createDataFrame(
        data,
        ["ID", "Full Name", "Age", "Department", "Salary"]
    )
    
    print("\n=== Original Data ===")
    df.show()
    
    print("\n=== After Clean Column Names ===")
    df = DataTransformer.clean_column_names(df)
    df.show()
    
    print("\n=== After Trim Strings ===")
    df = DataTransformer.trim_all_strings(df)
    df.show()
    
    spark.stop()
