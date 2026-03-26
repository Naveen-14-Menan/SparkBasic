#!/usr/bin/env python3
"""
Data Loading Utilities
Load data from various sources (CSV, Parquet, JSON, SQL, etc.)
"""

import logging
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


class DataLoader:
    """Utility class for loading data from various sources"""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize DataLoader
        
        Args:
            spark (SparkSession): Active SparkSession
        """
        self.spark = spark
    
    # ========================================================================
    # CSV Files
    # ========================================================================
    
    def load_csv(self, 
                 path: str,
                 header: bool = True,
                 infer_schema: bool = True,
                 **options) -> DataFrame:
        """
        Load CSV file into DataFrame
        
        Args:
            path (str): Path to CSV file
            header (bool): Whether file has header
            infer_schema (bool): Whether to infer schema from data
            **options: Additional options for spark.read.csv()
        
        Returns:
            DataFrame: Loaded data
        
        Example:
            >>> loader = DataLoader(spark)
            >>> df = loader.load_csv('data/employees.csv')
        """
        try:
            logger.info(f"Loading CSV from {path}")
            df = self.spark.read.csv(
                path,
                header=header,
                inferSchema=infer_schema,
                **options
            )
            logger.info(f"Loaded {df.count()} rows from {path}")
            return df
        except Exception as e:
            logger.error(f"Failed to load CSV from {path}: {e}")
            raise
    
    # ========================================================================
    # Parquet Files
    # ========================================================================
    
    def load_parquet(self, path: str) -> DataFrame:
        """
        Load Parquet file into DataFrame
        
        Args:
            path (str): Path to Parquet file/directory
        
        Returns:
            DataFrame: Loaded data
        
        Example:
            >>> df = loader.load_parquet('data/output/processed.parquet')
        """
        try:
            logger.info(f"Loading Parquet from {path}")
            df = self.spark.read.parquet(path)
            logger.info(f"Loaded {df.count()} rows from {path}")
            return df
        except Exception as e:
            logger.error(f"Failed to load Parquet from {path}: {e}")
            raise
    
    # ========================================================================
    # JSON Files
    # ========================================================================
    
    def load_json(self, path: str, **options) -> DataFrame:
        """
        Load JSON file into DataFrame
        
        Args:
            path (str): Path to JSON file
            **options: Additional options for spark.read.json()
        
        Returns:
            DataFrame: Loaded data
        
        Example:
            >>> df = loader.load_json('data/events.json')
        """
        try:
            logger.info(f"Loading JSON from {path}")
            df = self.spark.read.json(path, **options)
            logger.info(f"Loaded {df.count()} rows from {path}")
            return df
        except Exception as e:
            logger.error(f"Failed to load JSON from {path}: {e}")
            raise
    
    # ========================================================================
    # SQL Database
    # ========================================================================
    
    def load_sql(self,
                 url: str,
                 table: str,
                 driver: str,
                 user: str,
                 password: str,
                 **options) -> DataFrame:
        """
        Load data from SQL database
        
        Args:
            url (str): JDBC connection URL
            table (str): Table name
            driver (str): JDBC driver class
            user (str): Database user
            password (str): Database password
            **options: Additional options
        
        Returns:
            DataFrame: Loaded data
        
        Example:
            >>> df = loader.load_sql(
            ...     url='jdbc:postgresql://localhost:5432/mydb',
            ...     table='employees',
            ...     driver='org.postgresql.Driver',
            ...     user='postgres',
            ...     password='password'
            ... )
        """
        try:
            logger.info(f"Loading data from {table}")
            df = self.spark.read.format("jdbc") \
                .option("url", url) \
                .option("dbtable", table) \
                .option("driver", driver) \
                .option("user", user) \
                .option("password", password) \
                .options(**options) \
                .load()
            logger.info(f"Loaded {df.count()} rows from {table}")
            return df
        except Exception as e:
            logger.error(f"Failed to load data from {table}: {e}")
            raise
    
    # ========================================================================
    # Text Files
    # ========================================================================
    
    def load_text(self, path: str) -> DataFrame:
        """
        Load text file into DataFrame
        
        Args:
            path (str): Path to text file
        
        Returns:
            DataFrame: Loaded data with 'value' column
        
        Example:
            >>> df = loader.load_text('data/logs.txt')
        """
        try:
            logger.info(f"Loading text file from {path}")
            df = self.spark.read.text(path)
            logger.info(f"Loaded {df.count()} lines from {path}")
            return df
        except Exception as e:
            logger.error(f"Failed to load text file from {path}: {e}")
            raise
    
    # ========================================================================
    # Sample Data (for testing)
    # ========================================================================
    
    def create_sample_employees(self) -> DataFrame:
        """
        Create sample employee data for testing
        
        Returns:
            DataFrame: Sample employee data
        """
        logger.info("Creating sample employee data")
        data = [
            (1, "Alice", 25, "Engineering", 80000.0),
            (2, "Bob", 30, "Sales", 75000.0),
            (3, "Charlie", 35, "Engineering", 95000.0),
            (4, "Diana", 28, "HR", 70000.0),
            (5, "Eve", 32, "Sales", 80000.0),
            (6, "Frank", 45, "Management", 120000.0),
        ]
        
        df = self.spark.createDataFrame(
            data,
            ["id", "name", "age", "department", "salary"]
        )
        return df
    
    def create_sample_sales(self) -> DataFrame:
        """
        Create sample sales data for testing
        
        Returns:
            DataFrame: Sample sales data
        """
        logger.info("Creating sample sales data")
        data = [
            (1, "2024-01-15", "Product A", 100, 10),
            (2, "2024-01-15", "Product B", 150, 5),
            (3, "2024-01-16", "Product A", 120, 8),
            (4, "2024-01-16", "Product C", 200, 3),
            (5, "2024-01-17", "Product B", 180, 6),
            (6, "2024-01-17", "Product A", 100, 12),
        ]
        
        df = self.spark.createDataFrame(
            data,
            ["transaction_id", "date", "product", "price", "quantity"]
        )
        return df
    
    def create_sample_customers(self) -> DataFrame:
        """
        Create sample customer data for testing
        
        Returns:
            DataFrame: Sample customer data
        """
        logger.info("Creating sample customer data")
        data = [
            (1, "Alice Johnson", "alice@email.com", "New York"),
            (2, "Bob Smith", "bob@email.com", "Los Angeles"),
            (3, "Charlie Brown", "charlie@email.com", "Chicago"),
            (4, "Diana Prince", "diana@email.com", "Houston"),
            (5, "Eve Wilson", "eve@email.com", "Phoenix"),
        ]
        
        df = self.spark.createDataFrame(
            data,
            ["customer_id", "name", "email", "city"]
        )
        return df


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def load_csv(spark: SparkSession, path: str, **options) -> DataFrame:
    """Convenience function for loading CSV"""
    loader = DataLoader(spark)
    return loader.load_csv(path, **options)


def load_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Convenience function for loading Parquet"""
    loader = DataLoader(spark)
    return loader.load_parquet(path)


def load_json(spark: SparkSession, path: str, **options) -> DataFrame:
    """Convenience function for loading JSON"""
    loader = DataLoader(spark)
    return loader.load_json(path, **options)


def create_sample_data(spark: SparkSession, dataset: str = 'employees') -> DataFrame:
    """
    Create sample data
    
    Args:
        spark (SparkSession): Active SparkSession
        dataset (str): Dataset type ('employees', 'sales', 'customers')
    
    Returns:
        DataFrame: Sample data
    """
    loader = DataLoader(spark)
    
    datasets = {
        'employees': loader.create_sample_employees,
        'sales': loader.create_sample_sales,
        'customers': loader.create_sample_customers,
    }
    
    if dataset not in datasets:
        raise ValueError(f"Unknown dataset: {dataset}. Valid options: {', '.join(datasets.keys())}")
    
    return datasets[dataset]()


if __name__ == "__main__":
    # Example usage
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("DataLoaderExample") \
        .master("local[*]") \
        .getOrCreate()
    
    loader = DataLoader(spark)
    
    # Load sample data
    print("\n=== Sample Employee Data ===")
    employees = loader.create_sample_employees()
    employees.show()
    
    print("\n=== Sample Sales Data ===")
    sales = loader.create_sample_sales()
    sales.show()
    
    spark.stop()
