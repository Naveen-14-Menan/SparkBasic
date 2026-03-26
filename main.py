#!/usr/bin/env python3
"""
PySpark Application - Main Entry Point
A modern, production-ready Spark application with proper error handling and logging.
"""

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, year, count

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spark_app.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class SparkApp:
    """Base class for Spark applications with proper lifecycle management"""
    
    def __init__(self, app_name="MySparkApp", master="local[*]"):
        """
        Initialize Spark application
        
        Args:
            app_name (str): Name of the application
            master (str): Spark master URL
        """
        self.app_name = app_name
        self.master = master
        self.spark = None
        logger.info(f"Initializing {app_name}")
    
    def setup(self):
        """Initialize SparkSession with configuration"""
        try:
            logger.info(f"Setting up SparkSession for {self.app_name}")
            
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .master(self.master) \
                .config("spark.sql.shuffle.partitions", 4) \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
            
            # Set log level
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("SparkSession created successfully")
            logger.info(f"Spark Version: {self.spark.version}")
            logger.info(f"App Name: {self.spark.appName}")
            return self.spark
            
        except Exception as e:
            logger.error(f"Failed to setup SparkSession: {e}", exc_info=True)
            raise
    
    def run(self):
        """Override this method in subclasses"""
        raise NotImplementedError("Subclasses must implement run()")
    
    def cleanup(self):
        """Stop SparkSession"""
        try:
            if self.spark:
                logger.info("Stopping SparkSession...")
                self.spark.stop()
                logger.info("SparkSession stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping SparkSession: {e}", exc_info=True)
    
    def execute(self):
        """Main execution flow with error handling"""
        try:
            self.setup()
            self.run()
            return True
        except Exception as e:
            logger.error(f"Application failed: {e}", exc_info=True)
            return False
        finally:
            self.cleanup()


class MyFirstPySparkApp(SparkApp):
    """Example application: Working with DataFrames"""
    
    def __init__(self):
        super().__init__(
            app_name="MyFirstPySparkApp",
            master="local[*]"
        )
    
    def run(self):
        """Run the application logic"""
        logger.info("Starting application logic")
        
        try:
            # 1. Create sample data
            logger.info("Creating sample DataFrame")
            data = [
                ("Alice", 25, "Engineering"),
                ("Bob", 30, "Sales"),
                ("Charlie", 35, "Engineering"),
                ("Diana", 28, "HR"),
                ("Eve", 32, "Sales"),
            ]
            
            # 2. Create DataFrame
            df = self.spark.createDataFrame(
                data, 
                ["name", "age", "department"]
            )
            
            logger.info(f"DataFrame created with {df.count()} rows")
            
            # 3. Display the data
            logger.info("Full DataFrame:")
            df.show()
            
            # 4. Basic transformations
            logger.info("\nEmployees with age > 28:")
            df.filter(col("age") > 28).show()
            
            logger.info("\nNames in uppercase:")
            df.select(upper(col("name")).alias("name"), col("age")).show()
            
            # 5. Aggregations
            logger.info("\nAverage age by department:")
            df.groupBy("department") \
                .agg({"age": "avg", "name": "count"}) \
                .withColumnRenamed("avg(age)", "avg_age") \
                .withColumnRenamed("count(name)", "employee_count") \
                .show()
            
            # 6. Print schema
            logger.info("\nDataFrame schema:")
            df.printSchema()
            
            logger.info("Application logic completed successfully")
            
        except Exception as e:
            logger.error(f"Error in application logic: {e}", exc_info=True)
            raise


# Additional example applications

class DataProcessingApp(SparkApp):
    """Example: Processing and transforming data"""
    
    def __init__(self):
        super().__init__(
            app_name="DataProcessingApp",
            master="local[*]"
        )
    
    def run(self):
        """Process and transform sample data"""
        logger.info("Starting data processing")
        
        # Sample data - sales records
        sales_data = [
            (1, "Product A", 100, 2024),
            (2, "Product B", 150, 2024),
            (3, "Product A", 120, 2024),
            (4, "Product C", 200, 2024),
            (5, "Product B", 180, 2024),
        ]
        
        df = self.spark.createDataFrame(
            sales_data,
            ["transaction_id", "product", "amount", "year"]
        )
        
        logger.info("Sales data:")
        df.show()
        
        # Transformations
        logger.info("\nTotal sales by product:")
        df.groupBy("product") \
            .agg({"amount": "sum"}) \
            .withColumnRenamed("sum(amount)", "total_sales") \
            .orderBy(col("total_sales").desc()) \
            .show()
        
        logger.info("Data processing completed")


class RDDExampleApp(SparkApp):
    """Example: Using RDDs (legacy but still useful)"""
    
    def __init__(self):
        super().__init__(
            app_name="RDDExampleApp",
            master="local[*]"
        )
    
    def run(self):
        """Work with RDDs"""
        logger.info("Starting RDD example")
        
        # Get SparkContext from SparkSession
        sc = self.spark.sparkContext
        
        # Create RDD
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)
        
        # Transformations
        doubled = rdd.map(lambda x: x * 2)
        filtered = doubled.filter(lambda x: x > 5)
        
        # Action
        result = filtered.collect()
        logger.info(f"RDD transformation result: {result}")
        logger.info(f"RDD count: {filtered.count()}")
        
        # Word count example
        words = ["spark", "is", "awesome", "spark", "is", "fast"]
        word_rdd = sc.parallelize(words)
        
        word_counts = word_rdd \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortBy(lambda x: x[1], ascending=False)
        
        logger.info("Word counts:")
        for word, count in word_counts.collect():
            logger.info(f"  {word}: {count}")


def run_first_app():
    """Run the first example application"""
    logger.info("=" * 70)
    logger.info("Running MyFirstPySparkApp")
    logger.info("=" * 70)
    
    app = MyFirstPySparkApp()
    success = app.execute()
    
    return success


def run_data_processing_app():
    """Run the data processing application"""
    logger.info("\n" + "=" * 70)
    logger.info("Running DataProcessingApp")
    logger.info("=" * 70)
    
    app = DataProcessingApp()
    success = app.execute()
    
    return success


def run_rdd_example_app():
    """Run the RDD example application"""
    logger.info("\n" + "=" * 70)
    logger.info("Running RDDExampleApp")
    logger.info("=" * 70)
    
    app = RDDExampleApp()
    success = app.execute()
    
    return success


def main():
    """Main entry point"""
    logger.info(f"Application started at {datetime.now()}")
    
    # Run examples (comment out what you don't need)
    results = []
    
    # 1. Run first app (basic DataFrame operations)
    results.append(("MyFirstPySparkApp", run_first_app()))
    
    # 2. Uncomment to run data processing app
    # results.append(("DataProcessingApp", run_data_processing_app()))
    
    # 3. Uncomment to run RDD example
    # results.append(("RDDExampleApp", run_rdd_example_app()))
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)
    for app_name, success in results:
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{app_name}: {status}")
    
    logger.info(f"Application ended at {datetime.now()}")
    
    # Return exit code
    return 0 if all(result[1] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
