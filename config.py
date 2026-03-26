#!/usr/bin/env python3
"""
Spark Configuration Settings
Define configuration for different environments (local, development, production)
"""

from typing import Dict, Any


class SparkConfigs:
    """Centralized Spark configuration management"""
    
    # ========================================================================
    # LOCAL DEVELOPMENT (single machine with all cores)
    # ========================================================================
    LOCAL = {
        'app_name': 'MySparkApp',
        'master': 'local[*]',  # Use all available cores
        'spark.sql.shuffle.partitions': 4,
        'spark.sql.adaptive.enabled': 'true',
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g',
        'spark.driver.cores': 2,
        'spark.default.parallelism': 4,
        'spark.sql.legacy.timeParserPolicy': 'LEGACY',
        'spark.sql.legacy.castComplexTypesToString.enabled': 'true',
    }
    
    # ========================================================================
    # DEVELOPMENT (small cluster with limited resources)
    # ========================================================================
    DEVELOPMENT = {
        'app_name': 'MySparkApp-Dev',
        'master': 'spark://localhost:7077',  # Standalone cluster
        'spark.executor.instances': 4,
        'spark.executor.cores': 2,
        'spark.executor.memory': '2g',
        'spark.driver.memory': '2g',
        'spark.driver.cores': 2,
        'spark.sql.shuffle.partitions': 8,
        'spark.sql.adaptive.enabled': 'true',
        'spark.default.parallelism': 8,
    }
    
    # ========================================================================
    # PRODUCTION (YARN cluster - large scale)
    # ========================================================================
    PRODUCTION_YARN = {
        'app_name': 'MySparkApp-Prod',
        'master': 'yarn',
        'spark.submit.deployMode': 'cluster',
        'spark.executor.instances': 20,
        'spark.executor.cores': 4,
        'spark.executor.memory': '4g',
        'spark.driver.memory': '4g',
        'spark.driver.cores': 4,
        'spark.sql.shuffle.partitions': 200,
        'spark.sql.adaptive.enabled': 'true',
        'spark.default.parallelism': 200,
        'spark.sql.autoBroadcastJoinThreshold': '100mb',
        'spark.dynamicAllocation.enabled': 'true',
        'spark.dynamicAllocation.minExecutors': 10,
        'spark.dynamicAllocation.maxExecutors': 50,
        'spark.dynamicAllocation.schedulerBacklogTimeout': '1m',
    }
    
    # ========================================================================
    # PRODUCTION (Standalone cluster)
    # ========================================================================
    PRODUCTION_STANDALONE = {
        'app_name': 'MySparkApp-Prod',
        'master': 'spark://spark-master:7077',
        'spark.executor.instances': 10,
        'spark.executor.cores': 8,
        'spark.executor.memory': '8g',
        'spark.driver.memory': '4g',
        'spark.driver.cores': 4,
        'spark.sql.shuffle.partitions': 200,
        'spark.sql.adaptive.enabled': 'true',
        'spark.default.parallelism': 200,
    }
    
    # ========================================================================
    # TESTING (minimal resources for unit tests)
    # ========================================================================
    TESTING = {
        'app_name': 'MySparkApp-Test',
        'master': 'local[1]',  # Single thread for deterministic results
        'spark.sql.shuffle.partitions': 1,
        'spark.driver.memory': '512m',
        'spark.executor.memory': '512m',
        'spark.sql.adaptive.enabled': 'false',
        'spark.default.parallelism': 1,
    }


class DatabaseConfigs:
    """Database connection configurations"""
    
    # PostgreSQL
    POSTGRESQL = {
        'host': 'localhost',
        'port': 5432,
        'database': 'mydb',
        'user': 'postgres',
        'password': 'password',  # Use environment variables in production!
        'driver': 'org.postgresql.Driver',
    }
    
    # MySQL
    MYSQL = {
        'host': 'localhost',
        'port': 3306,
        'database': 'mydb',
        'user': 'root',
        'password': 'password',  # Use environment variables in production!
        'driver': 'com.mysql.jdbc.Driver',
    }


class DataConfigs:
    """Data source configurations"""
    
    # Paths for different data types
    CSV_OPTIONS = {
        'header': 'true',
        'inferSchema': 'true',
        'sep': ',',
        'mode': 'PERMISSIVE',  # PERMISSIVE, DROPMALFORMED, FAILFAST
    }
    
    PARQUET_OPTIONS = {
        'compression': 'snappy',  # snappy, gzip, lz4, uncompressed
    }
    
    JSON_OPTIONS = {
        'mode': 'PERMISSIVE',
        'inferSchema': 'true',
    }


class LoggingConfigs:
    """Logging configurations"""
    
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    LOG_LEVEL = {
        'development': 'DEBUG',
        'production': 'INFO',
        'testing': 'WARNING',
    }


def get_spark_config(environment: str = 'local') -> Dict[str, Any]:
    """
    Get Spark configuration for the specified environment
    
    Args:
        environment (str): Environment name ('local', 'development', 'production_yarn', 'testing')
    
    Returns:
        dict: Spark configuration
    
    Examples:
        >>> config = get_spark_config('local')
        >>> config = get_spark_config('production_yarn')
    """
    configs = {
        'local': SparkConfigs.LOCAL,
        'development': SparkConfigs.DEVELOPMENT,
        'production_yarn': SparkConfigs.PRODUCTION_YARN,
        'production_standalone': SparkConfigs.PRODUCTION_STANDALONE,
        'testing': SparkConfigs.TESTING,
    }
    
    if environment not in configs:
        raise ValueError(
            f"Unknown environment: {environment}. "
            f"Valid options: {', '.join(configs.keys())}"
        )
    
    return configs[environment]


def get_database_url(db_type: str = 'postgresql') -> str:
    """
    Get database connection URL
    
    Args:
        db_type (str): Database type ('postgresql', 'mysql')
    
    Returns:
        str: JDBC connection URL for Spark
    """
    if db_type == 'postgresql':
        config = DatabaseConfigs.POSTGRESQL
        return (f"jdbc:postgresql://{config['host']}:{config['port']}"
                f"/{config['database']}")
    elif db_type == 'mysql':
        config = DatabaseConfigs.MYSQL
        return (f"jdbc:mysql://{config['host']}:{config['port']}"
                f"/{config['database']}")
    else:
        raise ValueError(f"Unknown database type: {db_type}")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def merge_configs(base_config: Dict[str, Any], 
                  overrides: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge base config with overrides
    
    Args:
        base_config (dict): Base configuration
        overrides (dict): Configuration overrides
    
    Returns:
        dict: Merged configuration
    
    Example:
        >>> base = get_spark_config('local')
        >>> custom = {'spark.driver.memory': '4g'}
        >>> merged = merge_configs(base, custom)
    """
    config = base_config.copy()
    config.update(overrides)
    return config


def print_config(config: Dict[str, Any], title: str = "Spark Configuration"):
    """
    Pretty print configuration
    
    Args:
        config (dict): Configuration dictionary
        title (str): Title for the output
    """
    print("\n" + "=" * 80)
    print(f"{title:^80}")
    print("=" * 80)
    
    for key, value in sorted(config.items()):
        print(f"{key:<50} = {value}")
    
    print("=" * 80 + "\n")


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Example 1: Get local config
    local_config = get_spark_config('local')
    print_config(local_config, "Local Configuration")
    
    # Example 2: Get production config
    prod_config = get_spark_config('production_yarn')
    print_config(prod_config, "Production YARN Configuration")
    
    # Example 3: Merge configs with overrides
    custom_config = merge_configs(
        get_spark_config('local'),
        {'spark.driver.memory': '4g', 'spark.executor.memory': '4g'}
    )
    print_config(custom_config, "Local Config with Overrides")
    
    # Example 4: Database URL
    print(f"PostgreSQL URL: {get_database_url('postgresql')}")
    print(f"MySQL URL: {get_database_url('mysql')}")
