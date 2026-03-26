# PySpark Project Structure

## Recommended Directory Layout

```
my_spark_project/
│
├── main.py                          # Main entry point
├── requirements.txt                 # Python dependencies
├── setup.py                         # Package setup (optional)
├── README.md                        # Project documentation
├── spark_app.log                    # Log file (created at runtime)
│
├── config/
│   └── spark_config.py              # Spark configuration settings
│
├── src/
│   ├── __init__.py
│   ├── app.py                       # Main application class
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── data_processing.py       # Data processing job
│   │   ├── aggregation.py           # Aggregation job
│   │   └── reporting.py             # Reporting job
│   │
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── data_loader.py           # Load data from various sources
│   │   ├── data_saver.py            # Save data to various destinations
│   │   ├── transformations.py       # Common transformation functions
│   │   └── validation.py            # Data validation functions
│   │
│   └── models/
│       ├── __init__.py
│       └── data_models.py           # Data structures/schemas
│
├── tests/
│   ├── __init__.py
│   ├── test_transformations.py      # Unit tests
│   ├── test_jobs.py                 # Job tests
│   └── conftest.py                  # Pytest configuration
│
├── data/
│   ├── input/                       # Input data files
│   ├── output/                      # Output data files
│   └── sample/                      # Sample data for testing
│
└── scripts/
    ├── run_local.sh                 # Run locally
    ├── run_cluster.sh               # Run on cluster
    └── install_dependencies.sh      # Install dependencies
```

---

## File Descriptions

### Root Level Files

#### `main.py`
- Main entry point for the application
- Handles application lifecycle
- Routes to different jobs/applications

#### `requirements.txt`
```
pyspark==3.5.0
py4j==0.10.9.7
pandas==2.0.0
pytest==7.0.0
```

#### `setup.py`
Allows package installation: `pip install -e .`

---

### `config/`

#### `spark_config.py`
```python
# Spark configuration settings
SPARK_CONFIG = {
    'spark.app.name': 'MySparkApp',
    'spark.master': 'local[*]',
    'spark.sql.shuffle.partitions': 4,
    'spark.driver.memory': '2g',
    'spark.executor.memory': '2g',
}
```

---

### `src/`

#### `app.py`
Base application class with common functionality:
- SparkSession initialization
- Error handling
- Logging setup
- Resource cleanup

#### `jobs/`
Individual job modules:
- `data_processing.py` - Transform and clean data
- `aggregation.py` - Aggregate and summarize data
- `reporting.py` - Generate reports

#### `utils/`
Helper functions:
- `data_loader.py` - Read CSV, Parquet, JSON, etc.
- `data_saver.py` - Write to various formats
- `transformations.py` - Common Spark transformations
- `validation.py` - Data quality checks

#### `models/`
Data structures:
- `data_models.py` - Define DataFrame schemas

---

### `tests/`

Unit tests for your code:
```python
# tests/test_transformations.py
import pytest
from src.utils.transformations import clean_data

def test_clean_data():
    # Test your transformations
    pass
```

---

### `data/`

Data directories:
- `input/` - Raw input data
- `output/` - Processed output data
- `sample/` - Small sample data for testing

---

### `scripts/`

Executable scripts for deployment:
- `run_local.sh` - Run locally
- `run_cluster.sh` - Run on Spark cluster
- `install_dependencies.sh` - Install all dependencies

---

## Example File Contents

### `config/spark_config.py`
```python
"""Spark configuration settings"""

# Local development
LOCAL_CONFIG = {
    'app_name': 'MySparkApp',
    'master': 'local[*]',
    'spark.sql.shuffle.partitions': 4,
    'spark.driver.memory': '2g',
    'spark.executor.memory': '2g',
    'spark.sql.adaptive.enabled': 'true',
}

# Cluster configuration
CLUSTER_CONFIG = {
    'app_name': 'MySparkApp',
    'master': 'spark://cluster-master:7077',
    'spark.executor.instances': 10,
    'spark.executor.cores': 4,
    'spark.executor.memory': '4g',
    'spark.driver.memory': '4g',
}

# YARN configuration
YARN_CONFIG = {
    'app_name': 'MySparkApp',
    'master': 'yarn',
    'spark.submit.deployMode': 'cluster',
    'spark.executor.instances': 10,
}
```

### `src/utils/data_loader.py`
```python
"""Data loading utilities"""

from pyspark.sql import SparkSession

def load_csv(spark: SparkSession, path: str, **options):
    """Load CSV file into DataFrame"""
    return spark.read.csv(path, header=True, inferSchema=True, **options)

def load_parquet(spark: SparkSession, path: str):
    """Load Parquet file into DataFrame"""
    return spark.read.parquet(path)

def load_json(spark: SparkSession, path: str):
    """Load JSON file into DataFrame"""
    return spark.read.json(path)
```

### `src/utils/transformations.py`
```python
"""Common transformation functions"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower

def clean_column_names(df: DataFrame) -> DataFrame:
    """Clean column names: lowercase, remove spaces"""
    for column in df.columns:
        df = df.withColumnRenamed(column, column.lower().replace(" ", "_"))
    return df

def remove_nulls(df: DataFrame, subset=None) -> DataFrame:
    """Remove rows with null values"""
    return df.dropna(subset=subset)

def trim_strings(df: DataFrame) -> DataFrame:
    """Trim whitespace from string columns"""
    for col_name in df.columns:
        if col_name.startswith("string"):
            df = df.withColumn(col_name, trim(col(col_name)))
    return df
```

### `src/jobs/data_processing.py`
```python
"""Data processing job"""

from pyspark.sql import SparkSession
from src.utils.data_loader import load_csv
from src.utils.transformations import clean_column_names, remove_nulls

def run(spark: SparkSession):
    """Main job logic"""
    
    # Load data
    df = load_csv(spark, "data/input/raw_data.csv")
    
    # Transform
    df = clean_column_names(df)
    df = remove_nulls(df)
    
    # Save
    df.write.parquet("data/output/processed_data", mode="overwrite")
```

### `tests/test_transformations.py`
```python
"""Unit tests for transformations"""

import pytest
from pyspark.sql import SparkSession
from src.utils.transformations import clean_column_names

@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for tests"""
    return SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .getOrCreate()

def test_clean_column_names(spark):
    """Test column name cleaning"""
    data = [(1, "Alice"), (2, "Bob")]
    df = spark.createDataFrame(data, ["User ID", "User Name"])
    
    cleaned = clean_column_names(df)
    
    assert "user_id" in cleaned.columns
    assert "user_name" in cleaned.columns
```

### `scripts/run_local.sh`
```bash
#!/bin/bash
# Run Spark application locally

set -e  # Exit on error

echo "Installing dependencies..."
pip install -r requirements.txt

echo "Running application..."
python main.py

echo "Done!"
```

### `scripts/run_cluster.sh`
```bash
#!/bin/bash
# Submit Spark job to cluster

spark-submit \
    --master spark://cluster-master:7077 \
    --deploy-mode cluster \
    --num-executors 10 \
    --executor-cores 4 \
    --executor-memory 4g \
    --driver-memory 4g \
    main.py
```

---

## Development Workflow

### 1. **Local Development**
```bash
cd my_spark_project
python -m venv venv
source venv/bin/activate  # or: venv\Scripts\activate on Windows
pip install -r requirements.txt
python main.py
```

### 2. **Running Tests**
```bash
pytest tests/
pytest tests/test_transformations.py -v
```

### 3. **Package Installation**
```bash
pip install -e .  # Install in editable mode
```

### 4. **Cluster Deployment**
```bash
chmod +x scripts/run_cluster.sh
./scripts/run_cluster.sh
```

---

## Best Practices

1. **Separate Concerns**
   - Business logic in `jobs/`
   - Utilities in `utils/`
   - Configuration in `config/`

2. **Reusability**
   - Create shared utilities in `utils/`
   - Don't repeat code between jobs

3. **Testing**
   - Write unit tests for utilities
   - Use pytest fixtures for SparkSession

4. **Configuration Management**
   - Store configs in `config/`
   - Use environment variables for secrets
   - Don't hardcode paths or settings

5. **Error Handling**
   - Use try/except in main application
   - Log all errors
   - Provide meaningful error messages

6. **Documentation**
   - Document each module
   - Include README with setup instructions
   - Add docstrings to functions

7. **Version Control**
   - Use `.gitignore` for `data/`, logs, etc.
   - Track `requirements.txt` and `setup.py`
   - Document environment setup

---

## Quick Start Commands

```bash
# Create project structure
mkdir -p my_spark_project/{src/{jobs,utils,models},tests,config,data/{input,output,sample},scripts}

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install pyspark pytest pandas

# Run application
python main.py

# Run tests
pytest tests/

# Submit to cluster
spark-submit main.py
```

---

## Integration with IDEs

### Visual Studio Code
Create `.vscode/settings.json`:
```json
{
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "python.formatting.provider": "black",
    "editor.formatOnSave": true
}
```

### PyCharm
- Mark `src/` as "Sources Root"
- Set Python interpreter to venv
- Run configurations for `main.py` and tests
