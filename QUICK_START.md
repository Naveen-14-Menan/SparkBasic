# 🚀 Quick Start Guide

Get your Spark project running in 5 minutes!

## Step 1: Check Prerequisites (1 minute)

### Verify Python
```bash
python --version
# Should be 3.6 or higher
```

### Verify Java
```bash
java -version
# Should be JDK 8 or higher
```

If Java is not installed:
- **macOS**: `brew install openjdk@11`
- **Ubuntu**: `sudo apt-get install openjdk-11-jdk`
- **Windows**: Download from https://www.oracle.com/java/technologies/downloads/

---

## Step 2: Set Up Environment (2 minutes)

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate

# On Windows:
venv\Scripts\activate

# You should see (venv) at the start of your terminal line
```

---

## Step 3: Install Dependencies (2 minutes)

```bash
# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# This will install:
# - pyspark (3.5.0)
# - py4j (0.10.9.7)
# - pandas, numpy
# - pytest (for testing)
```

---

## Step 4: Verify Installation (1 minute)

```bash
# Test if everything works
python -c "from pyspark.sql import SparkSession; print('✓ PySpark works!')"
```

If you see `✓ PySpark works!`, you're ready to go!

---

## Step 5: Run the Application

```bash
# Run the main application
python main.py
```

You should see output like:

```
2024-01-15 10:30:45 - __main__ - INFO - Starting application logic
2024-01-15 10:30:46 - __main__ - INFO - Creating sample DataFrame
2024-01-15 10:30:46 - __main__ - INFO - DataFrame created with 5 rows
...
+-------+---+-------------+------+
|name   |age|department   |salary|
+-------+---+-------------+------+
|Alice  |25 |Engineering  |80000 |
|Bob    |30 |Sales        |75000 |
|Charlie|35 |Engineering  |95000 |
|Diana  |28 |HR           |70000 |
|Eve    |32 |Sales        |80000 |
+-------+---+-------------+------+
```

---

## 🎯 Common First Steps

### 1. Run a Specific Example

Open `main.py` and uncomment the example you want to run:

```python
# In main.py, find the main() function and uncomment:

results = []

# 1. Run first app (basic DataFrame operations) - UNCOMMENT THIS
results.append(("MyFirstPySparkApp", run_first_app()))

# 2. Uncomment to run data processing app
# results.append(("DataProcessingApp", run_data_processing_app()))

# 3. Uncomment to run RDD example
# results.append(("RDDExampleApp", run_rdd_example_app()))
```

### 2. Load Your Own Data

Replace the sample data in `main.py`:

```python
from SparkBasic.data_loader import DataLoader

loader = DataLoader(spark)

# Load CSV
df = loader.load_csv('path/to/your/data.csv')

# Or use sample data
df = loader.create_sample_employees()

df.show()
```

### 3. Transform Your Data

```python
from SparkBasic.transformations import DataTransformer

# Clean column names and trim strings
df = DataTransformer.clean_column_names(df)
df = DataTransformer.trim_all_strings(df)

# Filter data
df = DataTransformer.filter_by_condition(df, "age > 25")

df.show()
```

### 4. Save Results

```python
# Save as CSV
df.write.csv("output/results.csv", header=True, mode="overwrite")

# Save as Parquet (more efficient)
df.write.parquet("output/results.parquet", mode="overwrite")

# Save as JSON
df.write.json("output/results.json", mode="overwrite")
```

---

## 📊 Project Structure Overview

```
my_spark_project/
├── main.py                    # ← Main entry point (start here!)
├── config.py                  # Configuration for different environments
├── data_loader.py             # Load data from CSV, Parquet, JSON, SQL
├── transformations.py         # Transform and clean data
├── requirements.txt           # Package dependencies
└── README.md                  # Full documentation
```

---

## 🔧 Troubleshooting

### Error: `ModuleNotFoundError: No module named 'pyspark'`

```bash
# Make sure virtual environment is activated
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Then install again
pip install -r requirements.txt
```

### Error: `java.lang.RuntimeException: JAVA_HOME is not set`

```bash
# Find Java installation
which java  # Linux/Mac
where java  # Windows

# Set JAVA_HOME (Linux/Mac)
export JAVA_HOME=/usr/libexec/java_home

# Or add to ~/.bashrc to make it permanent
echo 'export JAVA_HOME=/usr/libexec/java_home' >> ~/.bashrc
source ~/.bashrc
```

### Error: `Address already in use`

This means Spark ports are already in use. Either:
1. Kill the existing Spark process
2. Or use a different port in `main.py`:

```python
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.driver.port", "5050") \
    .config("spark.blockManager.port", "5051") \
    .getOrCreate()
```

---

## 💡 Pro Tips

### 1. Use spark-submit for Production

Instead of `python main.py`, use:

```bash
spark-submit main.py
```

This is the recommended way to run Spark applications.

### 2. View Spark UI

While your application is running, open browser to:
```
http://localhost:4040
```

You'll see real-time metrics and job information!

### 3. Increase Memory for Large Data

In `main.py`, before creating SparkSession:

```python
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g")  # Change to 4g, 8g, etc.
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

### 4. Run Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov=.
```

---

## 📚 Next Steps

1. ✅ **Complete**: Install and run the basic application
2. 📖 **Read**: Go through the README.md for full documentation
3. 🔧 **Modify**: Update `main.py` with your own logic
4. 📝 **Learn**: Study the examples in `data_loader.py` and `transformations.py`
5. 🚀 **Scale**: Deploy to a Spark cluster using `spark-submit`

---

## 🎓 Learning Resources

Inside the project files:
- **main.py** - See different application examples
- **config.py** - Understand Spark configuration for different environments
- **data_loader.py** - Learn how to load data from different sources
- **transformations.py** - Discover common data transformation patterns

Online resources:
- [PySpark Official Docs](https://spark.apache.org/docs/latest/api/python/)
- [PySpark by Examples](https://sparkbyexamples.com/pyspark-tutorial/)
- [Databricks Learning](https://www.databricks.com/learn/intro-to-apache-spark)

---

## ✨ Congratulations!

You now have a production-ready Spark project! 🎉

### What you have:
- ✅ Modern PySpark with SparkSession
- ✅ Proper project organization
- ✅ Data loading utilities
- ✅ Data transformation utilities
- ✅ Error handling and logging
- ✅ Configuration management
- ✅ Ready to scale and deploy

### Next: Use these files to build your application!

---

## 🆘 Still Having Issues?

1. **Check the logs**: Look at `spark_app.log` for detailed error messages
2. **Review README.md**: Full documentation and troubleshooting
3. **Check config.py**: See all available configurations
4. **Run tests**: `pytest tests/ -v` to verify setup

Happy Sparking! 🚀✨
