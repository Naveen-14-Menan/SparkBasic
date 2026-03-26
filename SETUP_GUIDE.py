#!/usr/bin/env python3
"""
PySpark Project Setup and Initialization Guide

This script helps you understand the project structure and provides
setup instructions for getting started with the Spark project.
"""

import os
import sys
from pathlib import Path


def print_header(text):
    """Print a formatted header"""
    print("\n" + "=" * 80)
    print(f" {text}")
    print("=" * 80)


def print_section(text):
    """Print a formatted section"""
    print(f"\n{'─' * 80}")
    print(f" {text}")
    print(f"{'─' * 80}")


def print_file_description(filename, description):
    """Print file description"""
    print(f"\n  📄 {filename:<30} - {description}")


def show_project_structure():
    """Show project structure"""
    print_header("PROJECT STRUCTURE")
    
    structure = """
my_spark_project/
│
├── 📄 main.py                    Main application entry point
│   └── Contains: SparkApp class, example applications
│   └── Usage: python main.py or spark-submit main.py
│
├── 📄 config.py                  Spark configuration settings
│   └── Contains: Environment configs (local, development, production)
│   └── Usage: get_spark_config('local')
│
├── 📄 data_loader.py             Data loading utilities
│   └── Contains: DataLoader class for CSV, Parquet, JSON, SQL
│   └── Usage: loader = DataLoader(spark); df = loader.load_csv('data.csv')
│
├── 📄 transformations.py         Data transformation utilities
│   └── Contains: DataTransformer class with common operations
│   └── Usage: DataTransformer.clean_column_names(df)
│
├── 📄 requirements.txt           Python dependencies
│   └── Contains: pyspark, pytest, pandas, numpy, etc.
│   └── Usage: pip install -r requirements.txt
│
├── 📄 README.md                  Full documentation
│   └── Contains: Installation, usage, API reference, troubleshooting
│   └── Read this for: Complete understanding of the project
│
├── 📄 QUICK_START.md             Quick start guide
│   └── Contains: 5-minute setup and first steps
│   └── Read this for: Getting started immediately
│
├── 📄 PROJECT_STRUCTURE.md       Detailed structure guide
│   └── Contains: File organization, best practices
│   └── Read this for: Understanding how to extend the project
│
└── 📁 data/
    ├── input/                    Input data directory
    ├── output/                   Output results directory
    └── sample/                   Sample data for testing
"""
    print(structure)


def show_setup_steps():
    """Show setup steps"""
    print_header("SETUP STEPS (5 MINUTES)")
    
    steps = """
Step 1: Check Prerequisites (1 minute)
├── Python: python --version          (Need 3.6+)
├── Java: java -version               (Need JDK 8+)
└── Both installed? → Continue to Step 2

Step 2: Create Virtual Environment (30 seconds)
├── python -m venv venv
└── source venv/bin/activate  (Linux/Mac) OR venv\\Scripts\\activate (Windows)

Step 3: Install Dependencies (2 minutes)
├── pip install --upgrade pip
└── pip install -r requirements.txt

Step 4: Verify Installation (1 minute)
├── python -c "from pyspark.sql import SparkSession; print('✓ Success!')"
└── If error → Check Java and JAVA_HOME settings

Step 5: Run Application (30 seconds)
└── python main.py
"""
    print(steps)


def show_quick_examples():
    """Show quick examples"""
    print_header("QUICK EXAMPLES")
    
    print_section("Example 1: Load Data")
    example1 = """
from pyspark.sql import SparkSession
from data_loader import DataLoader

spark = SparkSession.builder.appName("MyApp").master("local[*]").getOrCreate()
loader = DataLoader(spark)

# Load CSV
df = loader.load_csv('data/employees.csv')

# Or use sample data
df = loader.create_sample_employees()

df.show()
spark.stop()
"""
    print(example1)
    
    print_section("Example 2: Transform Data")
    example2 = """
from transformations import DataTransformer

# Clean column names
df = DataTransformer.clean_column_names(df)

# Remove nulls
df = DataTransformer.remove_nulls(df)

# Add timestamp
df = DataTransformer.add_timestamp_column(df)

df.show()
"""
    print(example2)
    
    print_section("Example 3: Save Results")
    example3 = """
# Save as different formats
df.write.csv("output/results.csv", header=True, mode="overwrite")
df.write.parquet("output/results.parquet", mode="overwrite")
df.write.json("output/results.json", mode="overwrite")
"""
    print(example3)


def show_file_details():
    """Show detailed file information"""
    print_header("FILE DESCRIPTIONS")
    
    files = {
        "main.py": [
            "Entry point of the application",
            "Contains SparkApp base class with lifecycle management",
            "Contains example applications (DataFrame, RDD, Text Processing)",
            "Use: python main.py OR spark-submit main.py"
        ],
        "config.py": [
            "Centralized configuration management",
            "Predefined configs for local, development, production, testing",
            "Database connection configurations",
            "Data format options (CSV, Parquet, JSON)"
        ],
        "data_loader.py": [
            "Load data from various sources",
            "Supports: CSV, Parquet, JSON, SQL, Text files",
            "Create sample data for testing",
            "Includes logging for debugging"
        ],
        "transformations.py": [
            "Common data transformation operations",
            "Functions: clean names, trim strings, remove nulls, etc.",
            "Aggregation and window operations",
            "Conditional columns and row numbering"
        ],
        "requirements.txt": [
            "Python package dependencies",
            "Includes: pyspark, pytest, pandas, numpy",
            "Use: pip install -r requirements.txt"
        ],
        "README.md": [
            "Complete project documentation",
            "Installation, usage, configuration, examples",
            "API reference, deployment, troubleshooting",
            "Start here for comprehensive understanding"
        ],
        "QUICK_START.md": [
            "5-minute quick start guide",
            "Essential setup and first steps",
            "Common first steps and tips",
            "Use: When you need to get going fast"
        ],
        "PROJECT_STRUCTURE.md": [
            "Detailed project organization guide",
            "How to extend the project",
            "Best practices and patterns",
            "Development workflow guidance"
        ]
    }
    
    for filename, descriptions in files.items():
        print(f"\n  {filename}")
        for desc in descriptions:
            print(f"    • {desc}")


def show_common_commands():
    """Show common commands"""
    print_header("COMMON COMMANDS")
    
    commands = {
        "Setup & Installation": [
            ("python -m venv venv", "Create virtual environment"),
            ("source venv/bin/activate", "Activate (Linux/Mac)"),
            ("venv\\Scripts\\activate", "Activate (Windows)"),
            ("pip install -r requirements.txt", "Install dependencies"),
            ("python -c \"import pyspark; print(pyspark.__version__)\"", "Check PySpark version"),
        ],
        "Running Applications": [
            ("python main.py", "Run application directly"),
            ("spark-submit main.py", "Run with spark-submit (recommended)"),
            ("python main.py 2>&1 | tee output.log", "Run and save output"),
        ],
        "Testing": [
            ("pytest tests/", "Run all tests"),
            ("pytest tests/ -v", "Run with verbose output"),
            ("pytest tests/ --cov=.", "Run with coverage report"),
        ],
        "Troubleshooting": [
            ("java -version", "Check Java installation"),
            ("which java", "Find Java location (Linux/Mac)"),
            ("where java", "Find Java location (Windows)"),
            ("tail -f spark_app.log", "View application logs"),
        ],
    }
    
    for category, cmd_list in commands.items():
        print_section(category)
        for cmd, description in cmd_list:
            print(f"  $ {cmd}")
            print(f"    ↳ {description}\n")


def show_next_steps():
    """Show next steps"""
    print_header("NEXT STEPS")
    
    steps = """
1. ✅ IMMEDIATE (Now)
   └── Read: QUICK_START.md
   └── Run: python main.py
   └── Time: 5 minutes

2. 📖 LEARNING (Next 30 minutes)
   └── Read: README.md (full documentation)
   └── Study: data_loader.py (how to load data)
   └── Study: transformations.py (how to transform data)

3. 🔧 CUSTOMIZATION (Next 1 hour)
   └── Modify: main.py (add your own logic)
   └── Load: Your own data using DataLoader
   └── Transform: Data using DataTransformer
   └── Save: Results in desired format

4. 🚀 DEPLOYMENT (Next steps)
   └── Test: Run pytest tests/
   └── Scale: Use spark-submit for larger jobs
   └── Deploy: To Spark cluster or cloud

5. 📚 ADVANCED (As needed)
   └── Extend: Add more data sources
   └── Create: Custom transformations
   └── Optimize: Tune Spark configuration
   └── Monitor: Use Spark UI (http://localhost:4040)
"""
    print(steps)


def show_environment_info():
    """Show current environment information"""
    print_header("ENVIRONMENT INFORMATION")
    
    print(f"\nPython Information:")
    print(f"  Version: {sys.version}")
    print(f"  Executable: {sys.executable}")
    
    print(f"\nProject Directory:")
    current_dir = os.getcwd()
    print(f"  Current: {current_dir}")
    
    print(f"\nKey Files Status:")
    files_to_check = [
        "main.py",
        "config.py",
        "data_loader.py",
        "transformations.py",
        "requirements.txt",
        "README.md"
    ]
    
    for filename in files_to_check:
        exists = "✓" if os.path.exists(filename) else "✗"
        print(f"  {exists} {filename}")


def main():
    """Main function"""
    print_header("PYSPARK PROJECT SETUP GUIDE")
    
    print("\nWelcome to your PySpark project!")
    print("This guide will help you understand and set up the project.\n")
    
    # Show components
    show_environment_info()
    show_project_structure()
    show_setup_steps()
    show_file_details()
    show_common_commands()
    show_quick_examples()
    show_next_steps()
    
    # Final message
    print_header("READY TO START?")
    print("""
1. If you haven't set up yet:
   → Read QUICK_START.md
   → Run: pip install -r requirements.txt
   → Run: python main.py

2. If you want to understand the project:
   → Read README.md (comprehensive documentation)
   → Review each Python file with comments

3. If you want to customize:
   → Modify main.py with your logic
   → Use DataLoader and DataTransformer utilities
   → Test with pytest

4. If you have questions:
   → Check PROJECT_STRUCTURE.md for organization patterns
   → See README.md for troubleshooting
   → Review code comments in each file

Happy Sparking! 🚀✨
""")


if __name__ == "__main__":
    main()
