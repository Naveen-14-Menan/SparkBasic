# SparkBasic

Data engineering practice with PySpark: a small, tested library of DataFrame
transformations plus example Spark apps. This is the foundation for a series of
production-style pipeline projects.

## Status
- `transformations.py`: tested (pytest, local SparkSession), linted (ruff), CI on GitHub Actions.
- `main.py`, `data_loader.py`, `config.py`: example/scaffold code, not yet tested or wired together.

## Run it
Requires Python 3.9+ and a JDK (Spark 3.5 supports Java 8, 11 or 17).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check transformations.py tests
pytest --cov=transformations
```

## Roadmap
1. Restructure into a `src/` package, add types, test the loaders.
2. Batch lakehouse pipeline (bronze/silver/gold, Parquet/Delta).
3. Orchestrated pipeline with data-quality checks.
4. Streaming pipeline (Kafka + Structured Streaming).
