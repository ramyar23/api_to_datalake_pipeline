# API to Data Lake Pipeline

A Python + PySpark ETL pipeline that extracts data from a public REST API, cleans and transforms it, and loads it into a data lake as Parquet files.

## Architecture

```
Public API  --->  Extract (requests)  --->  Transform (PySpark)  --->  Load (Parquet)
```

- **Extract**: Calls a public REST API using Python's `requests` library, with retry logic and a timeout to handle temporary network/server failures.
- **Transform**: Uses PySpark to remove duplicates, drop rows with missing required fields, and add an ingestion timestamp.
- **Load**: Writes the cleaned data to a data lake location as Parquet — a compressed, columnar format optimized for analytics.

## Tech Stack

- Python
- PySpark
- `requests` (API calls)
- Parquet (storage format)

## Project Structure

```
api-to-datalake-pipeline/
├── api_pipeline.py       # main pipeline code
├── requirements.txt      # dependencies
└── README.md
```

## Key Design Choices

- **Class-based design** (`ApiPipeline`) — keeps the Spark session, API URL, and output path stored once, reused across every stage.
- **Method chaining** — `extract().transform().load()` runs the full pipeline in a clear, readable sequence.
- **Retry logic** — the `_fetch_with_retry()` method retries failed API calls up to 3 times with a short delay, instead of failing on the first network hiccup.
- **Structured logging** — every stage logs its progress (`logger.info`) and failures (`logger.error`), instead of using `print()`, so the pipeline is production-style and debuggable.
- **Fail-fast error handling** — errors are logged and then re-raised (`raise`), so failures are visible immediately rather than silently producing incomplete data.
- **Environment-based secrets** — the API key is read from an environment variable (`MY_API_KEY`), never hardcoded in the code.

## How to Run

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. (Optional) Set your API key as an environment variable if the API requires authentication:
   ```
   export MY_API_KEY=your_key_here
   ```

3. Run the pipeline:
   ```
   python api_pipeline.py
   ```

The cleaned output will be written to the `output/users/` folder as Parquet files.

## What I Learned

Building this project helped me understand how real-world data pipelines pull data from live APIs rather than static files — including handling authentication, network failures, and nested JSON structures — and how to structure that logic using classes, logging, and PySpark transformations in a way that mirrors production pipeline code.