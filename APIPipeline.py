"""
API to Data Lake Pipeline
==========================
Extracts data from a public API, cleans it with PySpark, and loads it
as Parquet into a data lake.
"""

import os
import time
import logging
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def create_spark_session(app_name="ApiPipeline"):
    return SparkSession.builder.appName(app_name).getOrCreate()


class ApiPipeline:
    def __init__(self, spark, api_url, output_path, max_retries=3):
        self.spark = spark
        self.api_url = api_url
        self.output_path = output_path
        self.max_retries = max_retries
        self.df = None

    def _fetch_with_retry(self):
        api_key = os.environ.get("MY_API_KEY")
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(self.api_url, headers=headers, timeout=10)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt} failed: {e}")
                if attempt == self.max_retries:
                    logger.error("All retry attempts failed")
                    raise
                time.sleep(2)

    def extract(self):
        logger.info(f"Extracting from {self.api_url}")
        response = self._fetch_with_retry()
        raw_data = response.json()
        logger.info(f"Received {len(raw_data)} records")
        self.df = self.spark.createDataFrame(raw_data)
        return self

    def transform(self):
        logger.info("Transforming data")
        self.df = self.df.dropDuplicates()
        self.df = self.df.na.drop(subset=["id"])
        self.df = self.df.withColumn("ingested_at", F.current_timestamp())
        return self

    def load(self):
        logger.info(f"Writing to {self.output_path}")
        self.df.write.mode("overwrite").parquet(self.output_path)
        return self

    def run(self):
        try:
            self.extract().transform().load()
            logger.info("Pipeline completed successfully")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


def main():
    spark = create_spark_session("ApiPipeline")
    pipeline = ApiPipeline(
        spark=spark,
        api_url="https://jsonplaceholder.typicode.com/users",
        output_path="output/users/",
    )
    pipeline.run()
    spark.stop()


if __name__ == "__main__":
    main()