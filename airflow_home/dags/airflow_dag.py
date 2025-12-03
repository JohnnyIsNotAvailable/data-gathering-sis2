"""
Airflow DAG for Trustpilot Reviews Data Pipeline

This DAG orchestrates the data pipeline:
1. Scrape reviews from Trustpilot
2. Clean and validate the data
3. Load into SQLite database

Schedule: Daily (no more than once per 24 hours)
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Project root - use absolute path for reliability
# When DAG is in dags folder, we need to go up to find the project
PROJECT_ROOT = Path(__file__).parent.absolute()

# If we're in a dags subfolder, go up one level to find project root
if PROJECT_ROOT.name == 'dags':
    PROJECT_ROOT = PROJECT_ROOT.parent.parent

# Add project root to path for imports
sys.path.insert(0, str(PROJECT_ROOT))

# Configuration
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_DIR / "raw_reviews.json"
CLEANED_DATA_PATH = DATA_DIR / "cleaned_reviews.json"
DB_PATH = DATA_DIR / "reviews.db"
COMPANY_DOMAIN = "finelo.com"
NUM_PAGES = 10


def scrape_reviews(**context):
    """Task 1: Scrape reviews from Trustpilot."""
    # Import at runtime to avoid module path issues
    from src.scraper import TrustpilotScraper

    print(f"Starting scraping task for {COMPANY_DOMAIN}")
    print(f"Target: {NUM_PAGES} pages")

    scraper = TrustpilotScraper(COMPANY_DOMAIN, headless=True)
    reviews = scraper.scrape_all_pages(num_pages=NUM_PAGES)
    scraper.save_to_json(str(RAW_DATA_PATH))

    print(f"Scraping complete. Total reviews: {len(reviews)}")
    print(f"Output saved to: {RAW_DATA_PATH}")

    return len(reviews)


def clean_reviews(**context):
    """Task 2: Clean and validate scraped reviews."""
    # Import at runtime to avoid module path issues
    from src.cleaner import ReviewCleaner

    print(f"Starting cleaning task")
    print(f"Input: {RAW_DATA_PATH}")

    cleaner = ReviewCleaner(str(RAW_DATA_PATH), str(CLEANED_DATA_PATH))
    cleaned_df = cleaner.clean()
    cleaner.save_cleaned_data()

    summary = cleaner.get_summary()
    print(f"Cleaning complete. Records: {summary['total_records']}")
    print(f"Output saved to: {CLEANED_DATA_PATH}")

    return summary['total_records']


def load_to_database(**context):
    """Task 3: Load cleaned data into SQLite database."""
    # Import at runtime to avoid module path issues
    from src.loader import ReviewLoader

    print(f"Starting loading task")
    print(f"Input: {CLEANED_DATA_PATH}")
    print(f"Database: {DB_PATH}")

    loader = ReviewLoader(str(DB_PATH))

    try:
        loader.connect()
        loader.create_table()
        inserted = loader.load_from_json(str(CLEANED_DATA_PATH))

        summary = loader.get_summary()
        print(f"Loading complete. Records inserted: {inserted}")
        print(f"Total records in DB: {summary['total_records']}")
        print(f"Average rating: {summary['average_rating']}")

    finally:
        loader.close()

    return inserted


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='trustpilot_reviews_pipeline',
    default_args=default_args,
    description='ETL pipeline for Trustpilot reviews: scrape -> clean -> load',
    schedule='@daily',  # Run once per day
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['trustpilot', 'etl', 'reviews'],
) as dag:

    # Task 1: Scrape reviews
    task_scrape = PythonOperator(
        task_id='scrape_reviews',
        python_callable=scrape_reviews,
        doc_md="""
        ### Scrape Reviews
        Scrapes customer reviews from Trustpilot using Playwright.
        - Target: finelo.com
        - Pages: 10 (~200 reviews)
        - Output: data/raw_reviews.json
        """,
    )

    # Task 2: Clean reviews
    task_clean = PythonOperator(
        task_id='clean_reviews',
        python_callable=clean_reviews,
        doc_md="""
        ### Clean Reviews
        Cleans and validates the scraped data using pandas and pandera.
        - Remove duplicates
        - Handle missing values
        - Normalize text fields
        - Validate schema
        - Output: data/cleaned_reviews.json
        """,
    )

    # Task 3: Load to database
    task_load = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database,
        doc_md="""
        ### Load to Database
        Loads cleaned data into SQLite database.
        - Creates table if not exists
        - Inserts/updates records
        - Output: data/reviews.db
        """,
    )

    # Define task dependencies: scrape -> clean -> load
    task_scrape >> task_clean >> task_load
