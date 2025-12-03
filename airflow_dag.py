import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).parent.absolute()
if PROJECT_ROOT.name == 'dags':
    PROJECT_ROOT = PROJECT_ROOT.parent.parent

sys.path.insert(0, str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_PATH = DATA_DIR / "raw_reviews.json"
CLEANED_DATA_PATH = DATA_DIR / "cleaned_reviews.json"
DB_PATH = DATA_DIR / "reviews.db"
COMPANY_DOMAIN = "finelo.com"
NUM_PAGES = 10


def scrape_reviews(**context):
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


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='trustpilot_reviews_pipeline',
    default_args=default_args,
    description='ETL pipeline for Trustpilot reviews',
    schedule='@daily',
    start_date=datetime(2025, 12, 3),
    catchup=False,
    tags=['trustpilot', 'etl', 'reviews'],
) as dag:

    task_scrape = PythonOperator(
        task_id='scrape_reviews',
        python_callable=scrape_reviews,
    )

    task_clean = PythonOperator(
        task_id='clean_reviews',
        python_callable=clean_reviews,
    )

    task_load = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database,
    )

    task_scrape >> task_clean >> task_load
