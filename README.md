# Trustpilot Reviews Pipeline

Data pipeline that scrapes customer reviews from Trustpilot and stores them in SQLite.

## Website

https://www.trustpilot.com/review/finelo.com

Finelo.com is a financial education platform. The site has 15,000+ customer reviews with ratings, titles, and review text.

## Data Flow

```
Trustpilot Website --> Scraper --> raw_reviews.json --> Cleaner --> cleaned_reviews.json --> Loader --> reviews.db
```

## Components

### Scraper (src/scraper.py)
Uses Playwright to extract reviews from Trustpilot pages. Collects rating, title, body, reviewer name, and date from each review.

### Cleaner (src/cleaner.py)
Removes duplicates, handles missing values, and normalizes text fields. Uses Pandera to validate data types and constraints.

### Loader (src/loader.py)
Creates SQLite table and inserts cleaned reviews. Table has indexes on rating and date for faster queries.

### Airflow DAG (airflow_dag.py)
Runs the pipeline daily: scrape -> clean -> load. Has 2 retries with 5 minute delay on failure.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install
```

## Run Scraper

```bash
python src/scraper.py
```

## Run Cleaner

```bash
python src/cleaner.py
```

## Run Loader

```bash
python src/loader.py
```

## Run Airflow

```bash
export AIRFLOW_HOME="$(pwd)/airflow_home"
airflow db migrate
cp airflow_dag.py airflow_home/dags/
airflow dags test trustpilot_reviews_pipeline 2025-12-03
```

## Output

- `data/raw_reviews.json` - scraped reviews
- `data/cleaned_reviews.json` - cleaned reviews
- `data/reviews.db` - SQLite database

## Database Schema

```sql
CREATE TABLE reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id TEXT UNIQUE NOT NULL,
    rating INTEGER NOT NULL CHECK(rating >= 1 AND rating <= 5),
    title TEXT NOT NULL,
    body TEXT,
    reviewer_name TEXT NOT NULL,
    date TIMESTAMP NOT NULL,
    is_verified BOOLEAN NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
