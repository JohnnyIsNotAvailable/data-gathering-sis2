import json
import logging
import sqlite3
from pathlib import Path
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reviews (
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
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);
CREATE INDEX IF NOT EXISTS idx_reviews_date ON reviews(date);
CREATE INDEX IF NOT EXISTS idx_reviews_verified ON reviews(is_verified);
"""

INSERT_REVIEW_SQL = """
INSERT OR REPLACE INTO reviews (review_id, rating, title, body, reviewer_name, date, is_verified)
VALUES (?, ?, ?, ?, ?, ?, ?);
"""


class ReviewLoader:

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.conn = None

    def connect(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        logger.info(f"Connected to database: {self.db_path}")

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")

    def create_table(self) -> None:
        if not self.conn:
            raise ConnectionError("Database not connected. Call connect() first.")

        cursor = self.conn.cursor()
        cursor.execute(CREATE_TABLE_SQL)

        for index_sql in CREATE_INDEX_SQL.strip().split(';'):
            if index_sql.strip():
                cursor.execute(index_sql)

        self.conn.commit()
        logger.info("Reviews table and indexes created")

    def load_from_json(self, json_path: str) -> int:
        if not self.conn:
            raise ConnectionError("Database not connected. Call connect() first.")

        with open(json_path, 'r', encoding='utf-8') as f:
            reviews = json.load(f)

        logger.info(f"Loaded {len(reviews)} reviews from {json_path}")
        return self.insert_reviews(reviews)

    def insert_reviews(self, reviews: List[Dict[str, Any]]) -> int:
        if not self.conn:
            raise ConnectionError("Database not connected. Call connect() first.")

        cursor = self.conn.cursor()
        inserted = 0

        for review in reviews:
            try:
                cursor.execute(INSERT_REVIEW_SQL, (
                    review['review_id'],
                    review['rating'],
                    review['title'],
                    review.get('body'),
                    review['reviewer_name'],
                    review['date'],
                    1 if review.get('is_verified', False) else 0
                ))
                inserted += 1
            except sqlite3.Error as e:
                logger.warning(f"Error inserting review {review.get('review_id')}: {e}")

        self.conn.commit()
        logger.info(f"Inserted {inserted} reviews into database")
        return inserted

    def get_record_count(self) -> int:
        if not self.conn:
            raise ConnectionError("Database not connected. Call connect() first.")

        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM reviews")
        return cursor.fetchone()[0]

    def get_rating_distribution(self) -> Dict[int, int]:
        if not self.conn:
            raise ConnectionError("Database not connected. Call connect() first.")

        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT rating, COUNT(*) as count
            FROM reviews
            GROUP BY rating
            ORDER BY rating DESC
        """)
        return {row['rating']: row['count'] for row in cursor.fetchall()}

    def get_sample_reviews(self, limit: int = 5) -> List[Dict]:
        if not self.conn:
            raise ConnectionError("Database not connected. Call connect() first.")

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM reviews ORDER BY date DESC LIMIT {limit}")
        return [dict(row) for row in cursor.fetchall()]

    def get_summary(self) -> Dict[str, Any]:
        if not self.conn:
            raise ConnectionError("Database not connected. Call connect() first.")

        cursor = self.conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM reviews")
        total = cursor.fetchone()[0]

        rating_dist = self.get_rating_distribution()

        cursor.execute("SELECT AVG(rating) FROM reviews")
        avg_rating = round(cursor.fetchone()[0], 2)

        cursor.execute("SELECT COUNT(*) FROM reviews WHERE is_verified = 1")
        verified = cursor.fetchone()[0]

        cursor.execute("SELECT MIN(date), MAX(date) FROM reviews")
        date_row = cursor.fetchone()

        return {
            'total_records': total,
            'rating_distribution': rating_dist,
            'average_rating': avg_rating,
            'verified_count': verified,
            'date_range': {
                'min': date_row[0],
                'max': date_row[1]
            }
        }


def main():
    base_path = Path(__file__).parent.parent / "data"
    json_path = base_path / "cleaned_reviews.json"
    db_path = base_path / "reviews.db"

    loader = ReviewLoader(str(db_path))

    try:
        loader.connect()
        loader.create_table()

        logger.info(f"Loading data from {json_path}")
        inserted = loader.load_from_json(str(json_path))

        summary = loader.get_summary()
        print("\n" + "=" * 50)
        print("LOADING SUMMARY")
        print("=" * 50)
        print(f"Database: {db_path}")
        print(f"Records inserted: {inserted}")
        print(f"Total records in DB: {summary['total_records']}")
        print(f"Average rating: {summary['average_rating']}")
        print(f"\nRating distribution:")
        for rating, count in sorted(summary['rating_distribution'].items(), reverse=True):
            print(f"  {rating} stars: {count}")
        print(f"\nVerified reviews: {summary['verified_count']}")
        print(f"Date range: {summary['date_range']['min']} to {summary['date_range']['max']}")

        print("\n" + "-" * 50)
        print("SAMPLE REVIEWS (most recent)")
        print("-" * 50)
        for review in loader.get_sample_reviews(3):
            print(f"\n[{review['rating']} stars] {review['title']}")
            print(f"  By: {review['reviewer_name']} on {review['date']}")
            if review['body'] and review['body'] != review['title']:
                body_preview = review['body'][:80] + "..." if len(review['body']) > 80 else review['body']
                print(f"  \"{body_preview}\"")

    finally:
        loader.close()

    return inserted


if __name__ == "__main__":
    main()
