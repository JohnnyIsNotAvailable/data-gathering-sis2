"""
Data Cleaner for Trustpilot Reviews

This module cleans and validates scraped review data using pandas and pandera.
"""

import json
import logging
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column, Check, DataFrameSchema

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Define Pandera schema for validated reviews
ReviewSchema = DataFrameSchema(
    {
        "review_id": Column(
            str,
            nullable=False,
            unique=True,
            description="Unique identifier for the review"
        ),
        "rating": Column(
            int,
            Check.in_range(1, 5),
            nullable=False,
            description="Star rating from 1 to 5"
        ),
        "title": Column(
            str,
            Check(lambda x: len(x) > 0, error="Title cannot be empty"),
            nullable=False,
            description="Review title/headline"
        ),
        "body": Column(
            str,
            nullable=True,
            description="Review body text"
        ),
        "reviewer_name": Column(
            str,
            Check(lambda x: len(x) > 0, error="Reviewer name cannot be empty"),
            nullable=False,
            description="Name of the reviewer"
        ),
        "date": Column(
            "datetime64[ns]",
            nullable=False,
            description="Date of the review"
        ),
        "is_verified": Column(
            bool,
            nullable=False,
            description="Whether the review is verified"
        ),
    },
    coerce=True,
    strict=True
)


class ReviewCleaner:
    """Cleaner for Trustpilot review data."""

    def __init__(self, input_path: str, output_path: str):
        """
        Initialize the cleaner.

        Args:
            input_path: Path to raw JSON data
            output_path: Path to save cleaned data
        """
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.df = None
        self.cleaned_df = None

    def load_data(self) -> pd.DataFrame:
        """Load raw JSON data into a DataFrame."""
        logger.info(f"Loading data from {self.input_path}")

        with open(self.input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        self.df = pd.DataFrame(data)
        logger.info(f"Loaded {len(self.df)} records")
        return self.df

    def remove_duplicates(self) -> pd.DataFrame:
        """Remove duplicate reviews based on title and body."""
        initial_count = len(self.df)

        # Remove exact duplicates
        self.df = self.df.drop_duplicates()

        # Remove duplicates based on title + reviewer_name (same person, same review)
        self.df = self.df.drop_duplicates(subset=['title', 'reviewer_name'], keep='first')

        removed = initial_count - len(self.df)
        logger.info(f"Removed {removed} duplicate records")
        return self.df

    def handle_missing_values(self) -> pd.DataFrame:
        """Handle missing values in the dataset."""
        # Log missing values before cleaning
        missing_before = self.df.isnull().sum()
        logger.info(f"Missing values before cleaning:\n{missing_before[missing_before > 0]}")

        # Generate review_id if missing (use index + timestamp hash)
        if 'review_id' in self.df.columns:
            mask = self.df['review_id'].isnull() | (self.df['review_id'] == '')
            self.df.loc[mask, 'review_id'] = [
                f"gen_{i}_{hash(str(row['title']) + str(row['date']))}"
                for i, (_, row) in enumerate(self.df[mask].iterrows())
            ]

        # Fill missing body with title (some reviews only have title)
        self.df['body'] = self.df['body'].fillna(self.df['title'])

        # Fill missing reviewer_name with 'Anonymous'
        self.df['reviewer_name'] = self.df['reviewer_name'].fillna('Anonymous')
        self.df['reviewer_name'] = self.df['reviewer_name'].replace('', 'Anonymous')

        # Fill missing is_verified with False
        self.df['is_verified'] = self.df['is_verified'].fillna(False)

        # Drop rows with missing critical fields (rating, title, date)
        critical_fields = ['rating', 'title', 'date']
        rows_before = len(self.df)
        self.df = self.df.dropna(subset=critical_fields)
        rows_dropped = rows_before - len(self.df)

        if rows_dropped > 0:
            logger.warning(f"Dropped {rows_dropped} rows with missing critical fields")

        logger.info(f"Missing values after cleaning:\n{self.df.isnull().sum()[self.df.isnull().sum() > 0]}")
        return self.df

    def normalize_text_fields(self) -> pd.DataFrame:
        """Normalize text fields (title, body, reviewer_name)."""

        def clean_text(text):
            if pd.isna(text) or text is None:
                return text
            # Convert to string
            text = str(text)
            # Remove extra whitespace
            text = re.sub(r'\s+', ' ', text)
            # Strip leading/trailing whitespace
            text = text.strip()
            # Remove special characters but keep basic punctuation
            text = re.sub(r'[^\w\s.,!?\'"-]', '', text)
            return text

        # Clean text fields
        self.df['title'] = self.df['title'].apply(clean_text)
        self.df['body'] = self.df['body'].apply(clean_text)
        self.df['reviewer_name'] = self.df['reviewer_name'].apply(clean_text)

        logger.info("Text fields normalized")
        return self.df

    def convert_types(self) -> pd.DataFrame:
        """Convert columns to appropriate data types."""
        # Convert rating to integer
        self.df['rating'] = pd.to_numeric(self.df['rating'], errors='coerce').astype('Int64')

        # Convert date to datetime
        self.df['date'] = pd.to_datetime(self.df['date'], errors='coerce', utc=True)
        # Remove timezone info for SQLite compatibility
        self.df['date'] = self.df['date'].dt.tz_localize(None)

        # Convert is_verified to boolean
        self.df['is_verified'] = self.df['is_verified'].astype(bool)

        # Ensure review_id is string
        self.df['review_id'] = self.df['review_id'].astype(str)

        logger.info("Data types converted")
        return self.df

    def validate_schema(self) -> pd.DataFrame:
        """Validate the DataFrame against the pandera schema."""
        logger.info("Validating data against schema...")

        try:
            validated_df = ReviewSchema.validate(self.df, lazy=True)
            logger.info("Schema validation passed")
            return validated_df
        except pa.errors.SchemaErrors as e:
            logger.error(f"Schema validation failed:\n{e.failure_cases}")
            # Return rows that passed validation
            invalid_indices = e.failure_cases['index'].unique()
            valid_df = self.df.drop(index=invalid_indices, errors='ignore')
            logger.warning(f"Removed {len(invalid_indices)} invalid rows")
            return valid_df

    def clean(self) -> pd.DataFrame:
        """
        Run the full cleaning pipeline.

        Returns:
            Cleaned and validated DataFrame
        """
        logger.info("Starting cleaning pipeline...")

        # Step 1: Load data
        self.load_data()
        logger.info(f"Initial record count: {len(self.df)}")

        # Step 2: Remove duplicates
        self.remove_duplicates()

        # Step 3: Handle missing values
        self.handle_missing_values()

        # Step 4: Normalize text fields
        self.normalize_text_fields()

        # Step 5: Convert data types
        self.convert_types()

        # Step 6: Validate against schema
        self.cleaned_df = self.validate_schema()

        logger.info(f"Final record count: {len(self.cleaned_df)}")
        logger.info("Cleaning pipeline completed")

        return self.cleaned_df

    def save_cleaned_data(self) -> None:
        """Save cleaned data to JSON file."""
        if self.cleaned_df is None:
            raise ValueError("No cleaned data to save. Run clean() first.")

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert datetime to ISO format string for JSON serialization
        df_to_save = self.cleaned_df.copy()
        df_to_save['date'] = df_to_save['date'].dt.strftime('%Y-%m-%d %H:%M:%S')

        records = df_to_save.to_dict(orient='records')

        with open(self.output_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(records)} cleaned records to {self.output_path}")

    def get_summary(self) -> dict:
        """Get summary statistics of the cleaned data."""
        if self.cleaned_df is None:
            return {}

        summary = {
            'total_records': len(self.cleaned_df),
            'rating_distribution': self.cleaned_df['rating'].value_counts().to_dict(),
            'verified_count': int(self.cleaned_df['is_verified'].sum()),
            'date_range': {
                'min': str(self.cleaned_df['date'].min()),
                'max': str(self.cleaned_df['date'].max())
            },
            'avg_title_length': round(self.cleaned_df['title'].str.len().mean(), 2),
            'avg_body_length': round(self.cleaned_df['body'].str.len().mean(), 2)
        }
        return summary


def main():
    """Main function to run the cleaner."""
    # Configuration
    base_path = Path(__file__).parent.parent / "data"
    input_path = base_path / "raw_reviews.json"
    output_path = base_path / "cleaned_reviews.json"

    # Initialize and run cleaner
    cleaner = ReviewCleaner(str(input_path), str(output_path))

    logger.info("Starting data cleaning process")

    # Run cleaning pipeline
    cleaned_df = cleaner.clean()

    # Save cleaned data
    cleaner.save_cleaned_data()

    # Print summary
    summary = cleaner.get_summary()
    print("\n" + "=" * 50)
    print("CLEANING SUMMARY")
    print("=" * 50)
    print(f"Total records: {summary['total_records']}")
    print(f"\nRating distribution:")
    for rating, count in sorted(summary['rating_distribution'].items(), reverse=True):
        print(f"  {rating} stars: {count}")
    print(f"\nVerified reviews: {summary['verified_count']}")
    print(f"Date range: {summary['date_range']['min']} to {summary['date_range']['max']}")
    print(f"Average title length: {summary['avg_title_length']} chars")
    print(f"Average body length: {summary['avg_body_length']} chars")
    print(f"\nOutput saved to: {output_path}")

    return cleaned_df


if __name__ == "__main__":
    main()
