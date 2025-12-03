"""
Trustpilot Review Scraper using Playwright

This module scrapes customer reviews from Trustpilot for a specified company.
"""

import json
import logging
import re
import time
import random
from pathlib import Path
from playwright.sync_api import sync_playwright, Page

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TrustpilotScraper:
    """Scraper for Trustpilot reviews using Playwright."""

    BASE_URL = "https://www.trustpilot.com/review"

    def __init__(self, company_domain: str, headless: bool = True):
        """
        Initialize the scraper.

        Args:
            company_domain: The company domain on Trustpilot (e.g., 'finelo.com')
            headless: Whether to run browser in headless mode
        """
        self.company_domain = company_domain
        self.headless = headless
        self.reviews = []

    def _get_page_url(self, page_num: int) -> str:
        """Generate URL for a specific page number."""
        base = f"{self.BASE_URL}/{self.company_domain}"
        if page_num == 1:
            return base
        return f"{base}?page={page_num}"

    def _extract_rating(self, img_src: str) -> int:
        """Extract rating number from star image URL."""
        # URL pattern: .../stars-4.svg or .../stars-5.svg
        match = re.search(r'stars-(\d)', img_src)
        if match:
            return int(match.group(1))
        return 0

    def _parse_review_card(self, card, page: Page) -> dict:
        """
        Parse a single review card element.

        Args:
            card: Playwright element handle for the review card
            page: Playwright page object

        Returns:
            Dictionary with review data
        """
        review = {
            'review_id': None,
            'rating': 0,
            'title': None,
            'body': None,
            'reviewer_name': None,
            'date': None,
            'is_verified': False
        }

        try:
            # Extract review ID from data attribute or element ID
            review_id = card.get_attribute('id')
            if review_id:
                review['review_id'] = review_id

            # Extract rating from star image
            star_img = card.query_selector('img[src*="stars-"]')
            if star_img:
                src = star_img.get_attribute('src')
                review['rating'] = self._extract_rating(src)

            # Extract review title - look for h2 element
            title_elem = card.query_selector('h2')
            if title_elem:
                review['title'] = title_elem.inner_text().strip()

            # Extract review body - look for paragraph with review text
            # Try multiple selectors for the review body
            body_elem = card.query_selector('[data-service-review-text-typography="true"]')
            if not body_elem:
                # Fallback: look for p element that's not part of other sections
                paragraphs = card.query_selector_all('p')
                for p in paragraphs:
                    text = p.inner_text().strip()
                    # Skip short text (likely labels) and date-like text
                    if len(text) > 20 and not re.match(r'^(Date of experience|Updated|Replied)', text):
                        review['body'] = text
                        break
            else:
                review['body'] = body_elem.inner_text().strip()

            # Extract reviewer name
            # Look for the consumer name in the review card
            name_selectors = [
                '[data-consumer-name-typography="true"]',
                'a[name="consumer-profile"]',
                'span[data-consumer-name]'
            ]
            for selector in name_selectors:
                name_elem = card.query_selector(selector)
                if name_elem:
                    review['reviewer_name'] = name_elem.inner_text().strip()
                    break

            # If still no name, try to find it in a different way
            if not review['reviewer_name']:
                # Look for any anchor that links to a user profile
                profile_link = card.query_selector('a[href*="/users/"]')
                if profile_link:
                    review['reviewer_name'] = profile_link.inner_text().strip()

            # Extract date from time element
            time_elem = card.query_selector('time')
            if time_elem:
                review['date'] = time_elem.get_attribute('datetime')

            # Check if review is verified
            card_text = card.inner_text().lower()
            review['is_verified'] = 'verified' in card_text

        except Exception as e:
            logger.warning(f"Error parsing review card: {e}")

        return review

    def scrape_page(self, page: Page, page_num: int) -> list:
        """
        Scrape reviews from a single page.

        Args:
            page: Playwright page object
            page_num: Page number to scrape

        Returns:
            List of review dictionaries
        """
        url = self._get_page_url(page_num)
        logger.info(f"Scraping page {page_num}: {url}")

        page.goto(url)

        # Wait for review cards to load
        page.wait_for_selector('article', timeout=10000)

        # Additional wait for dynamic content
        time.sleep(1)

        # Find all review cards - they're typically in article elements
        review_cards = page.query_selector_all('article')
        logger.info(f"Found {len(review_cards)} review cards on page {page_num}")

        page_reviews = []
        for card in review_cards:
            review = self._parse_review_card(card, page)
            if review['review_id'] or review['title']:  # Only add if we got some data
                page_reviews.append(review)

        logger.info(f"Successfully parsed {len(page_reviews)} reviews from page {page_num}")
        return page_reviews

    def scrape_all_pages(self, num_pages: int = 10) -> list:
        """
        Scrape reviews from multiple pages.

        Args:
            num_pages: Number of pages to scrape

        Returns:
            List of all review dictionaries
        """
        all_reviews = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()

            for page_num in range(1, num_pages + 1):
                try:
                    page_reviews = self.scrape_page(page, page_num)
                    all_reviews.extend(page_reviews)

                    # Random delay between pages to be respectful
                    if page_num < num_pages:
                        delay = random.uniform(1.5, 3.0)
                        logger.info(f"Waiting {delay:.1f}s before next page...")
                        time.sleep(delay)

                except Exception as e:
                    logger.error(f"Error scraping page {page_num}: {e}")
                    continue

            browser.close()

        self.reviews = all_reviews
        logger.info(f"Total reviews scraped: {len(all_reviews)}")
        return all_reviews

    def save_to_json(self, filepath: str) -> None:
        """
        Save scraped reviews to a JSON file.

        Args:
            filepath: Path to save the JSON file
        """
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.reviews, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(self.reviews)} reviews to {filepath}")


def main():
    """Main function to run the scraper."""
    # Configuration
    company_domain = "finelo.com"
    num_pages = 10  # ~20 reviews per page = ~200 reviews
    output_path = Path(__file__).parent.parent / "data" / "raw_reviews.json"

    # Initialize and run scraper
    scraper = TrustpilotScraper(company_domain, headless=True)

    logger.info(f"Starting Trustpilot scraper for {company_domain}")
    logger.info(f"Target: {num_pages} pages")

    reviews = scraper.scrape_all_pages(num_pages)

    # Save results
    scraper.save_to_json(str(output_path))

    # Print summary
    print(f"\nScraping complete!")
    print(f"Total reviews: {len(reviews)}")
    print(f"Output saved to: {output_path}")

    return reviews


if __name__ == "__main__":
    main()
