import json
import logging
import re
import time
import random
from pathlib import Path
from playwright.sync_api import sync_playwright, Page

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TrustpilotScraper:

    BASE_URL = "https://www.trustpilot.com/review"

    def __init__(self, company_domain: str, headless: bool = True):
        self.company_domain = company_domain
        self.headless = headless
        self.reviews = []

    def _get_page_url(self, page_num: int) -> str:
        base = f"{self.BASE_URL}/{self.company_domain}"
        if page_num == 1:
            return base
        return f"{base}?page={page_num}"

    def _extract_rating(self, img_src: str) -> int:
        match = re.search(r'stars-(\d)', img_src)
        if match:
            return int(match.group(1))
        return 0

    def _parse_review_card(self, card, page: Page) -> dict:
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
            review_id = card.get_attribute('id')
            if review_id:
                review['review_id'] = review_id

            star_img = card.query_selector('img[src*="stars-"]')
            if star_img:
                src = star_img.get_attribute('src')
                review['rating'] = self._extract_rating(src)

            title_elem = card.query_selector('h2')
            if title_elem:
                review['title'] = title_elem.inner_text().strip()

            body_elem = card.query_selector('[data-service-review-text-typography="true"]')
            if not body_elem:
                paragraphs = card.query_selector_all('p')
                for p in paragraphs:
                    text = p.inner_text().strip()
                    if len(text) > 20 and not re.match(r'^(Date of experience|Updated|Replied)', text):
                        review['body'] = text
                        break
            else:
                review['body'] = body_elem.inner_text().strip()

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

            if not review['reviewer_name']:
                profile_link = card.query_selector('a[href*="/users/"]')
                if profile_link:
                    review['reviewer_name'] = profile_link.inner_text().strip()

            time_elem = card.query_selector('time')
            if time_elem:
                review['date'] = time_elem.get_attribute('datetime')

            card_text = card.inner_text().lower()
            review['is_verified'] = 'verified' in card_text

        except Exception as e:
            logger.warning(f"Error parsing review card: {e}")

        return review

    def scrape_page(self, page: Page, page_num: int) -> list:
        url = self._get_page_url(page_num)
        logger.info(f"Scraping page {page_num}: {url}")

        page.goto(url)
        page.wait_for_selector('article', timeout=10000)
        time.sleep(1)

        review_cards = page.query_selector_all('article')
        logger.info(f"Found {len(review_cards)} review cards on page {page_num}")

        page_reviews = []
        for card in review_cards:
            review = self._parse_review_card(card, page)
            if review['review_id'] or review['title']:
                page_reviews.append(review)

        logger.info(f"Successfully parsed {len(page_reviews)} reviews from page {page_num}")
        return page_reviews

    def scrape_all_pages(self, num_pages: int = 10) -> list:
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
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.reviews, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(self.reviews)} reviews to {filepath}")


def main():
    company_domain = "finelo.com"
    num_pages = 10
    output_path = Path(__file__).parent.parent / "data" / "raw_reviews.json"

    scraper = TrustpilotScraper(company_domain, headless=True)

    logger.info(f"Starting Trustpilot scraper for {company_domain}")
    logger.info(f"Target: {num_pages} pages")

    reviews = scraper.scrape_all_pages(num_pages)
    scraper.save_to_json(str(output_path))

    print(f"\nScraping complete!")
    print(f"Total reviews: {len(reviews)}")
    print(f"Output saved to: {output_path}")

    return reviews


if __name__ == "__main__":
    main()
