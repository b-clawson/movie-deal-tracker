"""
Letterboxd list scraper - extracts movie titles from public lists.
Uses Playwright headless browser to avoid 403 blocks.
"""

import re
import time
import random
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Optional
from dataclasses import dataclass
from contextlib import contextmanager

# Playwright imports - use sync API
from playwright.sync_api import sync_playwright, Browser, Page, Playwright

logger = logging.getLogger(__name__)

# Static mapping for known generic titles that benefit from alternative search terms
# Format: (title_lower, year): [alternative_titles]
# This is a reliable fallback when Letterboxd lookup fails or is blocked
KNOWN_ALTERNATIVES = {
    ("house", 1977): ["Hausu", "ハウス"],
    ("ring", 1998): ["Ringu", "リング"],
    ("pulse", 2001): ["Kairo", "回路"],
    ("cure", 1997): ["Kyua", "キュア"],
    ("audition", 1999): ["Ôdishon", "オーディション"],
    ("mother", 2009): ["Madeo", "마더"],
    ("dark water", 2002): ["Honogurai mizu no soko kara", "仄暗い水の底から"],
    ("one cut of the dead", 2017): ["Kamera o tomeru na!", "カメラを止めるな!"],
    ("battle royale", 2000): ["Batoru rowaiaru", "バトル・ロワイアル"],
    ("oldboy", 2003): ["Oldeuboi", "올드보이"],
    ("sympathy for mr. vengeance", 2002): ["Boksuneun naui geot", "복수는 나의 것"],
    ("lady vengeance", 2005): ["Chinjeolhan geumjassi", "친절한 금자씨"],
    ("i saw the devil", 2010): ["Akmareul boatda", "악마를 보았다"],
    ("the host", 2006): ["Gwoemul", "괴물"],
    ("train to busan", 2016): ["Busanhaeng", "부산행"],
    ("parasite", 2019): ["Gisaengchung", "기생충"],
    ("memories of murder", 2003): ["Salinui chueok", "살인의 추억"],
    ("a tale of two sisters", 2003): ["Janghwa, Hongryeon", "장화, 홍련"],
    ("thirst", 2009): ["Bakjwi", "박쥐"],
}


@dataclass
class Movie:
    """Represents a movie from a Letterboxd list."""
    title: str
    year: Optional[int] = None
    letterboxd_url: Optional[str] = None
    director: Optional[str] = None
    alternative_titles: Optional[List[str]] = None

    def __str__(self) -> str:
        parts = [self.title]
        if self.year:
            parts[0] = f"{self.title} ({self.year})"
        if self.director:
            parts.append(f"dir. {self.director}")
        return " - ".join(parts)

    def get_search_title(self) -> str:
        """
        Get the best title for searching.
        For generic English titles, prefer a more specific alternative title
        (e.g., romanized Japanese title like "Hausu" instead of "House").
        """
        title_lower = self.title.lower()

        # First check static mapping for known titles (most reliable)
        if self.year:
            key = (title_lower, self.year)
            if key in KNOWN_ALTERNATIVES:
                alts = KNOWN_ALTERNATIVES[key]
                # Prefer romanized (ASCII) alternatives
                for alt in alts:
                    if alt.isascii() and len(alt) >= 3:
                        logger.info(f"Using known alternative '{alt}' for '{self.title}' ({self.year})")
                        # Also populate alternative_titles if not set
                        if not self.alternative_titles:
                            self.alternative_titles = alts
                        return alt

        # Fall back to dynamically fetched alternatives
        if not self.alternative_titles:
            return self.title

        # Common generic English words that benefit from alternative titles
        generic_words = {'house', 'ring', 'pulse', 'cure', 'audition', 'mother',
                        'father', 'brother', 'sister', 'home', 'dark', 'gate'}

        # Check if title is generic (single common word or very short)
        if title_lower in generic_words or len(self.title) <= 5:
            # Look for a romanized alternative (Latin characters, not the same as title)
            for alt in self.alternative_titles:
                # Skip if same as original title
                if alt.lower() == title_lower:
                    continue
                # Prefer romanized titles (ASCII-friendly, good for search)
                if alt.isascii() and len(alt) >= 3:
                    logger.debug(f"Using alternative title '{alt}' instead of '{self.title}'")
                    return alt

        return self.title


class LetterboxdScraper:
    """Scrapes movie titles from Letterboxd lists using Playwright headless browser."""

    BASE_URL = "https://letterboxd.com"

    # Retry configuration
    MAX_RETRIES = 3
    BASE_DELAY = 2.0  # Base delay in seconds
    MAX_DELAY = 30.0  # Maximum delay between retries

    def __init__(self):
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None

    def _ensure_browser(self) -> Browser:
        """Ensure Playwright browser is initialized."""
        if self._browser is None:
            logger.info("Starting Playwright browser...")
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ]
            )
            logger.info("Playwright browser started")
        return self._browser

    def close(self):
        """Close the browser and Playwright instance."""
        if self._browser:
            self._browser.close()
            self._browser = None
        if self._playwright:
            self._playwright.stop()
            self._playwright = None
        logger.info("Playwright browser closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _fetch_with_retry(self, url: str, timeout: int = 30000) -> Optional[str]:
        """
        Fetch URL using Playwright with exponential backoff retry.
        Returns HTML content as string, or None if all retries fail.
        Timeout is in milliseconds for Playwright.
        """
        browser = self._ensure_browser()

        for attempt in range(self.MAX_RETRIES):
            page = None
            try:
                # Add random jitter to delay (makes requests look more human)
                if attempt > 0:
                    delay = min(self.BASE_DELAY * (2 ** attempt) + random.uniform(0, 1), self.MAX_DELAY)
                    logger.info(f"Retry {attempt + 1}/{self.MAX_RETRIES} after {delay:.1f}s delay...")
                    time.sleep(delay)

                # Create a new browser context with realistic settings
                context = browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    locale="en-US",
                    timezone_id="America/New_York",
                )

                page = context.new_page()

                # Navigate to the URL
                response = page.goto(url, timeout=timeout, wait_until="domcontentloaded")

                if response is None:
                    logger.warning(f"No response for {url}")
                    continue

                status = response.status

                # If we get 403 or 429, retry with backoff
                if status in (403, 429):
                    logger.warning(f"Got {status} for {url}, will retry...")
                    page.close()
                    context.close()
                    continue

                if status >= 400:
                    logger.warning(f"HTTP {status} for {url}")
                    page.close()
                    context.close()
                    return None

                # Get page content
                content = page.content()
                page.close()
                context.close()
                return content

            except Exception as e:
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                if page:
                    try:
                        page.close()
                    except Exception:
                        pass
                if attempt == self.MAX_RETRIES - 1:
                    logger.error(f"All {self.MAX_RETRIES} retries failed for {url}")
                    return None

        return None

    def scrape_list(self, list_url: str) -> List[Movie]:
        """
        Scrape all movies from a Letterboxd list.
        Uses the /detail/ endpoint which renders without JS.
        Handles pagination automatically.
        """
        import time
        import random

        movies = []
        page = 1

        while True:
            page_url = self._get_page_url(list_url, page)
            logger.info(f"Scraping page {page}: {page_url}")

            page_movies = self._scrape_page(page_url)

            if not page_movies:
                break

            movies.extend(page_movies)
            page += 1

            # Letterboxd lists paginate at 100 items
            if len(page_movies) < 100:
                break

            # Add delay between pagination requests to avoid rate limiting
            delay = random.uniform(1.5, 3.0)
            logger.debug(f"Waiting {delay:.1f}s before next page...")
            time.sleep(delay)

        logger.info(f"Found {len(movies)} movies in list")
        return movies

    def _is_watchlist_url(self, url: str) -> bool:
        """Check if URL is a watchlist (not a regular list)."""
        # Watchlist URLs end with /watchlist/ or /username/watchlist
        return bool(re.search(r'/watchlist/?$', url))

    def _get_page_url(self, list_url: str, page: int) -> str:
        """Generate paginated URL."""
        # Remove trailing slash
        list_url = list_url.rstrip("/")

        # Remove /detail if already present
        if list_url.endswith("/detail"):
            list_url = list_url[:-7]

        # Fix common mistake: /list/watchlist/ should be just /watchlist/
        # Letterboxd watchlists have a different URL structure
        list_url = re.sub(r'/list/watchlist$', '/watchlist', list_url)

        # Watchlists don't have a /detail/ endpoint, use base URL with pagination
        if self._is_watchlist_url(list_url):
            if page == 1:
                return f"{list_url}/"
            return f"{list_url}/page/{page}/"

        # Regular lists use /detail/ endpoint for year info
        if page == 1:
            return f"{list_url}/detail/"
        return f"{list_url}/detail/page/{page}/"

    def _scrape_page(self, url: str) -> List[Movie]:
        """Scrape a single page of the list."""
        html = self._fetch_with_retry(url)
        if not html:
            logger.error(f"Failed to fetch {url} after retries")
            return []

        soup = BeautifulSoup(html, "html.parser")
        movies = []
        seen_urls = set()

        # Method 1: Find film links in the detail view (regular lists)
        # The detail view has links like /film/movie-slug/
        film_links = soup.select('a[href*="/film/"]')

        for link in film_links:
            href = link.get("href", "")

            # Only process actual film links (not user reviews etc)
            if not re.match(r"^/film/[^/]+/?$", href):
                continue

            # Skip duplicates (each film may appear multiple times)
            if href in seen_urls:
                continue
            seen_urls.add(href)

            movie = self._parse_film_link(link, href)
            if movie:
                movies.append(movie)

        # Method 2: Find films via data-target-link (watchlists use this)
        # Elements with data-target-link="/film/movie-slug/"
        data_link_elements = soup.select('[data-target-link*="/film/"]')

        for elem in data_link_elements:
            href = elem.get("data-target-link", "")

            # Only process actual film links
            if not re.match(r"^/film/[^/]+/?$", href):
                continue

            # Skip duplicates
            if href in seen_urls:
                continue
            seen_urls.add(href)

            movie = self._parse_data_link_element(elem, href)
            if movie:
                movies.append(movie)

        return movies

    def _parse_film_link(self, link, href: str) -> Optional[Movie]:
        """Parse movie info from a film link."""
        try:
            # Get title from link text
            title = link.get_text(strip=True)

            if not title:
                return None

            # Try to extract year from title if present (e.g., "Movie Title (2024)")
            year_match = re.search(r"\((\d{4})\)$", title)
            year = None
            if year_match:
                year = int(year_match.group(1))
                title = title[: year_match.start()].strip()

            # Build full Letterboxd URL
            letterboxd_url = f"{self.BASE_URL}{href}"

            return Movie(title=title, year=year, letterboxd_url=letterboxd_url)

        except Exception as e:
            logger.warning(f"Failed to parse movie: {e}")
            return None

    def _parse_data_link_element(self, elem, href: str) -> Optional[Movie]:
        """Parse movie info from a watchlist element with data-target-link."""
        try:
            # For watchlist items, get title from img alt or frame-title
            title = None

            # Try to find title in img alt attribute
            img = elem.select_one('img[alt]')
            if img:
                title = img.get('alt', '').strip()

            # Try frame-title class
            if not title:
                frame_title = elem.select_one('.frame-title')
                if frame_title:
                    title = frame_title.get_text(strip=True)

            # Extract from slug as fallback
            if not title:
                # /film/three-giant-men/ -> Three Giant Men
                slug = href.strip('/').split('/')[-1]
                title = slug.replace('-', ' ').title()

            if not title:
                return None

            # Build full Letterboxd URL
            letterboxd_url = f"{self.BASE_URL}{href}"

            # Watchlist doesn't show year, so we'll need to fetch details later
            return Movie(title=title, year=None, letterboxd_url=letterboxd_url)

        except Exception as e:
            logger.warning(f"Failed to parse watchlist movie: {e}")
            return None

    def fetch_movie_details(self, movie: Movie) -> Movie:
        """
        Fetch additional details (title, director, year) from the movie's Letterboxd page.
        Returns the same Movie object with updated fields.
        """
        if not movie.letterboxd_url:
            return movie

        html = self._fetch_with_retry(movie.letterboxd_url)
        if not html:
            logger.warning(f"Failed to fetch details for {movie.letterboxd_url} after retries")
            return movie

        soup = BeautifulSoup(html, "html.parser")

        # Extract title and year from og:title
        # Format: <meta property="og:title" content="Movie Title (1982)">
        og_title = soup.select_one('meta[property="og:title"]')
        if og_title:
            content = og_title.get("content", "")
            year_match = re.search(r"\((\d{4})\)$", content)
            if year_match:
                # Extract title (everything before the year)
                if not movie.title:
                    movie.title = content[:year_match.start()].strip()
                # Extract year
                if not movie.year:
                    movie.year = int(year_match.group(1))
                    logger.debug(f"Found year for {movie.title}: {movie.year}")
            elif not movie.title:
                # No year in og:title, use whole content as title
                movie.title = content.strip()

        # Extract director from the credits section
        # Format: <a class="contributor" href="/director/...">Director Name</a>
        director_link = soup.select_one('a.contributor[href*="/director/"]')
        if director_link:
            director_name = director_link.get_text(strip=True)
            movie.director = director_name
            logger.debug(f"Found director for {movie.title}: {director_name}")

        # Extract alternative titles
        # Look for the "Alternative Titles" section in the page
        alt_titles = self._extract_alternative_titles(soup)
        if alt_titles:
            movie.alternative_titles = alt_titles
            logger.debug(f"Found {len(alt_titles)} alternative titles for {movie.title}: {alt_titles[:3]}...")

        return movie

    def _extract_alternative_titles(self, soup: BeautifulSoup) -> Optional[List[str]]:
        """Extract alternative titles from a Letterboxd film page."""
        alt_titles = []

        # Method 1: Look for "Alternative Titles" section header
        # The structure is typically: <h3><span>Alternative Titles</span></h3> followed by text
        alt_header = soup.find('h3', string=lambda s: s and 'Alternative' in s)
        if not alt_header:
            # Try finding span inside h3
            for h3 in soup.find_all('h3'):
                span = h3.find('span')
                if span and 'Alternative' in span.get_text():
                    alt_header = h3
                    break

        if alt_header:
            # Get the next sibling or parent's text content
            # Alternative titles are often in a <p> or text node after the header
            next_elem = alt_header.find_next_sibling()
            if next_elem:
                alt_text = next_elem.get_text(strip=True)
                if alt_text:
                    # Split by common delimiters
                    for title in re.split(r'[,،、]', alt_text):
                        title = title.strip()
                        if title and len(title) >= 2:
                            alt_titles.append(title)

        # Method 2: Look in the tab content for alternative titles
        # Letterboxd sometimes puts them in a details tab
        details_section = soup.select_one('.film-details, .tabbed-content')
        if details_section and not alt_titles:
            text = details_section.get_text()
            if 'Alternative' in text:
                # Find the text after "Alternative Titles"
                match = re.search(r'Alternative\s+Titles?\s*[:\s]*([^\n]+)', text, re.IGNORECASE)
                if match:
                    alt_text = match.group(1)
                    for title in re.split(r'[,،、]', alt_text):
                        title = title.strip()
                        if title and len(title) >= 2:
                            alt_titles.append(title)

        # Method 3: Check meta tags for alternate names
        for meta in soup.find_all('meta', attrs={'property': 'og:locale:alternate'}):
            # Sometimes alternative titles are in locale-specific meta tags
            pass  # Letterboxd doesn't use this, but kept for potential future use

        return alt_titles if alt_titles else None

    def enrich_movies(self, movies: List[Movie], delay: float = 0.5) -> List[Movie]:
        """
        Fetch detailed info for all movies in a list.

        Args:
            movies: List of movies to enrich
            delay: Delay between requests to be respectful to Letterboxd

        Returns:
            Same list with enriched movie data
        """
        import time

        total = len(movies)
        logger.info(f"Enriching {total} movies with director info...")

        for i, movie in enumerate(movies, 1):
            logger.info(f"Fetching details {i}/{total}: {movie.title}")
            self.fetch_movie_details(movie)

            # Be respectful to Letterboxd
            if i < total:
                time.sleep(delay)

        enriched_count = sum(1 for m in movies if m.director)
        logger.info(f"Enriched {enriched_count}/{total} movies with director info")

        return movies

    def search_movie_by_title(self, title: str, year: Optional[int] = None) -> Optional[Movie]:
        """
        Search Letterboxd for a movie by title and optionally year.
        Returns the best matching Movie with alternative titles populated.
        """
        # Build search URL
        search_query = title
        if year:
            search_query = f"{title} {year}"

        search_url = f"{self.BASE_URL}/search/films/{requests.utils.quote(search_query)}/"

        html = self._fetch_with_retry(search_url)
        if not html:
            logger.warning(f"Letterboxd search failed for '{search_query}' after retries")
            return None

        soup = BeautifulSoup(html, "html.parser")

        # Find film results - they appear as <span class="film-title-wrapper">
        # or <a href="/film/..."> in search results
        film_links = soup.select('a[href^="/film/"]')

        best_match = None
        for link in film_links:
            href = link.get("href", "")

            # Only process actual film links
            if not re.match(r"^/film/[^/]+/?$", href):
                continue

            # Get title and year from the result
            result_title = link.get_text(strip=True)

            # Look for year in a sibling or parent element
            result_year = None
            parent = link.find_parent(['li', 'div', 'span'])
            if parent:
                year_elem = parent.select_one('.metadata, .film-year, small')
                if year_elem:
                    year_match = re.search(r'(\d{4})', year_elem.get_text())
                    if year_match:
                        result_year = int(year_match.group(1))

            # If we have a year filter, check it matches
            if year and result_year and result_year != year:
                continue

            # Construct Letterboxd URL
            letterboxd_url = f"{self.BASE_URL}{href}"

            # Create Movie object and fetch details
            movie = Movie(title=result_title, year=result_year, letterboxd_url=letterboxd_url)
            self.fetch_movie_details(movie)

            # If year matches or we don't have a year filter, this is our best match
            if movie.year == year or not year:
                best_match = movie
                break

            # Keep first result as fallback
            if not best_match:
                best_match = movie

        if best_match:
            logger.info(f"Found Letterboxd match for '{title}' ({year}): {best_match.title} - alternatives: {best_match.alternative_titles}")
        else:
            logger.warning(f"No Letterboxd match found for '{title}' ({year})")

        return best_match


def get_movies_from_list(list_url: str, enrich: bool = True) -> List[Movie]:
    """
    Convenience function to scrape movies from a list.

    Args:
        list_url: URL of the Letterboxd list
        enrich: If True, fetch director info for each movie (slower but more accurate searches)

    Returns:
        List of Movie objects
    """
    with LetterboxdScraper() as scraper:
        movies = scraper.scrape_list(list_url)

        if enrich and movies:
            movies = scraper.enrich_movies(movies)

        return movies


if __name__ == "__main__":
    # Test the scraper
    logging.basicConfig(level=logging.INFO)
    test_url = "https://letterboxd.com/brandt_clawson/list/my-hater-movie-club-list-2026/"

    print("Testing Playwright-based Letterboxd scraper...")
    movies = get_movies_from_list(test_url, enrich=True)

    print(f"\nFound {len(movies)} movies:\n")
    for i, movie in enumerate(movies, 1):
        print(f"{i:3}. {movie}")
        if movie.alternative_titles:
            print(f"      Alt titles: {movie.alternative_titles[:3]}")
