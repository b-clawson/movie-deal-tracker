"""
Direct scrapers for boutique Blu-ray retailer websites.
Searches retailer sites directly to find special editions that may not appear in Google Shopping.
"""

import re
import logging
import requests
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote_plus

logger = logging.getLogger(__name__)


@dataclass
class RetailerResult:
    """A product found on a retailer site."""
    title: str
    price: Optional[float]
    url: str
    retailer: str
    edition_type: str  # e.g., "Criterion Collection", "Arrow Video"
    thumbnail: Optional[str] = None
    in_stock: bool = True


class RetailerScraper(ABC):
    """Base class for retailer scrapers."""

    name: str = "Unknown"
    base_url: str = ""
    edition_type: str = "Boutique Release"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    @abstractmethod
    def search(self, movie_title: str, year: Optional[int] = None) -> List[RetailerResult]:
        """Search for a movie on this retailer's site."""
        pass

    def _extract_price(self, price_str: str) -> Optional[float]:
        """Extract numeric price from string."""
        if not price_str:
            return None
        matches = re.findall(r'\d+\.?\d*', price_str.replace(',', ''))
        if matches:
            return float(matches[0])
        return None

    def _title_matches(self, product_title: str, search_title: str, alternative_titles: Optional[List[str]] = None) -> bool:
        """Check if the product title contains the search title or any alternative."""
        product_lower = product_title.lower()

        # Build list of all acceptable titles
        titles_to_check = [search_title]
        if alternative_titles:
            titles_to_check.extend(alternative_titles)

        for title in titles_to_check:
            title_lower = title.lower()

            # Direct match
            if title_lower in product_lower:
                return True

            # Check individual words (for multi-word titles)
            title_words = title_lower.split()
            if len(title_words) > 1:
                if all(word in product_lower for word in title_words):
                    return True

        return False


class VinegarSyndromeScraper(RetailerScraper):
    """Scraper for Vinegar Syndrome (Shopify-based)."""

    name = "Vinegar Syndrome"
    base_url = "https://vinegarsyndrome.com"
    edition_type = "Vinegar Syndrome"

    def search(self, movie_title: str, year: Optional[int] = None) -> List[RetailerResult]:
        results = []
        search_url = f"{self.base_url}/search?q={quote_plus(movie_title)}&type=product"

        try:
            response = self.session.get(search_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Vinegar Syndrome search failed: {e}")
            return results

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find product cards - Shopify structure
        products = soup.select('.product-card, .product-item, [data-product-card]')

        for product in products[:10]:  # Limit to 10 results
            try:
                # Get title
                title_elem = product.select_one('.product-card__title, .product-title, h3 a, h2 a')
                if not title_elem:
                    continue
                title = title_elem.get_text(strip=True)

                # Get URL
                link = product.select_one('a[href*="/products/"]')
                if link:
                    url = urljoin(self.base_url, link.get('href', ''))
                else:
                    continue

                # Get price
                price_elem = product.select_one('.price, .product-price, [data-price]')
                price = None
                if price_elem:
                    price = self._extract_price(price_elem.get_text())

                # Get thumbnail
                img = product.select_one('img')
                thumbnail = img.get('src', '') if img else None
                if thumbnail and thumbnail.startswith('//'):
                    thumbnail = 'https:' + thumbnail

                results.append(RetailerResult(
                    title=title,
                    price=price,
                    url=url,
                    retailer=self.name,
                    edition_type=self.edition_type,
                    thumbnail=thumbnail,
                ))
            except Exception as e:
                logger.debug(f"Error parsing Vinegar Syndrome product: {e}")
                continue

        logger.info(f"Vinegar Syndrome: found {len(results)} results for '{movie_title}'")
        return results


class ArrowVideoScraper(RetailerScraper):
    """Scraper for Arrow Video/Arrow Films."""

    name = "Arrow Video"
    base_url = "https://www.arrowfilms.com"
    edition_type = "Arrow Video"

    def search(self, movie_title: str, year: Optional[int] = None) -> List[RetailerResult]:
        results = []
        search_url = f"{self.base_url}/search?q={quote_plus(movie_title)}"

        try:
            response = self.session.get(search_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Arrow Video search failed: {e}")
            return results

        soup = BeautifulSoup(response.text, 'html.parser')

        # Arrow uses a custom React/Astro frontend - look for product data in scripts
        # Try to find product links and titles in the page
        product_links = soup.select('a[href*="/product/"]')

        seen_urls = set()
        for link in product_links[:20]:
            try:
                url = urljoin(self.base_url, link.get('href', ''))
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                # Get title from link text or nearby elements
                title = link.get_text(strip=True)
                if not title or len(title) < 3:
                    continue

                # Try to find price nearby
                parent = link.find_parent(['div', 'article', 'li'])
                price = None
                if parent:
                    price_elem = parent.select_one('[class*="price"], .price')
                    if price_elem:
                        price = self._extract_price(price_elem.get_text())

                # Try to find thumbnail
                thumbnail = None
                if parent:
                    img = parent.select_one('img')
                    if img:
                        thumbnail = img.get('src', '') or img.get('data-src', '')
                        if thumbnail and thumbnail.startswith('//'):
                            thumbnail = 'https:' + thumbnail

                results.append(RetailerResult(
                    title=title,
                    price=price,
                    url=url,
                    retailer=self.name,
                    edition_type=self.edition_type,
                    thumbnail=thumbnail,
                ))
            except Exception as e:
                logger.debug(f"Error parsing Arrow Video product: {e}")
                continue

        logger.info(f"Arrow Video: found {len(results)} results for '{movie_title}'")
        return results


class SeverinFilmsScraper(RetailerScraper):
    """Scraper for Severin Films (Shopify-based)."""

    name = "Severin Films"
    base_url = "https://severinfilms.com"
    edition_type = "Severin Films"

    def search(self, movie_title: str, year: Optional[int] = None) -> List[RetailerResult]:
        results = []
        search_url = f"{self.base_url}/search?q={quote_plus(movie_title)}&type=product"

        try:
            response = self.session.get(search_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Severin Films search failed: {e}")
            return results

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find product links
        product_links = soup.select('a[href*="/products/"]')

        seen_urls = set()
        for link in product_links[:15]:
            try:
                url = urljoin(self.base_url, link.get('href', ''))
                if url in seen_urls or '/collections/' in url:
                    continue
                seen_urls.add(url)

                title = link.get_text(strip=True)
                if not title or len(title) < 3:
                    # Try to get title from img alt
                    img = link.select_one('img')
                    if img:
                        title = img.get('alt', '')
                if not title or len(title) < 3:
                    continue

                # Find price
                parent = link.find_parent(['div', 'article', 'li'])
                price = None
                if parent:
                    price_elem = parent.select_one('.price, .money, [data-price]')
                    if price_elem:
                        price = self._extract_price(price_elem.get_text())

                # Find thumbnail
                thumbnail = None
                img = link.select_one('img') or (parent.select_one('img') if parent else None)
                if img:
                    thumbnail = img.get('src', '') or img.get('data-src', '')
                    if thumbnail and thumbnail.startswith('//'):
                        thumbnail = 'https:' + thumbnail

                results.append(RetailerResult(
                    title=title,
                    price=price,
                    url=url,
                    retailer=self.name,
                    edition_type=self.edition_type,
                    thumbnail=thumbnail,
                ))
            except Exception as e:
                logger.debug(f"Error parsing Severin product: {e}")
                continue

        logger.info(f"Severin Films: found {len(results)} results for '{movie_title}'")
        return results


class GrindHouseVideoScraper(RetailerScraper):
    """Scraper for Grindhouse Video (Shopify-based)."""

    name = "Grindhouse Video"
    base_url = "https://www.grindhousevideo.com"
    edition_type = "Boutique Release"

    def search(self, movie_title: str, year: Optional[int] = None) -> List[RetailerResult]:
        results = []
        search_url = f"{self.base_url}/search?q={quote_plus(movie_title)}&type=product"

        try:
            response = self.session.get(search_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Grindhouse Video search failed: {e}")
            return results

        soup = BeautifulSoup(response.text, 'html.parser')
        product_links = soup.select('a[href*="/products/"]')

        seen_urls = set()
        for link in product_links[:15]:
            try:
                url = urljoin(self.base_url, link.get('href', ''))
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                title = link.get_text(strip=True)
                img = link.select_one('img')
                if not title and img:
                    title = img.get('alt', '')
                if not title or len(title) < 3:
                    continue

                parent = link.find_parent(['div', 'article', 'li'])
                price = None
                if parent:
                    price_elem = parent.select_one('.price, .money')
                    if price_elem:
                        price = self._extract_price(price_elem.get_text())

                thumbnail = None
                if img:
                    thumbnail = img.get('src', '') or img.get('data-src', '')
                    if thumbnail and thumbnail.startswith('//'):
                        thumbnail = 'https:' + thumbnail

                results.append(RetailerResult(
                    title=title,
                    price=price,
                    url=url,
                    retailer=self.name,
                    edition_type=self.edition_type,
                    thumbnail=thumbnail,
                ))
            except Exception as e:
                logger.debug(f"Error parsing Grindhouse product: {e}")
                continue

        logger.info(f"Grindhouse Video: found {len(results)} results for '{movie_title}'")
        return results


class DiabolikDVDScraper(RetailerScraper):
    """Scraper for Diabolik DVD - aggregator of boutique releases."""

    name = "Diabolik DVD"
    base_url = "https://www.diabolikdvd.com"
    edition_type = "Boutique Release"

    def search(self, movie_title: str, year: Optional[int] = None) -> List[RetailerResult]:
        results = []
        search_url = f"{self.base_url}/catalogsearch/result/?q={quote_plus(movie_title)}"

        try:
            response = self.session.get(search_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Diabolik DVD search failed: {e}")
            return results

        soup = BeautifulSoup(response.text, 'html.parser')

        # Magento-based - look for product items
        products = soup.select('.product-item, .item.product')

        for product in products[:15]:
            try:
                # Get title and link
                title_link = product.select_one('.product-item-link, .product-name a')
                if not title_link:
                    continue

                title = title_link.get_text(strip=True)
                url = title_link.get('href', '')
                if not url.startswith('http'):
                    url = urljoin(self.base_url, url)

                # Get price
                price_elem = product.select_one('.price, .regular-price')
                price = None
                if price_elem:
                    price = self._extract_price(price_elem.get_text())

                # Get thumbnail
                img = product.select_one('img.product-image-photo, .product-image img')
                thumbnail = None
                if img:
                    thumbnail = img.get('src', '') or img.get('data-src', '')

                # Detect edition type from title
                edition = self.edition_type
                title_lower = title.lower()
                if 'criterion' in title_lower:
                    edition = "Criterion Collection"
                elif 'arrow' in title_lower:
                    edition = "Arrow Video"
                elif 'shout' in title_lower or 'scream factory' in title_lower:
                    edition = "Scream Factory"
                elif 'kino' in title_lower:
                    edition = "Kino Lorber"
                elif 'vinegar' in title_lower:
                    edition = "Vinegar Syndrome"

                results.append(RetailerResult(
                    title=title,
                    price=price,
                    url=url,
                    retailer=self.name,
                    edition_type=edition,
                    thumbnail=thumbnail,
                ))
            except Exception as e:
                logger.debug(f"Error parsing Diabolik product: {e}")
                continue

        logger.info(f"Diabolik DVD: found {len(results)} results for '{movie_title}'")
        return results


class SerpAPISiteSearcher:
    """
    Uses SerpAPI to search specific retailer sites that block direct scraping.
    This uses Google web search with site: operator to find products.
    """

    # Sites that block direct scraping but can be found via Google
    PROTECTED_SITES = {
        "criterion.com": "Criterion Collection",
        "kinolorber.com": "Kino Lorber",
        "shoutfactory.com": "Shout Factory",
        "shop.bfi.org.uk": "BFI",
        "eurekavideo.co.uk": "Eureka/Masters of Cinema",
        "88films.co.uk": "88 Films",
    }

    def __init__(self, api_key: str):
        self.api_key = api_key

    def search(self, movie_title: str, year: Optional[int] = None, alternative_titles: Optional[List[str]] = None) -> List[RetailerResult]:
        """Search protected sites via SerpAPI Google search."""
        from serpapi import GoogleSearch

        results = []

        # Build site: query for all protected sites
        site_query = " OR ".join([f"site:{site}" for site in self.PROTECTED_SITES.keys()])

        # Build title query with alternatives (e.g., "Hausu" OR "House")
        all_titles = [movie_title]
        if alternative_titles:
            for alt in alternative_titles:
                if alt.lower() != movie_title.lower() and alt not in all_titles:
                    all_titles.append(alt)

        if len(all_titles) > 1:
            title_query = " OR ".join([f'"{t}"' for t in all_titles[:4]])  # Limit to 4 to avoid too long query
            title_query = f"({title_query})"
        else:
            title_query = f'"{movie_title}"'

        query = f'{title_query} blu-ray ({site_query})'

        if year:
            query = f'{title_query} {year} blu-ray ({site_query})'

        logger.debug(f"SerpAPI site search query: {query}")

        try:
            params = {
                "api_key": self.api_key,
                "engine": "google",
                "q": query,
                "num": 20,
            }
            search = GoogleSearch(params)
            data = search.get_dict()

            organic_results = data.get("organic_results", [])

            for item in organic_results:
                try:
                    title = item.get("title", "")
                    url = item.get("link", "")
                    snippet = item.get("snippet", "")

                    # Determine which retailer this is from
                    retailer = "Boutique Retailer"
                    edition_type = "Boutique Release"
                    for site, label in self.PROTECTED_SITES.items():
                        if site in url:
                            retailer = label
                            edition_type = label
                            break

                    # Try to extract price from snippet
                    price = None
                    price_match = re.search(r'\$(\d+\.?\d*)', snippet)
                    if price_match:
                        price = float(price_match.group(1))

                    # Get thumbnail if available
                    thumbnail = item.get("thumbnail", None)

                    results.append(RetailerResult(
                        title=title,
                        price=price,
                        url=url,
                        retailer=retailer,
                        edition_type=edition_type,
                        thumbnail=thumbnail,
                    ))
                except Exception as e:
                    logger.debug(f"Error parsing SerpAPI result: {e}")
                    continue

            logger.info(f"SerpAPI site search: found {len(results)} results for '{movie_title}'")

        except Exception as e:
            logger.error(f"SerpAPI site search failed: {e}")

        return results


class RetailerSearcher:
    """Searches across multiple boutique retailers."""

    def __init__(self, serpapi_key: Optional[str] = None):
        self.scrapers: List[RetailerScraper] = [
            VinegarSyndromeScraper(),
            ArrowVideoScraper(),
            SeverinFilmsScraper(),
            GrindHouseVideoScraper(),
        ]
        self.serpapi_key = serpapi_key
        self.site_searcher = SerpAPISiteSearcher(serpapi_key) if serpapi_key else None

    def _title_matches(self, product_title: str, search_title: str, alternative_titles: Optional[List[str]] = None) -> bool:
        """Check if the product title matches the search title or any alternative."""
        product_lower = product_title.lower()

        # Build list of all acceptable titles
        titles_to_check = [search_title]
        if alternative_titles:
            titles_to_check.extend(alternative_titles)

        for title in titles_to_check:
            title_lower = title.lower()
            title_words = title_lower.split()

            # Skip non-ASCII titles for matching (like Japanese characters)
            if not title.isascii():
                continue

            # For short/single-word titles, use stricter matching
            # to avoid "House" matching "House of Mortal Sin"
            if len(title_words) == 1 and len(title_lower) <= 10:
                # Check if title appears at start followed by delimiter or format keyword
                # "House [Hausu]" or "House (Criterion)" should match
                # "House of Mortal Sin" should not
                pattern = rf'^{re.escape(title_lower)}(\s*[\[\(\-:]|\s+blu|\s+4k|\s+dvd|\s*$)'
                if re.search(pattern, product_lower):
                    return True
                # Also check if it appears in brackets/parens (like "[Hausu]" or "(House)")
                pattern2 = rf'[\[\(]{re.escape(title_lower)}[\]\)]'
                if re.search(pattern2, product_lower):
                    return True
                continue

            # For multi-word titles, require all words to appear
            if len(title_words) > 1:
                if all(word in product_lower for word in title_words):
                    return True
            else:
                # Longer single words can use simple containment
                if title_lower in product_lower:
                    return True

        return False

    def search_all(
        self,
        movie_title: str,
        year: Optional[int] = None,
        max_price: Optional[float] = None,
        alternative_titles: Optional[List[str]] = None
    ) -> List[RetailerResult]:
        """
        Search all retailers for a movie.

        Args:
            movie_title: Movie title to search for
            year: Optional release year for filtering
            max_price: Optional maximum price filter
            alternative_titles: Optional list of alternative titles to accept in matching

        Returns:
            Combined list of results from all retailers
        """
        all_results = []

        # Search direct scrapers
        for scraper in self.scrapers:
            try:
                results = scraper.search(movie_title, year)
                all_results.extend(results)
            except Exception as e:
                logger.error(f"Error searching {scraper.name}: {e}")
                continue

        # Search protected sites via SerpAPI
        if self.site_searcher:
            try:
                site_results = self.site_searcher.search(movie_title, year, alternative_titles)
                all_results.extend(site_results)
            except Exception as e:
                logger.error(f"Error in SerpAPI site search: {e}")

        # Filter results that don't match the search title (or any alternative)
        # This removes featured/popular items that appear when search has no results
        all_results = [
            r for r in all_results
            if self._title_matches(r.title, movie_title, alternative_titles)
        ]
        logger.info(f"After title filtering: {len(all_results)} results match '{movie_title}' (or alternatives)")

        # Filter by price if specified
        if max_price is not None:
            all_results = [
                r for r in all_results
                if r.price is None or r.price <= max_price
            ]

        # Filter by year if specified (check if year appears in title)
        if year:
            filtered = []
            for r in all_results:
                # Check if wrong year is in the title
                years_in_title = re.findall(r'\b(19\d{2}|20\d{2})\b', r.title)
                if years_in_title:
                    # If years found, make sure at least one is close to expected
                    if any(abs(int(y) - year) <= 1 for y in years_in_title):
                        filtered.append(r)
                else:
                    # No year in title - include it
                    filtered.append(r)
            all_results = filtered

        logger.info(f"Total retailer results for '{movie_title}': {len(all_results)}")
        return all_results


# Convenience function
def search_boutique_retailers(
    movie_title: str,
    year: Optional[int] = None,
    max_price: Optional[float] = None,
    serpapi_key: Optional[str] = None,
    alternative_titles: Optional[List[str]] = None
) -> List[RetailerResult]:
    """Search all boutique retailers for a movie.

    Args:
        movie_title: Movie title to search for
        year: Optional release year for filtering
        max_price: Optional maximum price filter
        serpapi_key: Optional SerpAPI key for searching protected sites
        alternative_titles: Optional list of alternative titles to accept in matching

    Returns:
        List of RetailerResult objects
    """
    searcher = RetailerSearcher(serpapi_key=serpapi_key)
    return searcher.search_all(movie_title, year, max_price, alternative_titles)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = search_boutique_retailers("House", year=1977, max_price=100)
    print(f"\nFound {len(results)} results:\n")
    for r in results:
        print(f"  {r.retailer}: {r.title} - ${r.price} - {r.url}")
