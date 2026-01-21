"""
Deal finder using SerpAPI to search for physical movie editions.
Includes smart caching that respects sale periods.
"""

from __future__ import annotations

import re
import time
import logging
import hashlib
from typing import List, Optional, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass, asdict
from datetime import datetime

from serpapi import GoogleSearch

from .letterboxd_scraper import Movie
from .edition_classifier import EditionClassifier
from .database import get_db
from .sale_periods import get_cache_ttl_hours, is_sale_period
from .retailer_scrapers import search_boutique_retailers, RetailerResult

if TYPE_CHECKING:
    from .llm_service import OpenAIService

logger = logging.getLogger(__name__)


@dataclass
class Deal:
    """Represents a found deal."""

    movie_title: str
    product_title: str
    price: float
    retailer: str
    url: str
    similarity_score: float
    matched_example: str
    thumbnail: str = ""
    found_at: str = ""

    def __post_init__(self):
        if not self.found_at:
            self.found_at = datetime.now().isoformat()

    @property
    def deal_hash(self) -> str:
        """Generate unique hash for this deal."""
        key = f"{self.movie_title}|{self.product_title}|{self.retailer}"
        return hashlib.md5(key.encode()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DealFinder:
    """Searches for movie deals using SerpAPI."""

    def __init__(
        self,
        api_key: str,
        classifier: EditionClassifier,
        max_price: float = 20.0,
        requests_per_minute: int = 30,
        llm_service: Optional[OpenAIService] = None,
    ):
        self.api_key = api_key
        self.classifier = classifier
        self.max_price = max_price
        self.request_delay = 60.0 / requests_per_minute
        self.llm_service = llm_service

    def search_movie(self, movie: Movie, skip_cache: bool = False) -> List[Deal]:
        """Search for deals on a specific movie.

        Args:
            movie: Movie to search for
            skip_cache: If True, bypass cache and always do fresh search

        Returns:
            List of Deal objects found
        """
        db = get_db()

        # Check sale period status
        is_sale, sale_name = is_sale_period()
        cache_ttl = get_cache_ttl_hours()

        if is_sale:
            logger.info(f"Sale period active ({sale_name}) - using fresh searches")

        # Try cache first (unless skipping or during sale period)
        if not skip_cache and cache_ttl > 0:
            cached = db.get_cached_results(movie.title, self.max_price)
            if cached is not None:
                logger.info(f"Cache hit for '{movie.title}' - returning {len(cached)} cached deals")
                # Convert cached dicts back to Deal objects
                return [Deal(**deal_dict) for deal_dict in cached]

        # Cache miss or skipping cache - do fresh search
        deals = []

        # 1. Search Google Shopping via SerpAPI
        # Use LLM-expanded queries if available, otherwise use single templated query
        queries = self._get_search_queries(movie)
        query = queries[0]  # Keep track of primary query for refinement fallback

        for i, q in enumerate(queries):
            logger.info(f"Searching Google Shopping ({i+1}/{len(queries)}): {q}")
            try:
                results = self._execute_search(q)
                shopping_deals = self._process_results(movie, results)
                deals.extend(shopping_deals)
                logger.info(f"Google Shopping query {i+1}: found {len(shopping_deals)} deals")

                # Rate limiting between queries
                if i < len(queries) - 1:
                    time.sleep(self.request_delay)
            except Exception as e:
                logger.error(f"Google Shopping search failed for {movie.title} (query {i+1}): {e}")

        # 2. Search boutique retailer sites directly
        # Use same search title as Google Shopping for consistency
        search_title = movie.get_search_title()

        # Build list of all acceptable titles for filtering (original + alternatives)
        all_titles = [movie.title]
        if movie.alternative_titles:
            all_titles.extend(movie.alternative_titles)
        # Add search_title if different from original
        if search_title != movie.title and search_title not in all_titles:
            all_titles.append(search_title)

        try:
            retailer_results = search_boutique_retailers(
                movie_title=search_title,
                year=movie.year,
                max_price=self.max_price,
                serpapi_key=self.api_key,
                alternative_titles=all_titles,
                llm_service=self.llm_service,
                director=movie.director,
            )
            retailer_deals = self._convert_retailer_results(movie, retailer_results)
            deals.extend(retailer_deals)
            logger.info(f"Boutique retailers: found {len(retailer_deals)} deals")
        except Exception as e:
            logger.error(f"Boutique retailer search failed for {movie.title}: {e}")

        # Deduplicate by URL
        seen_urls = set()
        unique_deals = []
        for deal in deals:
            if deal.url not in seen_urls:
                seen_urls.add(deal.url)
                unique_deals.append(deal)
        deals = unique_deals

        # If few results and LLM service available, try refined searches
        if len(deals) < 2 and self.llm_service:
            logger.info(f"Few results ({len(deals)}) for '{movie.title}', trying LLM search refinement")
            refined_deals = self._refine_search_with_llm(movie, deals, query)
            # Deduplicate refined results
            for deal in refined_deals:
                if deal.url not in seen_urls:
                    seen_urls.add(deal.url)
                    deals.append(deal)
            if refined_deals:
                logger.info(f"LLM refinement added {len(refined_deals)} potential deals for '{movie.title}'")

        # Post-search batch validation - verify all results are for the correct movie
        if deals and self.llm_service:
            deals = self._batch_validate_results(movie, deals)

        # Cache the results (if caching is enabled)
        if cache_ttl > 0 and deals:
            deal_dicts = [deal.to_dict() for deal in deals]
            db.set_cached_results(movie.title, self.max_price, deal_dicts, cache_ttl)
            logger.info(f"Cached {len(deals)} deals for '{movie.title}' (TTL: {cache_ttl}h)")
        elif cache_ttl == 0:
            logger.info(f"Skipping cache during sale period")

        # Rate limiting (only needed for actual API calls)
        time.sleep(self.request_delay)

        return deals

    def _convert_retailer_results(self, movie: Movie, results: List[RetailerResult]) -> List[Deal]:
        """Convert RetailerResult objects to Deal objects."""
        deals = []
        for r in results:
            # For results without price, include them but mark price as 0
            # (better to show the listing than miss it entirely)
            price = r.price if r.price is not None else 0.0

            # Skip if price exceeds max (but allow price=0 which means unknown)
            if price > 0 and price > self.max_price:
                continue

            deals.append(Deal(
                movie_title=movie.title,
                product_title=r.title,
                price=price,
                retailer=r.retailer,
                url=r.url,
                similarity_score=0.9,  # High confidence for direct retailer results
                matched_example=r.edition_type,
                thumbnail=r.thumbnail or "",
            ))
        return deals

    def _build_query(self, movie: Movie) -> str:
        """Build search query for a movie.

        Uses best available title for search (may use alternative title for generic names).
        Year and director are used for result validation rather than in the query
        since they often don't appear in product titles.
        """
        # Use get_search_title() which prefers alternative titles for generic names
        # e.g., "Hausu" instead of "House" for the 1977 Japanese film
        search_title = movie.get_search_title()
        # Query requires the title AND (blu-ray or 4K) AND a boutique label
        # Using proper grouping to ensure title is always required
        return f'"{search_title}" (blu-ray OR 4K) (criterion OR arrow OR "shout factory" OR "vinegar syndrome" OR kino)'

    def _get_search_queries(self, movie: Movie) -> List[str]:
        """Get search queries for a movie, using LLM expansion if available.

        Returns multiple queries when LLM service is configured, otherwise
        returns a single templated query. Also includes bundle/collection queries.
        """
        # Always include the default templated query as fallback
        default_query = self._build_query(movie)

        if not self.llm_service:
            return [default_query]

        queries = []

        # 1. Get expanded title queries
        try:
            expansion = self.llm_service.generate_search_queries(
                movie_title=movie.title,
                year=movie.year,
                director=movie.director,
                alternative_titles=movie.alternative_titles,
            )

            if expansion.queries:
                logger.info(f"LLM query expansion for '{movie.title}': {len(expansion.queries)} queries")
                logger.debug(f"Expansion reasoning: {expansion.reasoning}")
                queries.extend(expansion.queries[:3])  # Limit to 3 title queries

        except Exception as e:
            logger.warning(f"LLM query expansion failed for '{movie.title}': {e}")
            queries.append(default_query)

        # 2. Get bundle/collection queries
        try:
            bundles = self.llm_service.detect_bundles(
                movie_title=movie.title,
                year=movie.year,
                director=movie.director,
            )

            if bundles.bundle_queries:
                logger.info(f"Bundle detection for '{movie.title}': {len(bundles.bundle_queries)} bundles")
                logger.debug(f"Bundle reasoning: {bundles.reasoning}")
                queries.extend(bundles.bundle_queries[:2])  # Limit to 2 bundle queries

        except Exception as e:
            logger.warning(f"LLM bundle detection failed for '{movie.title}': {e}")

        # Ensure we have at least one query
        if not queries:
            queries.append(default_query)

        # Limit total queries to control API costs
        return queries[:5]

    def _execute_search(self, query: str) -> Dict[str, Any]:
        """Execute SerpAPI Google Shopping search."""
        params = {
            "api_key": self.api_key,
            "engine": "google_shopping",
            "q": query,
            "gl": "us",
            "hl": "en",
            "num": 20,
        }

        search = GoogleSearch(params)
        return search.get_dict()

    def _process_results(self, movie: Movie, results: Dict) -> List[Deal]:
        """Process search results and filter deals."""
        deals = []

        shopping_results = results.get("shopping_results", [])
        logger.info(f"Found {len(shopping_results)} shopping results")

        for item in shopping_results:
            deal = self._process_item(movie, item)
            if deal:
                deals.append(deal)

        return deals

    def _process_item(self, movie: Movie, item: Dict) -> Optional[Deal]:
        """Process a single shopping result."""
        title = item.get("title", "")
        price_str = item.get("price", "")
        source = item.get("source", "Unknown")
        thumbnail = item.get("thumbnail", "")

        # Get the best available link (product_link preferred, fall back to link)
        link = item.get("product_link") or item.get("link", "")

        # Extract price
        price = self._extract_price(price_str)
        if price is None:
            logger.debug(f"Could not extract price from: {price_str}")
            return None

        # Check price threshold
        if price > self.max_price:
            logger.debug(f"Price ${price:.2f} exceeds max ${self.max_price:.2f}")
            return None

        # Validate year if we have one - helps filter out wrong movies with similar titles
        if movie.year and not self._validate_year(title, movie.year):
            logger.debug(f"Year mismatch for '{movie.title}' ({movie.year}): {title}")
            return None

        # LLM title validation for ambiguous cases
        if self.llm_service and self._is_ambiguous_title(movie, title):
            if not self._validate_title_with_llm(movie, title):
                logger.debug(f"LLM rejected title match for '{movie.title}': {title}")
                return None

        # Check if it's a special edition
        is_match, confidence, description = self.classifier.is_special_edition(title)
        if not is_match:
            return None

        return Deal(
            movie_title=movie.title,
            product_title=title,
            price=price,
            retailer=source,
            url=link,
            similarity_score=confidence,
            matched_example=description,
            thumbnail=thumbnail,
        )

    def _is_ambiguous_title(self, movie: Movie, product_title: str) -> bool:
        """Check if the movie title is ambiguous and needs LLM validation.

        Returns True for:
        - Short/generic titles (5 chars or less, common words)
        - Product listings without a year (uncertain which version)
        - Known problematic titles with remakes
        """
        title_lower = movie.title.lower()

        # Generic single words that have multiple films
        generic_words = {
            'house', 'ring', 'pulse', 'cure', 'it', 'us', 'them', 'mother',
            'father', 'home', 'dark', 'gate', 'host', 'thing', 'alien', 'ghost',
            'crash', 'heat', 'speed', 'dressed', 'psycho', 'carrie', 'suspiria'
        }

        # Short titles are often ambiguous
        if len(movie.title) <= 5:
            return True

        # Known generic words
        if title_lower in generic_words:
            return True

        # Product has no year - uncertain which version
        years_in_product = re.findall(r'\b(19\d{2}|20\d{2})\b', product_title)
        if not years_in_product and movie.year:
            return True

        return False

    def _validate_title_with_llm(self, movie: Movie, product_title: str) -> bool:
        """Use LLM to validate that the product is for the correct movie."""
        try:
            result = self.llm_service.validate_movie_match(
                product_title=product_title,
                movie_title=movie.title,
                year=movie.year,
                director=movie.director,
                alternative_titles=movie.alternative_titles,
            )

            # Accept matches with reasonable confidence
            if result.is_match and result.confidence >= 0.6:
                logger.debug(f"LLM validated match: {result.reason}")
                return True

            logger.info(f"LLM rejected match ({result.confidence:.0%}): {result.reason}")
            return False

        except Exception as e:
            logger.warning(f"LLM title validation failed: {e}")
            # On error, give benefit of doubt
            return True

    def _batch_validate_results(self, movie: Movie, deals: List[Deal], max_to_validate: int = 10) -> List[Deal]:
        """
        Batch validate search results using LLM.

        Only validates the top N results for performance.
        Filters out results that don't match the target movie.
        """
        if not deals or not self.llm_service:
            return deals

        try:
            # Only validate top N results for speed
            deals_to_validate = deals[:max_to_validate]
            product_titles = [deal.product_title for deal in deals_to_validate]

            result = self.llm_service.batch_validate_results(
                movie_title=movie.title,
                year=movie.year,
                director=movie.director,
                product_titles=product_titles
            )

            # Filter to only valid results from validated set
            valid_deals = [
                deals_to_validate[i] for i in result.valid_indices
                if i < len(deals_to_validate)
            ]

            removed_count = len(deals_to_validate) - len(valid_deals)
            if removed_count > 0:
                logger.info(
                    f"Batch validation removed {removed_count} results for '{movie.title}': "
                    f"{result.reasoning}"
                )

            return valid_deals

        except Exception as e:
            logger.warning(f"Batch validation failed: {e}")
            # On error, return all deals (benefit of doubt)
            return deals

    def _validate_year(self, product_title: str, expected_year: int) -> bool:
        """
        Validate that the product is likely for the correct movie year.

        Returns True if:
        - The product title contains the expected year
        - The product title contains no year at all (benefit of the doubt)
        - The expected year is within 1 year of any year found (for release date variations)

        Returns False if:
        - The product title contains a different year (likely wrong movie)
        """
        # Find all 4-digit years in the product title
        years_found = re.findall(r'\b(19\d{2}|20\d{2})\b', product_title)

        if not years_found:
            # No year in title - give benefit of the doubt
            return True

        # Check if any found year is close to expected (within 1 year for release variations)
        for year_str in years_found:
            found_year = int(year_str)
            if abs(found_year - expected_year) <= 1:
                return True

        # Found years but none match - likely wrong movie
        return False

    def _extract_price(self, price_str: str) -> Optional[float]:
        """Extract numeric price from string."""
        if not price_str:
            return None

        # Handle various formats: "$19.99", "From $15.00", "$10 - $20"
        # Extract all numbers
        matches = re.findall(r"\d+\.?\d*", price_str)
        if not matches:
            return None

        # Use the lowest price found
        prices = [float(m) for m in matches]
        return min(prices)

    def _refine_search_with_llm(
        self,
        movie: Movie,
        current_deals: List[Deal],
        original_query: str
    ) -> List[Deal]:
        """Use LLM to suggest alternative search queries when results are sparse."""
        if not self.llm_service:
            return []

        try:
            suggestions = self.llm_service.suggest_search_refinements(
                movie_title=movie.title,
                year=movie.year,
                current_results=len(current_deals),
                original_query=original_query
            )

            additional_deals = []
            # Limit to 2 refinement queries to control API costs
            for alt_query in suggestions.alternative_queries[:2]:
                logger.info(f"Trying refined search: {alt_query}")
                try:
                    results = self._execute_search(alt_query)
                    deals = self._process_results(movie, results)
                    additional_deals.extend(deals)
                    logger.info(f"Refined search found {len(deals)} deals")

                    # Rate limiting between refinement searches
                    time.sleep(self.request_delay)
                except Exception as e:
                    logger.warning(f"Refined search failed for query '{alt_query}': {e}")

            return additional_deals

        except Exception as e:
            logger.warning(f"LLM search refinement failed: {e}")
            return []  # Graceful degradation

    def find_deals(self, movies: List[Movie], skip_cache: bool = False) -> List[Deal]:
        """Search for deals across all movies.

        Args:
            movies: List of movies to search for
            skip_cache: If True, bypass cache for all searches

        Returns:
            List of all Deal objects found
        """
        all_deals = []
        total = len(movies)

        # Log cache status at start
        is_sale, sale_name = is_sale_period()
        cache_ttl = get_cache_ttl_hours()
        if is_sale:
            logger.info(f"Sale period: {sale_name} - caching disabled")
        else:
            logger.info(f"Normal period - cache TTL: {cache_ttl}h")

        for i, movie in enumerate(movies, 1):
            logger.info(f"Processing {i}/{total}: {movie.title}")
            deals = self.search_movie(movie, skip_cache=skip_cache)
            all_deals.extend(deals)
            logger.info(f"Found {len(deals)} deals for {movie.title}")

        logger.info(f"Total deals found: {len(all_deals)}")
        return all_deals


if __name__ == "__main__":
    # Test with mock data
    logging.basicConfig(level=logging.INFO)
    print("Deal finder module loaded. Run via main.py for full functionality.")
