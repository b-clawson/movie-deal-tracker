"""
TMDB (The Movie Database) API integration for movie search and metadata.
Used for movie suggestions with thumbnails in the search feature.
"""

import logging
from dataclasses import dataclass
from typing import List, Optional

import requests

logger = logging.getLogger(__name__)

TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p"


@dataclass
class TMDBMovie:
    """Movie data from TMDB."""
    id: int
    title: str
    year: Optional[int]
    overview: str
    poster_url: Optional[str]
    backdrop_url: Optional[str]
    popularity: float

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "title": self.title,
            "year": self.year,
            "overview": self.overview,
            "poster_url": self.poster_url,
            "backdrop_url": self.backdrop_url,
            "popularity": self.popularity,
        }


class TMDBService:
    """TMDB API client for movie search and metadata."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
        })
        # Use API key as query param (works with standard TMDB API key)
        self.default_params = {"api_key": api_key}

    def search_movies(
        self,
        query: str,
        year: Optional[int] = None,
        limit: int = 5
    ) -> List[TMDBMovie]:
        """
        Search for movies by title.

        Args:
            query: Movie title to search for
            year: Optional release year to filter results
            limit: Maximum number of results to return

        Returns:
            List of TMDBMovie objects
        """
        params = {
            **self.default_params,
            "query": query,
            "include_adult": "false",
            "language": "en-US",
            "page": 1,
        }

        if year:
            params["year"] = year

        try:
            response = self.session.get(
                f"{TMDB_BASE_URL}/search/movie",
                params=params,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            movies = []
            for result in data.get("results", [])[:limit]:
                movie = self._parse_movie(result)
                if movie:
                    movies.append(movie)

            logger.debug(f"TMDB search for '{query}': {len(movies)} results")
            return movies

        except requests.RequestException as e:
            logger.error(f"TMDB search failed: {e}")
            return []

    def get_movie(self, movie_id: int) -> Optional[TMDBMovie]:
        """
        Get movie details by TMDB ID.

        Args:
            movie_id: TMDB movie ID

        Returns:
            TMDBMovie object or None
        """
        try:
            response = self.session.get(
                f"{TMDB_BASE_URL}/movie/{movie_id}",
                params={**self.default_params, "language": "en-US"},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            return self._parse_movie(data)

        except requests.RequestException as e:
            logger.error(f"TMDB get movie failed: {e}")
            return None

    def _parse_movie(self, data: dict) -> Optional[TMDBMovie]:
        """Parse TMDB API response into TMDBMovie object."""
        try:
            # Extract year from release_date
            release_date = data.get("release_date", "")
            year = None
            if release_date and len(release_date) >= 4:
                try:
                    year = int(release_date[:4])
                except ValueError:
                    pass

            # Build image URLs
            poster_path = data.get("poster_path")
            backdrop_path = data.get("backdrop_path")

            poster_url = f"{TMDB_IMAGE_BASE}/w185{poster_path}" if poster_path else None
            backdrop_url = f"{TMDB_IMAGE_BASE}/w300{backdrop_path}" if backdrop_path else None

            return TMDBMovie(
                id=data.get("id"),
                title=data.get("title", ""),
                year=year,
                overview=data.get("overview", "")[:200],  # Truncate long overviews
                poster_url=poster_url,
                backdrop_url=backdrop_url,
                popularity=data.get("popularity", 0),
            )

        except Exception as e:
            logger.warning(f"Failed to parse TMDB movie: {e}")
            return None

    def get_popular_movies(self, limit: int = 10) -> List[TMDBMovie]:
        """Get currently popular movies for default suggestions."""
        try:
            response = self.session.get(
                f"{TMDB_BASE_URL}/movie/popular",
                params={**self.default_params, "language": "en-US", "page": 1},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            movies = []
            for result in data.get("results", [])[:limit]:
                movie = self._parse_movie(result)
                if movie:
                    movies.append(movie)

            return movies

        except requests.RequestException as e:
            logger.error(f"TMDB popular movies failed: {e}")
            return []
