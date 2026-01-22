#!/usr/bin/env python3
"""
Movie Deal Tracker - Web Application

A simple Flask app that allows users to subscribe to deal notifications
by entering their Letterboxd list URL and email address.
"""

import os
import re
import logging
from pathlib import Path

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from dotenv import load_dotenv

# Import deal finding components
from src.letterboxd_scraper import Movie, LetterboxdScraper
from src.edition_classifier import EditionClassifier
from src.deal_finder import DealFinder
from src.notifier import EmailNotifier

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")

# Import database after app setup
from src.database import get_db


def is_valid_letterboxd_url(url: str) -> bool:
    """Validate that a URL is a valid Letterboxd list URL."""
    pattern = r"^https?://letterboxd\.com/[\w_-]+/list/[\w_-]+/?$"
    return bool(re.match(pattern, url, re.IGNORECASE))


def is_valid_email(email: str) -> bool:
    """Basic email validation."""
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


@app.route("/")
def index():
    """Landing page with subscription form."""
    db = get_db()
    subscriber_count = db.get_subscriber_count()
    return render_template("index.html", subscriber_count=subscriber_count)


@app.route("/subscribe", methods=["POST"])
def subscribe():
    """Handle new subscription."""
    email = request.form.get("email", "").strip().lower()
    list_url = request.form.get("list_url", "").strip()
    max_price = request.form.get("max_price", "20")
    check_frequency = request.form.get("check_frequency", "daily")

    # Parse and validate max_price
    try:
        max_price = float(max_price)
        if max_price < 5:
            max_price = 5
        elif max_price > 100:
            max_price = 100
    except ValueError:
        max_price = 20.0

    # Validate check_frequency
    if check_frequency not in ["daily", "weekly", "monthly"]:
        check_frequency = "daily"

    # Validate inputs
    errors = []

    if not email:
        errors.append("Email is required")
    elif not is_valid_email(email):
        errors.append("Please enter a valid email address")

    if not list_url:
        errors.append("Letterboxd list URL is required")
    elif not is_valid_letterboxd_url(list_url):
        errors.append("Please enter a valid Letterboxd list URL (e.g., https://letterboxd.com/username/list/list-name/)")

    if errors:
        for error in errors:
            flash(error, "error")
        return redirect(url_for("index"))

    # Add subscriber to database
    db = get_db()
    subscriber = db.add_subscriber(email, list_url, max_price, check_frequency)

    if subscriber:
        logger.info(f"New subscriber: {email} -> {list_url} (max: ${max_price}, freq: {check_frequency})")

        # Add to Resend audience if configured
        audience_id = os.getenv("RESEND_AUDIENCE_ID")
        resend_api_key = os.getenv("RESEND_API_KEY")
        if audience_id and resend_api_key:
            notifier = EmailNotifier(api_key=resend_api_key, from_email="")
            notifier.add_to_audience(email, audience_id)

        return render_template("success.html", email=email)
    else:
        flash("Something went wrong. Please try again.", "error")
        return redirect(url_for("index"))


@app.route("/unsubscribe/<token>")
def unsubscribe(token: str):
    """Handle unsubscribe request."""
    db = get_db()

    # Get subscriber info before unsubscribing
    subscriber = db.get_subscriber_by_token(token)

    if subscriber:
        success = db.unsubscribe(token)
        if success:
            logger.info(f"Unsubscribed: {subscriber.email}")
            return render_template("unsubscribed.html", email=subscriber.email)

    # Invalid or expired token
    return render_template("unsubscribed.html", email=None, error=True)


@app.route("/search", methods=["GET", "POST"])
def search():
    """On-demand movie search page."""
    deals = []
    movie_title = ""
    max_price = 100.0
    error = None
    searched = False

    if request.method == "POST":
        movie_title = request.form.get("movie_title", "").strip()
        max_price_str = request.form.get("max_price", "100")
        search_title_override = request.form.get("search_title", "").strip()
        letterboxd_url = request.form.get("letterboxd_url", "").strip()
        searched = True

        # Parse max_price
        try:
            max_price = float(max_price_str)
            if max_price < 5:
                max_price = 5
            elif max_price > 200:
                max_price = 200
        except ValueError:
            max_price = 100.0

        if not movie_title:
            error = "Please enter a movie title or Letterboxd URL"
        else:
            # Check for SerpAPI key
            serpapi_key = os.getenv("SERPAPI_KEY")
            if not serpapi_key:
                error = "Search is temporarily unavailable"
                logger.error("SERPAPI_KEY not configured")
            else:
                try:
                    # Check if input is a Letterboxd film URL
                    letterboxd_match = re.match(r'^https?://letterboxd\.com/film/([^/]+)/?$', movie_title)

                    if letterboxd_match:
                        # Fetch movie details from Letterboxd (including alternative titles)
                        scraper = LetterboxdScraper()
                        movie = Movie(title="", letterboxd_url=movie_title)
                        scraper.fetch_movie_details(movie)

                        # Extract title from URL slug if fetch failed
                        if not movie.title:
                            slug = letterboxd_match.group(1)
                            movie.title = slug.replace("-", " ").title()

                        # Use user-selected search title if provided, otherwise use auto-detected
                        if search_title_override:
                            # Override the title for searching (user selected from alternatives)
                            movie.alternative_titles = None  # Clear to prevent auto-selection
                            original_title = movie.title
                            movie.title = search_title_override
                            logger.info(f"Letterboxd lookup: {original_title} ({movie.year}) - user selected '{search_title_override}'")
                        else:
                            # Log search title being used
                            search_title = movie.get_search_title()
                            if search_title != movie.title:
                                logger.info(f"Letterboxd lookup: {movie} (searching as '{search_title}')")
                            else:
                                logger.info(f"Letterboxd lookup: {movie}")
                    else:
                        # Parse year from title if provided (e.g., "The Thing (1982)" or "The Thing 1982")
                        title = movie_title
                        year = None
                        year_match = re.search(r'\s*\(?(\d{4})\)?$', movie_title)
                        if year_match:
                            year = int(year_match.group(1))
                            title = movie_title[:year_match.start()].strip()

                        # Try to find movie on Letterboxd for alternative titles
                        movie = None
                        try:
                            scraper = LetterboxdScraper()
                            movie = scraper.search_movie_by_title(title, year)
                        except Exception as letterboxd_err:
                            # Playwright fails in async environments (e.g., Railway)
                            logger.warning(f"Letterboxd lookup skipped: {letterboxd_err}")

                        if movie:
                            search_title = movie.get_search_title()
                            if search_title != movie.title:
                                logger.info(f"Letterboxd lookup: {movie.title} ({movie.year}) -> searching as '{search_title}'")
                        else:
                            # Fallback to basic Movie object if Letterboxd search fails
                            # Static mapping in get_search_title() will still provide alternatives
                            movie = Movie(title=title, year=year)
                            search_title = movie.get_search_title()
                            if search_title != movie.title:
                                logger.info(f"Using static mapping: {title} ({year}) -> searching as '{search_title}'")
                            else:
                                logger.info(f"Using basic search for '{title}' ({year})")

                    # Initialize classifier and deal finder
                    classifier = EditionClassifier()

                    # Initialize LLM service for batch validation (optional)
                    llm_service = None
                    openai_key = os.getenv("OPENAI_API_KEY")
                    if openai_key:
                        from src.llm_service import OpenAIService
                        llm_service = OpenAIService(api_key=openai_key)

                    finder = DealFinder(
                        api_key=serpapi_key,
                        classifier=classifier,
                        max_price=max_price,
                        requests_per_minute=30,
                        llm_service=llm_service,
                    )

                    # Search for deals (skip cache for on-demand searches so users get fresh results)
                    deals = finder.search_movie(movie, skip_cache=True)
                    search_desc = str(movie)
                    logger.info(f"Search for {search_desc} (max ${max_price}) found {len(deals)} deals")

                except Exception as e:
                    import traceback
                    logger.error(f"Search failed: {e}\n{traceback.format_exc()}")
                    error = "Search failed. Please try again."

    return render_template(
        "search.html",
        deals=deals,
        movie_title=movie_title,
        max_price=max_price,
        error=error,
        searched=searched,
    )


@app.route("/health")
def health():
    """Health check endpoint for deployment platforms."""
    return {"status": "ok"}


@app.route("/api/movie-details", methods=["POST"])
def get_movie_details():
    """Fetch movie details from a Letterboxd URL, including alternative titles."""
    data = request.get_json() or {}
    url = data.get("url", "").strip()

    if not url:
        return {"error": "URL is required"}, 400

    # Validate it's a Letterboxd film URL
    letterboxd_match = re.match(r'^https?://letterboxd\.com/film/([^/]+)/?$', url)
    if not letterboxd_match:
        return {"error": "Invalid Letterboxd film URL"}, 400

    try:
        scraper = LetterboxdScraper()
        movie = Movie(title="", letterboxd_url=url)
        scraper.fetch_movie_details(movie)

        if not movie.title:
            slug = letterboxd_match.group(1)
            movie.title = slug.replace("-", " ").title()

        # Build list of title options (original + romanized alternatives)
        title_options = [{"value": movie.title, "label": movie.title, "recommended": False}]

        if movie.alternative_titles:
            for alt in movie.alternative_titles:
                # Only include ASCII-friendly alternatives (good for searching)
                if alt.isascii() and len(alt) >= 3 and alt.lower() != movie.title.lower():
                    # Check if this would be the recommended search title
                    is_recommended = (alt == movie.get_search_title() and alt != movie.title)
                    title_options.append({
                        "value": alt,
                        "label": alt,
                        "recommended": is_recommended
                    })

        return {
            "title": movie.title,
            "year": movie.year,
            "director": movie.director,
            "alternative_titles": movie.alternative_titles,
            "title_options": title_options,
        }

    except Exception as e:
        logger.error(f"Failed to fetch movie details: {e}")
        return {"error": "Failed to fetch movie details"}, 500


@app.route("/api/movie-suggestions", methods=["POST"])
def get_movie_suggestions():
    """
    Get movie suggestions based on partial/ambiguous user input.
    Uses LLM to interpret input and TMDB to fetch movie details with thumbnails.
    """
    data = request.get_json() or {}
    query = data.get("query", "").strip()

    if not query or len(query) < 2:
        return {"suggestions": []}, 200

    suggestions = []

    # Try LLM-powered suggestions if available
    openai_key = os.getenv("OPENAI_API_KEY")
    tmdb_key = os.getenv("TMDB_API_KEY")

    if openai_key and tmdb_key:
        try:
            from src.llm_service import OpenAIService
            from src.tmdb_service import TMDBService

            llm = OpenAIService(api_key=openai_key)
            tmdb = TMDBService(api_key=tmdb_key)

            # Get LLM movie suggestions
            llm_result = llm.suggest_movies(query)

            # Enrich with TMDB data (thumbnails)
            for suggestion in llm_result.suggestions[:5]:
                tmdb_results = tmdb.search_movies(
                    suggestion.title,
                    year=suggestion.year,
                    limit=1
                )

                if tmdb_results:
                    movie = tmdb_results[0]
                    suggestions.append({
                        "title": movie.title,
                        "year": movie.year,
                        "poster_url": movie.poster_url,
                        "overview": movie.overview[:100] + "..." if len(movie.overview) > 100 else movie.overview,
                        "reason": suggestion.reason,
                        "tmdb_id": movie.id,
                    })
                else:
                    # Include LLM suggestion even without TMDB match
                    suggestions.append({
                        "title": suggestion.title,
                        "year": suggestion.year,
                        "poster_url": None,
                        "overview": "",
                        "reason": suggestion.reason,
                        "tmdb_id": None,
                    })

            return {
                "suggestions": suggestions,
                "interpreted": llm_result.interpreted_query,
            }, 200

        except Exception as e:
            logger.warning(f"LLM movie suggestions failed: {e}")
            # Fall through to TMDB-only search

    # Fallback: TMDB-only search (no LLM)
    if tmdb_key:
        try:
            from src.tmdb_service import TMDBService
            tmdb = TMDBService(api_key=tmdb_key)

            tmdb_results = tmdb.search_movies(query, limit=5)
            for movie in tmdb_results:
                suggestions.append({
                    "title": movie.title,
                    "year": movie.year,
                    "poster_url": movie.poster_url,
                    "overview": movie.overview[:100] + "..." if len(movie.overview) > 100 else movie.overview,
                    "reason": "",
                    "tmdb_id": movie.id,
                })

            return {"suggestions": suggestions}, 200

        except Exception as e:
            logger.warning(f"TMDB search failed: {e}")

    # No suggestions available
    return {"suggestions": []}, 200


@app.route("/admin/cache-status")
def cache_status():
    """Get cache status and sale period info."""
    from src.sale_periods import get_cache_status

    # Verify admin key
    admin_key = os.getenv("ADMIN_KEY", "")
    provided_key = request.args.get("key", "") or request.headers.get("X-Admin-Key", "")

    if not admin_key or provided_key != admin_key:
        return {"error": "Unauthorized"}, 401

    db = get_db()
    cache_stats = db.get_cache_stats()
    sale_status = get_cache_status()

    return {
        "cache": cache_stats,
        "sale_period": sale_status,
    }


@app.route("/admin/debug-search", methods=["GET"])
def debug_search():
    """Debug search to see raw results before filtering."""
    from src.edition_classifier import EditionClassifier
    from src.deal_finder import DealFinder
    from src.retailer_scrapers import search_boutique_retailers

    # Verify admin key
    admin_key = os.getenv("ADMIN_KEY", "")
    provided_key = request.args.get("key", "") or request.headers.get("X-Admin-Key", "")

    if not admin_key or provided_key != admin_key:
        return {"error": "Unauthorized"}, 401

    movie_title = request.args.get("title", "House")
    movie_year = request.args.get("year", "1977")
    max_price = float(request.args.get("max_price", "100"))

    try:
        year = int(movie_year) if movie_year else None
    except:
        year = None

    serpapi_key = os.getenv("SERPAPI_KEY")
    if not serpapi_key:
        return {"error": "SERPAPI_KEY not configured"}, 500

    # Check if input is a Letterboxd URL for the debug endpoint
    letterboxd_match = re.match(r'^https?://letterboxd\.com/film/([^/]+)/?$', movie_title)
    if letterboxd_match:
        scraper = LetterboxdScraper()
        movie = Movie(title="", letterboxd_url=movie_title)
        scraper.fetch_movie_details(movie)
        if not movie.title:
            slug = letterboxd_match.group(1)
            movie.title = slug.replace("-", " ").title()
    else:
        # Try to find movie on Letterboxd for alternative titles
        scraper = LetterboxdScraper()
        movie = scraper.search_movie_by_title(movie_title, year)
        if not movie:
            movie = Movie(title=movie_title, year=year)

    classifier = EditionClassifier()

    # Initialize LLM service for batch validation (optional)
    llm_service = None
    openai_key = os.getenv("OPENAI_API_KEY")
    if openai_key:
        from src.llm_service import OpenAIService
        llm_service = OpenAIService(api_key=openai_key)

    finder = DealFinder(
        api_key=serpapi_key,
        classifier=classifier,
        max_price=max_price,
        requests_per_minute=30,
        llm_service=llm_service,
    )

    # Get the search title (may use alternative title for generic names)
    search_title = movie.get_search_title()
    query = finder._build_query(movie)

    # 1. Google Shopping results
    shopping_analysis = []
    try:
        raw_results = finder._execute_search(query)
        shopping_results = raw_results.get("shopping_results", [])

        for item in shopping_results[:10]:
            title = item.get("title", "")
            price_str = item.get("price", "")
            source = item.get("source", "")
            price = finder._extract_price(price_str)
            is_special, confidence, edition_type = classifier.is_special_edition(title)
            year_valid = True
            if year:
                year_valid = finder._validate_year(title, year)

            shopping_analysis.append({
                "title": title,
                "price": price,
                "source": source,
                "is_special_edition": is_special,
                "edition_type": edition_type,
                "year_valid": year_valid,
                "would_include": is_special and year_valid and price is not None and price <= max_price
            })
    except Exception as e:
        shopping_analysis = [{"error": str(e)}]

    # 2. Boutique retailer results (use search_title for better specificity)
    # Build list of all acceptable titles for filtering
    all_titles = [movie.title]
    if movie.alternative_titles:
        all_titles.extend(movie.alternative_titles)
    if search_title != movie.title and search_title not in all_titles:
        all_titles.append(search_title)

    retailer_analysis = []
    try:
        retailer_results = search_boutique_retailers(
            movie_title=search_title,
            year=movie.year,
            max_price=max_price,
            serpapi_key=serpapi_key,
            alternative_titles=all_titles,
        )
        for r in retailer_results[:15]:
            retailer_analysis.append({
                "title": r.title,
                "price": r.price,
                "retailer": r.retailer,
                "edition_type": r.edition_type,
                "url": r.url,
                "would_include": r.price is not None and r.price <= max_price
            })
    except Exception as e:
        retailer_analysis = [{"error": str(e)}]

    return {
        "query": query,
        "movie": {
            "title": movie.title,
            "year": movie.year,
            "search_title": search_title,
            "alternative_titles": movie.alternative_titles,
            "all_titles_for_matching": all_titles,
        },
        "max_price": max_price,
        "google_shopping": {
            "total_results": len(shopping_analysis),
            "analysis": shopping_analysis
        },
        "boutique_retailers": {
            "total_results": len(retailer_analysis),
            "analysis": retailer_analysis
        }
    }


@app.route("/admin/clear-cache", methods=["POST"])
def clear_cache():
    """Clear the search cache."""
    # Verify admin key
    admin_key = os.getenv("ADMIN_KEY", "")
    provided_key = request.args.get("key", "") or request.headers.get("X-Admin-Key", "")

    if not admin_key or provided_key != admin_key:
        return {"error": "Unauthorized"}, 401

    db = get_db()
    deleted = db.clear_all_cache()

    logger.info(f"Cache cleared via admin endpoint ({deleted} entries)")
    return {"status": "ok", "entries_cleared": deleted}


@app.route("/admin/subscribers", methods=["GET"])
def admin_list_subscribers():
    """List all subscribers with their status."""
    admin_key = os.getenv("ADMIN_KEY", "")
    provided_key = request.args.get("key", "") or request.headers.get("X-Admin-Key", "")

    if not admin_key or provided_key != admin_key:
        return {"error": "Unauthorized"}, 401

    db = get_db()
    subscribers = db.get_all_subscribers()

    result = []
    for sub in subscribers:
        notified_count = db.get_notified_deals_count(sub.id)
        result.append({
            "id": sub.id,
            "email": sub.email,
            "list_url": sub.list_url,
            "created_at": sub.created_at,
            "last_checked": sub.last_checked,
            "active": sub.active,
            "max_price": sub.max_price,
            "check_frequency": sub.check_frequency,
            "notified_deals_count": notified_count,
        })

    return {"subscribers": result, "total": len(result)}


@app.route("/admin/sync-audience", methods=["POST"])
def admin_sync_audience():
    """Sync all active subscribers to Resend audience."""
    admin_key = os.getenv("ADMIN_KEY", "")
    provided_key = request.args.get("key", "") or request.headers.get("X-Admin-Key", "")

    if not admin_key or provided_key != admin_key:
        return {"error": "Unauthorized"}, 401

    audience_id = os.getenv("RESEND_AUDIENCE_ID")
    resend_api_key = os.getenv("RESEND_API_KEY")

    if not audience_id or not resend_api_key:
        return {"error": "RESEND_AUDIENCE_ID or RESEND_API_KEY not configured"}, 500

    db = get_db()
    subscribers = db.get_active_subscribers()

    notifier = EmailNotifier(api_key=resend_api_key, from_email="")

    synced = 0
    failed = 0
    for sub in subscribers:
        if notifier.add_to_audience(sub.email, audience_id):
            synced += 1
        else:
            failed += 1

    logger.info(f"Audience sync complete: {synced} synced, {failed} failed")
    return {"status": "ok", "synced": synced, "failed": failed, "total": len(subscribers)}


@app.route("/admin/run-check", methods=["POST"])
def admin_run_check():
    """Manually trigger a deal check for all subscribers (runs in background)."""
    import threading

    # Verify admin key
    admin_key = os.getenv("ADMIN_KEY", "")
    provided_key = request.args.get("key", "") or request.headers.get("X-Admin-Key", "")

    if not admin_key or provided_key != admin_key:
        return {"error": "Unauthorized"}, 401

    # Check for force flag to bypass frequency check
    force = request.args.get("force", "").lower() == "true"
    # Check for resend flag to send all deals (not just new ones)
    resend = request.args.get("resend", "").lower() == "true"

    def run_check():
        try:
            from src.job_runner import JobRunner
            logger.info(f"Background deal check started (force={force}, resend={resend})")
            runner = JobRunner()
            runner.run_all_subscribers(force=force, resend=resend)
            logger.info("Background deal check completed")
        except Exception as e:
            logger.error(f"Background deal check failed: {e}")

    # Start in background thread
    thread = threading.Thread(target=run_check, daemon=True)
    thread.start()

    logger.info(f"Manual deal check triggered via admin endpoint (force={force}, resend={resend})")
    return {"status": "ok", "message": f"Deal check started (force={force}, resend={resend}). Check logs."}


@app.route("/admin/run-subscriber", methods=["POST"])
def admin_run_subscriber():
    """Manually trigger a deal check for a single subscriber (runs in background).

    Query params:
        - id: Subscriber ID
        - email: Subscriber email (alternative to id)
        - resend: If true, send all deals (not just new ones)

    Example: POST /admin/run-subscriber?key=xxx&email=user@example.com&resend=true
    """
    import threading

    # Verify admin key
    admin_key = os.getenv("ADMIN_KEY", "")
    provided_key = request.args.get("key", "") or request.headers.get("X-Admin-Key", "")

    if not admin_key or provided_key != admin_key:
        return {"error": "Unauthorized"}, 401

    # Get subscriber identifier
    subscriber_id = request.args.get("id")
    email = request.args.get("email", "").strip().lower()
    resend = request.args.get("resend", "").lower() == "true"

    if not subscriber_id and not email:
        return {"error": "Must provide 'id' or 'email' parameter"}, 400

    # Parse subscriber_id if provided
    if subscriber_id:
        try:
            subscriber_id = int(subscriber_id)
        except ValueError:
            return {"error": "Invalid subscriber ID"}, 400

    def run_check():
        try:
            from src.job_runner import JobRunner
            logger.info(f"Background subscriber check started: id={subscriber_id}, email={email}, resend={resend}")
            runner = JobRunner()
            result = runner.run_single_subscriber(
                subscriber_id=subscriber_id,
                email=email,
                resend=resend,
            )
            logger.info(f"Background subscriber check completed: {result}")
        except Exception as e:
            logger.error(f"Background subscriber check failed: {e}")

    # Start in background thread
    thread = threading.Thread(target=run_check, daemon=True)
    thread.start()

    identifier = email if email else f"id={subscriber_id}"
    logger.info(f"Single subscriber check triggered: {identifier} (resend={resend})")
    return {"status": "ok", "message": f"Deal check started for {identifier}. Check logs for results."}


if __name__ == "__main__":
    # Development server
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"

    logger.info(f"Starting development server on port {port}")
    app.run(host="0.0.0.0", port=port, debug=debug)
