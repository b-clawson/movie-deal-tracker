#!/usr/bin/env python3
"""
Movie Deal Tracker - CLI Entry Point

Command-line interface for testing and local development.
For production, use app.py (web) and worker.py (background jobs).
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Setup logging
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    # File handler
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(log_dir / "tracker.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def load_config() -> dict:
    """Load configuration from YAML and environment."""
    config_path = Path(__file__).parent / "config" / "config.yaml"

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Load environment variables
    load_dotenv()

    return config


def validate_config() -> list:
    """Validate required environment variables. Returns list of errors."""
    errors = []

    if not os.getenv("SERPAPI_KEY"):
        errors.append("SERPAPI_KEY not set")

    # No API key needed for rule-based classifier

    if not os.getenv("RESEND_API_KEY"):
        errors.append("RESEND_API_KEY not set")

    return errors


def list_movies(config: dict):
    """List movies from Letterboxd list."""
    from src.letterboxd_scraper import get_movies_from_list

    list_url = config["letterboxd"]["list_url"]
    print(f"Fetching movies from: {list_url}\n")

    movies = get_movies_from_list(list_url)

    print(f"Found {len(movies)} movies:\n")
    for i, movie in enumerate(movies, 1):
        print(f"{i:3}. {movie}")


def test_classifier():
    """Test the rule-based edition classifier."""
    from src.edition_classifier import EditionClassifier

    classifier = EditionClassifier()

    test_products = [
        "The Shining (Criterion Collection) [4K UHD Blu-ray]",
        "Jaws - Standard Blu-ray",
        "Alien 4K Ultra HD Steelbook Limited Edition",
        "Spider-Man DVD Walmart Exclusive",
        "Arrow Video: Society Limited Edition Blu-ray",
        "House (1977) Blu-ray Criterion Collection",
        "Suspiria 4K UHD Synapse Films",
        "Office Space DVD",
    ]

    print("Testing Rule-Based Edition Classifier\n")
    print("-" * 60)

    for product in test_products:
        result = classifier.classify(product)
        status = "SPECIAL" if result.is_special_edition else "standard"
        print(f"\n[{status:8}] {result.confidence:.0%} confidence")
        print(f"Product:  {product}")
        print(f"Format:   {result.format}")
        print(f"Label:    {result.label or 'N/A'}")
        print(f"Reason:   {result.reason}")


def test_email():
    """Send a test email via Resend."""
    from src.notifier import EmailNotifier

    api_key = os.getenv("RESEND_API_KEY")
    from_email = os.getenv("EMAIL_FROM", "Movie Deal Tracker <deals@resend.dev>")
    test_recipient = os.getenv("TEST_EMAIL_RECIPIENT")

    if not api_key:
        print("Error: RESEND_API_KEY not set")
        sys.exit(1)

    if not test_recipient:
        print("Error: TEST_EMAIL_RECIPIENT not set")
        print("Set this to your email address to receive test emails")
        sys.exit(1)

    print(f"Sending test email to {test_recipient}...")

    notifier = EmailNotifier(api_key=api_key, from_email=from_email)
    success = notifier.send_test(recipient_email=test_recipient)

    if success:
        print("Test email sent successfully!")
    else:
        print("Failed to send test email. Check your API key.")
        sys.exit(1)


def run_job():
    """Run the deal check job once."""
    from src.job_runner import run_job as execute_job

    errors = validate_config()
    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        print("\nCopy .env.example to .env and fill in your credentials.")
        sys.exit(1)

    execute_job()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Movie Deal Tracker - CLI for testing and development"
    )
    parser.add_argument(
        "--list-movies",
        action="store_true",
        help="List movies from your Letterboxd list",
    )
    parser.add_argument(
        "--test-classifier",
        action="store_true",
        help="Test the OpenAI edition classifier",
    )
    parser.add_argument(
        "--test-email",
        action="store_true",
        help="Send a test email notification",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run the deal check job once",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(verbose=args.verbose)

    # Load environment
    load_dotenv()

    # Load config
    try:
        config = load_config()
    except Exception as e:
        print(f"Failed to load config: {e}")
        sys.exit(1)

    # Handle commands
    if args.list_movies:
        list_movies(config)
    elif args.test_classifier:
        test_classifier()
    elif args.test_email:
        test_email()
    elif args.run:
        run_job()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
