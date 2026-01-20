"""
Job runner for processing all subscribers.
Handles the multi-user deal checking workflow.
"""

import os
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta

import yaml
from dotenv import load_dotenv

from .database import get_db, Subscriber
from .letterboxd_scraper import get_movies_from_list
from .edition_classifier import EditionClassifier
from .deal_finder import DealFinder, Deal
from .notifier import EmailNotifier

logger = logging.getLogger(__name__)

# Load environment
load_dotenv()


class JobRunner:
    """Runs deal checks for all subscribers."""

    def __init__(self):
        self.config = self._load_config()
        self.classifier = self._create_classifier()
        self.notifier = self._create_notifier()
        self.db = get_db()
        self.serpapi_key = os.getenv("SERPAPI_KEY", "")
        if not self.serpapi_key:
            raise ValueError("SERPAPI_KEY not set in environment")

    def _load_config(self) -> dict:
        """Load configuration."""
        config_path = Path(__file__).parent.parent / "config" / "config.yaml"

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        return config

    def _create_classifier(self) -> EditionClassifier:
        """Create edition classifier."""
        return EditionClassifier()

    def _create_finder_for_subscriber(self, subscriber: Subscriber) -> DealFinder:
        """Create deal finder with subscriber's max_price preference."""
        return DealFinder(
            api_key=self.serpapi_key,
            classifier=self.classifier,
            max_price=subscriber.max_price,
            requests_per_minute=self.config["search"]["requests_per_minute"],
        )

    def _is_due_for_check(self, subscriber: Subscriber) -> bool:
        """Check if subscriber is due for a deal check based on their frequency."""
        if not subscriber.last_checked:
            return True

        try:
            last_checked = datetime.fromisoformat(subscriber.last_checked)
        except ValueError:
            return True

        now = datetime.now()

        if subscriber.check_frequency == "daily":
            return (now - last_checked) >= timedelta(days=1)
        elif subscriber.check_frequency == "weekly":
            return (now - last_checked) >= timedelta(weeks=1)
        elif subscriber.check_frequency == "monthly":
            return (now - last_checked) >= timedelta(days=30)

        return True

    def _create_notifier(self) -> EmailNotifier:
        """Create email notifier."""
        api_key = os.getenv("RESEND_API_KEY", "")
        if not api_key:
            raise ValueError("RESEND_API_KEY not set in environment")

        from_email = os.getenv("EMAIL_FROM", "Movie Deal Tracker <deals@resend.dev>")

        return EmailNotifier(api_key=api_key, from_email=from_email)

    def run_single_subscriber(self, subscriber_id: int = None, email: str = None, resend: bool = False) -> dict:
        """Process a single subscriber by ID or email.

        Args:
            subscriber_id: Subscriber ID to process
            email: Subscriber email to process (alternative to ID)
            resend: If True, send all deals (not just new ones)

        Returns:
            dict with status, deals_found, deals_sent, etc.
        """
        self._resend_mode = resend

        # Find subscriber
        subscriber = None
        if subscriber_id:
            subscriber = self.db.get_subscriber_by_id(subscriber_id)
        elif email:
            subscriber = self.db.get_subscriber_by_email(email)

        if not subscriber:
            return {"status": "error", "message": "Subscriber not found"}

        if not subscriber.active:
            return {"status": "error", "message": "Subscriber is inactive"}

        logger.info(f"Processing single subscriber: {subscriber.email} (resend={resend})")

        try:
            # Get movies from their list
            movies = get_movies_from_list(subscriber.list_url)
            logger.info(f"Found {len(movies)} movies in list for {subscriber.email}")

            if not movies:
                return {
                    "status": "warning",
                    "message": "No movies found in list",
                    "subscriber": subscriber.email,
                    "list_url": subscriber.list_url,
                }

            # Create finder with subscriber's price preference
            finder = self._create_finder_for_subscriber(subscriber)

            # Search for deals
            all_deals = finder.find_deals(movies)
            logger.info(f"Found {len(all_deals)} total deals for {subscriber.email}")

            # Filter to new deals (unless resend mode is enabled)
            if resend:
                deals_to_send = all_deals
            else:
                deals_to_send = self.db.filter_new_deals(subscriber.id, all_deals)

            # Send notification if we have deals
            email_sent = False
            if deals_to_send:
                self._send_notification(subscriber, deals_to_send)
                email_sent = True

            # Update last checked timestamp
            self.db.update_last_checked(subscriber.id)

            return {
                "status": "success",
                "subscriber": subscriber.email,
                "list_url": subscriber.list_url,
                "movies_in_list": len(movies),
                "deals_found": len(all_deals),
                "deals_sent": len(deals_to_send),
                "email_sent": email_sent,
                "resend_mode": resend,
            }

        except Exception as e:
            logger.error(f"Failed to process subscriber {subscriber.email}: {e}")
            return {"status": "error", "message": str(e), "subscriber": subscriber.email}

    def run_all_subscribers(self, force: bool = False, resend: bool = False):
        """Process all active subscribers.

        Args:
            force: If True, bypass frequency check and process all subscribers
            resend: If True, send all deals (not just new ones)
        """
        self._resend_mode = resend
        subscribers = self.db.get_active_subscribers()
        logger.info(f"Processing {len(subscribers)} active subscribers (force={force}, resend={resend})")

        processed = 0
        skipped = 0

        for subscriber in subscribers:
            # Check if subscriber is due for a check based on their frequency
            if not force and not self._is_due_for_check(subscriber):
                logger.info(f"Skipping {subscriber.email} (not due, frequency: {subscriber.check_frequency})")
                skipped += 1
                continue

            try:
                self._process_subscriber(subscriber)
                processed += 1
            except Exception as e:
                logger.error(f"Failed to process subscriber {subscriber.email}: {e}")

        logger.info(f"Finished processing subscribers: {processed} processed, {skipped} skipped")

    def _process_subscriber(self, subscriber: Subscriber):
        """Process a single subscriber."""
        logger.info(f"Processing subscriber: {subscriber.email} (max: ${subscriber.max_price}, freq: {subscriber.check_frequency})")

        # Get movies from their list
        try:
            movies = get_movies_from_list(subscriber.list_url)
            logger.info(f"Found {len(movies)} movies in list for {subscriber.email}")
        except Exception as e:
            logger.error(f"Failed to scrape list for {subscriber.email}: {e}")
            return

        if not movies:
            logger.warning(f"No movies found in list for {subscriber.email}")
            return

        # Create finder with subscriber's price preference
        finder = self._create_finder_for_subscriber(subscriber)

        # Search for deals
        all_deals = finder.find_deals(movies)
        logger.info(f"Found {len(all_deals)} total deals for {subscriber.email}")

        # Filter to new deals (unless resend mode is enabled)
        if getattr(self, '_resend_mode', False):
            deals_to_send = all_deals
            logger.info(f"Resend mode: sending all {len(deals_to_send)} deals for {subscriber.email}")
        else:
            deals_to_send = self.db.filter_new_deals(subscriber.id, all_deals)
            logger.info(f"New deals for {subscriber.email}: {len(deals_to_send)}")

        # Send notification if we have deals
        if deals_to_send:
            self._send_notification(subscriber, deals_to_send)

        # Update last checked timestamp
        self.db.update_last_checked(subscriber.id)

    def _send_notification(self, subscriber: Subscriber, deals: list):
        """Send deal notification to subscriber."""
        logger.info(f"Sending notification to {subscriber.email} with {len(deals)} deals")

        # Get base URL for unsubscribe link
        base_url = os.getenv("BASE_URL", "http://localhost:5000")
        unsubscribe_url = f"{base_url}/unsubscribe/{subscriber.unsubscribe_token}"

        success = self.notifier.send_deals_to(
            recipient_email=subscriber.email,
            deals=deals,
            unsubscribe_url=unsubscribe_url,
        )

        if success:
            logger.info(f"Notification sent to {subscriber.email}")
        else:
            logger.error(f"Failed to send notification to {subscriber.email}")


def run_job():
    """Entry point for running the job."""
    from .database import get_db

    logger.info("Starting deal check job...")

    # Clean up expired cache entries
    db = get_db()
    db.clear_expired_cache()

    runner = JobRunner()
    runner.run_all_subscribers()
    logger.info("Deal check job complete!")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    run_job()
