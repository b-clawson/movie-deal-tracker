#!/usr/bin/env python3
"""
Background worker for processing deal checks.
Runs the job runner on a schedule to check all subscribers' lists.
"""

import os
import logging
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging():
    """Configure logging."""
    level = logging.INFO

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    # File handler
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(log_dir / "worker.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def main():
    """Main entry point for the worker."""
    setup_logging()
    logger = logging.getLogger(__name__)

    # Import after logging setup
    from src.scheduler import create_scheduler
    from src.job_runner import run_job

    # Get schedule config from environment or defaults
    run_at = os.getenv("SCHEDULE_RUN_AT", "09:00")

    logger.info("Starting deal check worker...")
    logger.info(f"Scheduled to run daily at {run_at}")

    # Create and start scheduler
    scheduler = create_scheduler(
        job_func=run_job,
        run_at=run_at,
    )

    # Start with immediate run to process any pending checks
    scheduler.start(run_immediately=True)


if __name__ == "__main__":
    main()
