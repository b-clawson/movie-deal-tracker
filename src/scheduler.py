"""
Scheduler for automated deal checking.
"""

import logging
import signal
import sys
from datetime import datetime
from typing import Callable, Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)


class DealScheduler:
    """Schedules periodic deal checks."""

    def __init__(self, job_func: Callable[[], None]):
        self.job_func = job_func
        self.scheduler = BlockingScheduler()
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup graceful shutdown on signals."""

        def shutdown_handler(signum, frame):
            logger.info("Received shutdown signal, stopping scheduler...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

    def schedule_daily(self, run_at: str = "09:00"):
        """
        Schedule job to run daily at specified time.

        Args:
            run_at: Time in HH:MM format (24-hour)
        """
        hour, minute = map(int, run_at.split(":"))

        trigger = CronTrigger(hour=hour, minute=minute)
        self.scheduler.add_job(
            self.job_func,
            trigger=trigger,
            id="deal_check",
            name="Daily deal check",
            replace_existing=True,
        )

        logger.info(f"Scheduled daily deal check at {run_at}")

    def schedule_interval(self, hours: int = 24):
        """
        Schedule job to run at regular intervals.

        Args:
            hours: Interval in hours
        """
        trigger = IntervalTrigger(hours=hours)
        self.scheduler.add_job(
            self.job_func,
            trigger=trigger,
            id="deal_check",
            name=f"Deal check every {hours} hours",
            replace_existing=True,
        )

        logger.info(f"Scheduled deal check every {hours} hours")

    def run_now(self):
        """Run the job immediately (once)."""
        logger.info("Running deal check now...")
        self.job_func()

    def start(self, run_immediately: bool = False):
        """
        Start the scheduler.

        Args:
            run_immediately: If True, run the job once before starting scheduler
        """
        if run_immediately:
            self.run_now()

        next_run = self.scheduler.get_jobs()[0].next_run_time if self.scheduler.get_jobs() else None
        if next_run:
            logger.info(f"Next scheduled run: {next_run}")

        logger.info("Starting scheduler... Press Ctrl+C to stop.")
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped.")

    def stop(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped.")


def create_scheduler(
    job_func: Callable[[], None],
    run_at: Optional[str] = None,
    interval_hours: Optional[int] = None,
) -> DealScheduler:
    """
    Factory function to create a configured scheduler.

    Args:
        job_func: Function to run on schedule
        run_at: Time for daily run (HH:MM format)
        interval_hours: Hours between runs (alternative to run_at)
    """
    scheduler = DealScheduler(job_func)

    if run_at:
        scheduler.schedule_daily(run_at)
    elif interval_hours:
        scheduler.schedule_interval(interval_hours)
    else:
        # Default to daily at 9 AM
        scheduler.schedule_daily("09:00")

    return scheduler


if __name__ == "__main__":
    # Test scheduler with dummy job
    logging.basicConfig(level=logging.INFO)

    def test_job():
        print(f"Job executed at {datetime.now()}")

    scheduler = create_scheduler(test_job, interval_hours=1)
    print("Test scheduler created. Run via main.py for full functionality.")
