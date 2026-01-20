"""
Email notification system for deal alerts using Resend.
"""

import logging
from typing import List
from datetime import datetime

import resend

from .deal_finder import Deal

logger = logging.getLogger(__name__)

# Default placeholder image for deals without thumbnails
DEFAULT_THUMBNAIL = "https://via.placeholder.com/120x160/1c2228/9ab?text=No+Image"


class EmailNotifier:
    """Sends email notifications for found deals via Resend."""

    def __init__(self, api_key: str, from_email: str):
        self.api_key = api_key
        self.from_email = from_email
        resend.api_key = api_key

    def send_deals_to(
        self,
        recipient_email: str,
        deals: List[Deal],
        unsubscribe_url: str = ""
    ) -> bool:
        """Send email notification to a specific recipient with unsubscribe link."""
        if not deals:
            logger.info("No deals to notify")
            return True

        subject = f"🎬 {len(deals)} Boutique Deal{'s' if len(deals) > 1 else ''} Found"
        body = self._format_email_body(deals, unsubscribe_url=unsubscribe_url)

        return self._send_email(subject, body, recipient_email)

    def send_test(self, recipient_email: str) -> bool:
        """Send a test email to verify configuration."""
        subject = "Physical Media, Reigns Supreme - Test Email"
        body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="margin: 0; padding: 0; background-color: #14181c; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #14181c; padding: 40px 20px;">
                <tr>
                    <td align="center">
                        <table width="100%" cellpadding="0" cellspacing="0" style="max-width: 500px;">
                            <!-- Logo -->
                            <tr>
                                <td align="center" style="padding-bottom: 32px;">
                                    <span style="font-size: 18px; font-weight: 700; color: #ffffff;">Physical Media,</span> <span style="font-size: 18px; font-weight: 700; color: #40c463;">Reigns Supreme</span>
                                </td>
                            </tr>
                            <!-- Card -->
                            <tr>
                                <td style="background-color: #1c2228; border-radius: 12px; padding: 32px;">
                                    <h1 style="margin: 0 0 16px 0; font-size: 24px; font-weight: 700; color: #ffffff; text-align: center;">Test Email</h1>
                                    <p style="margin: 0 0 16px 0; font-size: 16px; line-height: 1.5; color: #9ab; text-align: center;">
                                        Your email notifications are configured correctly!
                                    </p>
                                    <p style="margin: 0; font-size: 14px; line-height: 1.5; color: #678; text-align: center;">
                                        You'll receive alerts when special editions from your Letterboxd list go on sale.
                                    </p>
                                </td>
                            </tr>
                            <!-- Footer -->
                            <tr>
                                <td align="center" style="padding-top: 24px;">
                                    <p style="margin: 0; font-size: 12px; color: #456;">
                                        Sent at {datetime.now().strftime("%Y-%m-%d %H:%M")}
                                    </p>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
        </body>
        </html>
        """

        return self._send_email(subject, body, recipient_email)

    def _format_deal_card(self, deal: Deal) -> str:
        """Format a single deal as an HTML card with thumbnail."""
        thumbnail = deal.thumbnail if deal.thumbnail else DEFAULT_THUMBNAIL

        # Display "Price Unavailable" for deals with no price (stored as 0)
        price_display = f"${deal.price:.2f}" if deal.price > 0 else "Price Unavailable"

        return f"""
        <tr>
            <td style="padding-bottom: 16px;">
                <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #252c34; border-radius: 10px; overflow: hidden;">
                    <tr>
                        <!-- Thumbnail -->
                        <td width="100" valign="top" style="padding: 16px;">
                            <a href="{deal.url}" target="_blank" style="display: block;">
                                <img src="{thumbnail}" alt="{deal.movie_title}" width="80" height="100" style="display: block; border-radius: 6px; object-fit: cover; background-color: #1c2228;" />
                            </a>
                        </td>
                        <!-- Details -->
                        <td valign="top" style="padding: 16px 16px 16px 0;">
                            <p style="margin: 0 0 6px 0; font-size: 11px; font-weight: 500; color: #40c463; text-transform: uppercase; letter-spacing: 0.5px;">
                                {deal.matched_example}
                            </p>
                            <a href="{deal.url}" target="_blank" style="text-decoration: none;">
                                <p style="margin: 0 0 8px 0; font-size: 15px; font-weight: 600; color: #ffffff; line-height: 1.3;">
                                    {deal.product_title[:80]}{'...' if len(deal.product_title) > 80 else ''}
                                </p>
                            </a>
                            <p style="margin: 0 0 4px 0; font-size: 20px; font-weight: 700; color: #40c463;">
                                {price_display}
                            </p>
                            <p style="margin: 0; font-size: 13px; color: #678;">
                                {deal.retailer}
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
        """

    def _format_email_body(self, deals: List[Deal], unsubscribe_url: str = "") -> str:
        """Format deals into HTML email body."""
        # Group deals by movie
        deals_by_movie = {}
        for deal in deals:
            if deal.movie_title not in deals_by_movie:
                deals_by_movie[deal.movie_title] = []
            deals_by_movie[deal.movie_title].append(deal)

        # Build deal cards HTML
        deals_html = ""
        for movie_title, movie_deals in deals_by_movie.items():
            # Movie title header
            deals_html += f"""
            <tr>
                <td style="padding: 20px 0 12px 0;">
                    <p style="margin: 0; font-size: 13px; font-weight: 600; color: #9ab; text-transform: uppercase; letter-spacing: 0.5px;">
                        {movie_title}
                    </p>
                </td>
            </tr>
            """
            # Deal cards for this movie
            for deal in movie_deals:
                deals_html += self._format_deal_card(deal)

        # Unsubscribe link
        unsubscribe_html = ""
        if unsubscribe_url:
            unsubscribe_html = f"""
            <tr>
                <td align="center" style="padding-top: 8px;">
                    <a href="{unsubscribe_url}" style="font-size: 12px; color: #456; text-decoration: none;">Unsubscribe</a>
                </td>
            </tr>
            """

        body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="margin: 0; padding: 0; background-color: #14181c; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #14181c; padding: 40px 20px;">
                <tr>
                    <td align="center">
                        <table width="100%" cellpadding="0" cellspacing="0" style="max-width: 520px;">
                            <!-- Logo -->
                            <tr>
                                <td align="center" style="padding-bottom: 32px;">
                                    <span style="font-size: 18px; font-weight: 700; color: #ffffff;">Physical Media,</span> <span style="font-size: 18px; font-weight: 700; color: #40c463;">Reigns Supreme</span>
                                </td>
                            </tr>
                            <!-- Header Card -->
                            <tr>
                                <td style="background-color: #1c2228; border-radius: 12px; padding: 24px; margin-bottom: 20px;">
                                    <h1 style="margin: 0 0 8px 0; font-size: 22px; font-weight: 700; color: #ffffff; text-align: center;">
                                        {len(deals)} Deal{'s' if len(deals) > 1 else ''} Found
                                    </h1>
                                    <p style="margin: 0; font-size: 14px; color: #9ab; text-align: center;">
                                        Special editions from your Letterboxd list
                                    </p>
                                </td>
                            </tr>
                            <!-- Deals List -->
                            {deals_html}
                            <!-- Footer -->
                            <tr>
                                <td align="center" style="padding-top: 32px; border-top: 1px solid #2c3440; margin-top: 20px;">
                                    <p style="margin: 0 0 4px 0; font-size: 12px; color: #567;">
                                        Sent by Physical Media, Reigns Supreme
                                    </p>
                                    <p style="margin: 0; font-size: 11px; color: #456;">
                                        Monitoring your Letterboxd list for collector's editions
                                    </p>
                                </td>
                            </tr>
                            {unsubscribe_html}
                        </table>
                    </td>
                </tr>
            </table>
        </body>
        </html>
        """

        return body

    def _send_email(self, subject: str, body: str, recipient_email: str) -> bool:
        """Send an email via Resend API."""
        try:
            params = {
                "from": self.from_email,
                "to": [recipient_email],
                "subject": subject,
                "html": body,
            }

            response = resend.Emails.send(params)
            logger.info(f"Email sent successfully to {recipient_email}, id: {response.get('id', 'unknown')}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

    def add_to_audience(self, email: str, audience_id: str) -> bool:
        """Add a contact to a Resend audience."""
        try:
            resend.Contacts.create({
                "audience_id": audience_id,
                "email": email,
                "unsubscribed": False,
            })
            logger.info(f"Added {email} to Resend audience {audience_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to add {email} to Resend audience: {e}")
            return False


def create_notifier(api_key: str, from_email: str) -> EmailNotifier:
    """Factory function to create an EmailNotifier."""
    return EmailNotifier(api_key=api_key, from_email=from_email)


if __name__ == "__main__":
    print("Notifier module loaded. Run via main.py for full functionality.")
