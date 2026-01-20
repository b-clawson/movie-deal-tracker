# Movie Deal Tracker

Automated deal finder for boutique Blu-ray collectors. Monitors your Letterboxd watchlist and sends email alerts when Criterion, Arrow, Vinegar Syndrome, Kino Lorber, and other special editions go on sale.

## Features

- **Letterboxd Integration** - Syncs with your public list or watchlist
- **Boutique Label Detection** - Identifies Criterion, Arrow, Vinegar Syndrome, Shout Factory, Kino Lorber editions
- **Price Monitoring** - Set your max price threshold ($5-$100)
- **Email Alerts** - Daily, weekly, or monthly notifications via Resend
- **Smart Search** - Handles alternative titles (e.g., "House" → "Hausu" for the 1977 Japanese film)
- **Sale Period Awareness** - Refreshes cache during known sales (Barnes & Noble Criterion sale, etc.)

## Tech Stack

- **Backend**: Python/Flask
- **Scraping**: Playwright (headless browser)
- **Search**: SerpAPI (Google Shopping + site-specific searches)
- **Email**: Resend
- **Database**: PostgreSQL (production) / SQLite (local)
- **Hosting**: Railway

## Setup

1. Clone the repo
2. Copy `.env.example` to `.env` and fill in:
   - `SERPAPI_KEY` - Get from [serpapi.com](https://serpapi.com)
   - `RESEND_API_KEY` - Get from [resend.com](https://resend.com)
   - `SECRET_KEY` - Random string for Flask sessions
   - `ADMIN_KEY` - Password for admin endpoints
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```
4. Run locally:
   ```bash
   python app.py
   ```

## Admin Endpoints

All require `?key=YOUR_ADMIN_KEY` parameter.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/subscribers` | GET | List all subscribers |
| `/admin/run-check` | POST | Run deal check for all subscribers |
| `/admin/run-subscriber?email=...` | POST | Run deal check for single subscriber |
| `/admin/cache-status` | GET | View cache and sale period status |
| `/admin/clear-cache` | POST | Clear search cache |

## License

MIT
