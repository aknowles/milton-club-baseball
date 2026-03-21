# MDB Knights Baseball - iCal Subscriptions

Automated calendar subscriptions for MDB Knights Perfect Game baseball schedules.

## How It Works

1. **GitHub Actions** runs twice daily (8 AM & 8 PM ET)
2. **Scraper** fetches team schedules from [Perfect Game](https://www.perfectgame.org)
3. **Calendar files** (per-team `.ics` in `calendars/`) are generated and committed
4. **GitHub Pages** serves the calendar for subscription

## Subscribe

Visit the [calendar page](https://aknowles.github.io/milton-club-baseball) for per-team subscription links.

### Setup by Platform

- **Google Calendar:** Other calendars (+) → From URL → paste URL
- **Apple Calendar:** File → New Calendar Subscription → paste URL
- **iPhone/iPad:** Tap "Subscribe" button on the calendar page
- **Outlook:** Add calendar → Subscribe from web

## Teams

| Team | Age Group |
|------|-----------|
| MDB Knights 11U Gold | 11U |
| MDB Knights 14U Blue | 14U |

Teams are configured in `config.json`. To add a team, add an entry with the Perfect Game team page URL, team name, and year.

## Configuration

Edit `config.json` to add or modify teams:

```json
{
  "teams": [
    {
      "url": "https://www.perfectgame.org/PGBA/Team/default.aspx?...",
      "team_name": "Team Name",
      "year": 2026
    }
  ],
  "base_url": "https://aknowles.github.io/milton-club-baseball",
  "timezone": "US/Eastern"
}
```

## Local Development

```bash
pip install -r requirements.txt pytz
python scraper.py
```

## File Structure

```
.github/workflows/update-calendar.yml  # GitHub Actions workflow
scraper.py                              # Schedule scraper + calendar generator
config.json                             # Team configuration
calendars/                              # Generated per-team .ics files
index.html                              # Generated GitHub Pages site
requirements.txt                        # Python dependencies
```
