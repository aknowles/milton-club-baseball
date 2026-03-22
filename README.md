# MDB Knights Baseball - iCal Subscriptions

Automated calendar subscriptions for MDB Knights Perfect Game baseball schedules, with push notifications and practice management.

## How It Works

1. **GitHub Actions** runs twice daily (8 AM & 8 PM ET)
2. **Scraper** fetches team schedules from [Perfect Game](https://www.perfectgame.org)
3. **Calendar files** (per-team `.ics` in `calendars/`) are generated and committed
4. **GitHub Pages** serves the calendar for subscription
5. **ntfy.sh** sends push notifications when schedule changes are detected
6. **Practices** from `config.json` are merged into team calendars

## Subscribe

Visit the [calendar page](https://aknowles.github.io/milton-club-baseball) for per-team subscription links.

### Setup by Platform

- **Google Calendar:** Other calendars (+) → From URL → paste URL
- **Apple Calendar:** File → New Calendar Subscription → paste URL
- **iPhone/iPad:** Tap "Subscribe" button on the calendar page
- **Outlook:** Add calendar → Subscribe from web

## Push Notifications (ntfy.sh)

Each team has an `ntfy_topic` in `config.json`. When the scraper detects schedule changes (new games, removed games, time/location changes), it sends a notification to `https://ntfy.sh/{topic}`.

To receive notifications:
1. Install the [ntfy app](https://ntfy.sh/) on your phone
2. Subscribe to your team's topic (e.g. `mdb-knights-11u-gold`)

## Teams

| Team | Age Group | ntfy Topic |
|------|-----------|------------|
| MDB Knights 11U Gold | 11U | `mdb-knights-11u-gold` |
| MDB Knights 14U Blue | 14U | `mdb-knights-14u-blue` |
| MDB Knights 11U Blue | 11U | `mdb-knights-11u-blue` |
| Braintree Bandits 11U Mercado | 11U | `braintree-bandits-11u-mercado` |

Teams are configured in `config.json`. To add a team, add an entry with the Perfect Game team page URL, team name, year, and ntfy topic.

## Practices

Practices are managed via the `practices` block in `config.json`. Each team supports:

- **`adhoc`**: One-off practice entries with date, time, duration, location, and optional title
- **`modifications`**: Cancel or reschedule a specific practice date
- **`blackout_dates`**: Dates where no practices occur

### Adding/Modifying Practices via GitHub Issues

Use the issue templates to request practice changes without editing `config.json` directly:

1. Go to **Issues → New Issue**
2. Choose a template: **Add Practice**, **Cancel Practice**, or **Modify Practice**
3. Fill in the form and submit
4. A repo admin adds the `approved` label
5. The workflow automatically updates `config.json` and closes the issue

### Practice Config Example

```json
{
  "practices": {
    "MDB Knights 11U Gold": {
      "adhoc": [
        {
          "date": "2026-04-15",
          "time": "5:30 PM",
          "duration_minutes": 90,
          "location": "Cunningham Park Field 2",
          "title": "Hitting Session"
        }
      ],
      "modifications": [
        {
          "date": "2026-04-15",
          "action": "cancel"
        }
      ],
      "blackout_dates": ["2026-07-04"]
    }
  }
}
```

## Notices

Add notices to `config.json` to append messages to calendar event descriptions within a date range:

```json
{
  "notices": [
    {
      "message": "⚠️ Field may be wet — check GroupMe before leaving.",
      "applies_from": "2026-04-01",
      "applies_to": "2026-04-15"
    }
  ]
}
```

Active notices are appended to the DESCRIPTION field of any calendar event falling within the date range.

## Snacks Signup

Track which families are bringing snacks to game days via the `snacks` block in `config.json`. Each team has an array of date/family entries:

```json
{
  "snacks": {
    "MDB Knights 11U Gold": [
      {
        "date": "2026-04-12",
        "families": ["Smith", "Johnson"]
      }
    ]
  }
}
```

When families are signed up for a game date:
- The calendar event description includes a `Snacks: Smith, Johnson` line
- The index page shows a snack tag next to the game (first game of the day for doubleheaders)

## Workflow Failure Notifications

The workflow automatically creates a GitHub issue with the `workflow-failure` label when it fails.

## Configuration

Edit `config.json` to add or modify teams, practices, notices, and snacks:

```json
{
  "teams": [
    {
      "url": "https://www.perfectgame.org/PGBA/Team/default.aspx?...",
      "team_name": "Team Name",
      "year": 2026,
      "ntfy_topic": "team-topic"
    }
  ],
  "base_url": "https://aknowles.github.io/milton-club-baseball",
  "timezone": "US/Eastern",
  "practices": { ... },
  "notices": [ ... ],
  "snacks": { ... }
}
```

## Local Development

```bash
pip install -r requirements.txt pytz
python scraper.py
```

## File Structure

```
.github/workflows/update-calendar.yml       # Main scraper + deploy workflow
.github/workflows/process-practice-changes.yml  # Practice issue processing
.github/ISSUE_TEMPLATE/                      # Issue templates for practice changes
scraper.py                                   # Schedule scraper + calendar generator
process_practice_issue.py                    # Issue body parser for practice changes
config.json                                  # Team, practice, notice, and snacks configuration
calendars/                                   # Generated per-team .ics files
calendars/.snapshot.json                     # Change detection snapshot (auto-managed)
index.html                                   # Generated GitHub Pages site
requirements.txt                             # Python dependencies
```
