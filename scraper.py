#!/usr/bin/env python3
"""
Perfect Game Baseball Schedule Scraper

Scrapes PGBA team schedule pages and generates a merged iCal calendar file.
Designed for GitHub Actions + GitHub Pages deployment.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse, unquote_plus

import requests
from bs4 import BeautifulSoup
from icalendar import Calendar, Event, vText

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TIMEZONE = "US/Eastern"
USER_AGENT = "Milton-Club-Baseball-Scraper/1.0 (GitHub Actions; +https://github.com/aknowles/milton-club-baseball)"
REQUEST_DELAY = 2.5  # seconds between requests – be polite
DEBUG = os.environ.get("SCRAPER_DEBUG", "0") == "1"


def debug_log(msg):
    """Print debug messages when SCRAPER_DEBUG=1."""
    if DEBUG:
        print(f"  [DEBUG] {msg}")


def load_config(path="config.json"):
    with open(path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def fetch_page(url):
    """Fetch a page with a standard User-Agent and return the HTML text."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def extract_address_from_maps_link(href):
    """Pull the address string out of a Google Maps URL (the q= parameter)."""
    if not href:
        return None
    parsed = urlparse(href)
    qs = parse_qs(parsed.query)
    addr = qs.get("q", [None])[0]
    if addr:
        return unquote_plus(addr)
    # Fallback: try daddr param
    addr = qs.get("daddr", [None])[0]
    if addr:
        return unquote_plus(addr)
    return None


def parse_schedule(html, team_name, team_url):
    """
    Parse the TEAM SCHEDULE section of a Perfect Game team page.

    The schedule is in a Telerik RadGrid with ID containing 'rgSchedule'.
    The outer grid has event header rows; a nested 'rgEvent' grid has game rows.

    Returns a list of game dicts.
    """
    soup = BeautifulSoup(html, "html.parser")
    games = []

    debug_log(f"HTML length: {len(html)} chars")

    # Save raw HTML for debugging
    if DEBUG:
        safe_name = re.sub(r"[^a-zA-Z0-9]", "_", team_name)
        debug_path = f"debug_{safe_name}.html"
        with open(debug_path, "w") as f:
            f.write(html)
        debug_log(f"Saved raw HTML to {debug_path}")

    # --- Step 1: Find the rgSchedule RadGrid table by ID ---
    schedule_table = soup.find("table", id=re.compile(r"rgSchedule.*_ctl00$", re.I))
    if schedule_table:
        debug_log(f"Found rgSchedule table: id='{schedule_table.get('id', '')}'")
    else:
        # Fallback: any table with ID containing 'rgSchedule'
        schedule_table = soup.find("table", id=re.compile(r"rgSchedule", re.I))
        if schedule_table:
            debug_log(f"Found rgSchedule table (fallback): id='{schedule_table.get('id', '')}'")

    if not schedule_table:
        print(f"  WARNING: Could not find rgSchedule table for {team_name}")
        return games

    # --- Step 2: Extract event info from the outer schedule grid ---
    # The event link (e.g. "2026 11U PG New England League") is in the outer grid
    current_event = "Unknown Event"
    current_event_url = None

    event_link = schedule_table.find("a", href=re.compile(r"/events/Default\.aspx\?event=", re.I))
    if event_link:
        current_event = event_link.get_text(strip=True)
        href = event_link.get("href", "")
        if href.startswith("/"):
            current_event_url = f"https://www.perfectgame.org{href}"
        elif href.startswith("http"):
            current_event_url = href
        else:
            current_event_url = f"https://www.perfectgame.org/{href}"
        debug_log(f"Event: '{current_event}' URL: {current_event_url}")

    # --- Step 3: Find the inner rgEvent table (nested game details) ---
    event_table = schedule_table.find("table", id=re.compile(r"rgEvent", re.I))
    if event_table:
        debug_log(f"Found rgEvent table: id='{event_table.get('id', '')}'")
    else:
        # Use the outer table if no nested one found
        event_table = schedule_table
        debug_log("No nested rgEvent table found, using outer table")

    # --- Step 4: Parse game rows from the event table ---
    rows = event_table.find_all("tr", recursive=False)
    if not rows:
        # RadGrid may have tbody
        tbody = event_table.find("tbody")
        if tbody:
            rows = tbody.find_all("tr", recursive=False)
    debug_log(f"Processing {len(rows)} rows in event table")

    for row_idx, row in enumerate(rows):
        cells = row.find_all("td", recursive=False)
        if not cells:
            continue

        cell_texts = [c.get_text(strip=True) for c in cells]
        if DEBUG and row_idx < 30:
            debug_log(f"  Row[{row_idx}] cells={len(cells)}: {[t[:60] for t in cell_texts]}")
            # Also log any nested tables/divs structure
            for ci, c in enumerate(cells):
                inner_divs = c.find_all("div", recursive=False)
                inner_spans = c.find_all("span", recursive=False)
                inner_links = c.find_all("a", recursive=False)
                if inner_divs or inner_spans or inner_links:
                    parts = []
                    for d in inner_divs:
                        parts.append(f"div:'{d.get_text(strip=True)[:40]}'")
                    for s in inner_spans:
                        parts.append(f"span:'{s.get_text(strip=True)[:40]}'")
                    for a in inner_links:
                        parts.append(f"a[{a.get('href','')[:50]}]:'{a.get_text(strip=True)[:30]}'")
                    debug_log(f"    Cell[{ci}] children: {parts}")

        # Skip empty rows or header rows
        full_text = " ".join(cell_texts)
        if not full_text.strip():
            continue

        # Check if this row has a DiamondKast game link (indicates a game row)
        game_link = row.find("a", href=re.compile(r"DiamondKast/Game\.aspx\?gameid=", re.I))

        # Try to parse the game data from cells
        game = parse_game_row(cells, cell_texts, full_text, game_link,
                              current_event, current_event_url, team_name, team_url)
        if game:
            games.append(game)
            debug_log(f"  -> Parsed game: {game['title']} on {game['date']}")

    # If no games found from the event table, try parsing ALL rows
    # from the outer schedule table (different page structure)
    if not games:
        debug_log("No games from rgEvent, trying all rows in rgSchedule")
        all_rows = schedule_table.find_all("tr")
        debug_log(f"Trying {len(all_rows)} rows from outer table")
        for row_idx, row in enumerate(all_rows):
            cells = row.find_all("td", recursive=False)
            if not cells:
                continue
            cell_texts = [c.get_text(strip=True) for c in cells]
            full_text = " ".join(cell_texts)
            if not full_text.strip():
                continue

            # Check for event header
            ev_link = row.find("a", href=re.compile(r"/events/Default\.aspx\?event=", re.I))
            if ev_link:
                current_event = ev_link.get_text(strip=True)
                href = ev_link.get("href", "")
                if href.startswith("/"):
                    current_event_url = f"https://www.perfectgame.org{href}"
                else:
                    current_event_url = href
                continue

            if DEBUG and row_idx < 30:
                debug_log(f"  OuterRow[{row_idx}] cells={len(cells)}: {[t[:60] for t in cell_texts]}")

            game_link = row.find("a", href=re.compile(r"DiamondKast/Game\.aspx\?gameid=", re.I))
            game = parse_game_row(cells, cell_texts, full_text, game_link,
                                  current_event, current_event_url, team_name, team_url)
            if game:
                games.append(game)
                debug_log(f"  -> Parsed game: {game['title']} on {game['date']}")

    print(f"  Found {len(games)} games for {team_name}")
    return games


def parse_game_row(cells, cell_texts, full_text, game_link,
                   event_name, event_url, team_name, team_url):
    """
    Parse a single game row from the RadGrid schedule table.

    Tries multiple strategies to extract date, time, opponent, and location:
    1. Standard date pattern: "Mar 28 Sat" or "Mar 28"
    2. Opponent from "vs." / "@" indicators
    3. Location from Google Maps links or field name text
    """
    # --- Date parsing ---
    # Try "Mon Day DayOfWeek" pattern first (e.g. "Mar 28 Sat")
    date_match = re.search(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})\s+(Sun|Mon|Tue|Wed|Thu|Fri|Sat)",
        full_text, re.I
    )
    # Fallback: "Mon Day" without day-of-week (e.g. "Mar 28")
    if not date_match:
        date_match = re.search(
            r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})",
            full_text, re.I
        )
    # Fallback: MM/DD/YYYY format
    if not date_match:
        date_match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", full_text)
        if date_match:
            try:
                game_date = datetime(int(date_match.group(3)),
                                     int(date_match.group(1)),
                                     int(date_match.group(2)))
            except ValueError:
                return None
        else:
            return None
    else:
        month_str = date_match.group(1)
        day_str = date_match.group(2)
        now = datetime.now()
        try:
            game_date = datetime.strptime(f"{month_str} {day_str} {now.year}", "%b %d %Y")
            if game_date.month < now.month - 6:
                game_date = datetime.strptime(f"{month_str} {day_str} {now.year + 1}", "%b %d %Y")
        except ValueError:
            return None

    # --- Time parsing ---
    time_match = re.search(r"(\d{1,2}:\d{2}\s*[AP]M)", full_text, re.I)
    game_time = None
    is_allday = False
    if time_match:
        try:
            game_time = datetime.strptime(time_match.group(1).strip(), "%I:%M %p")
        except ValueError:
            is_allday = True
    else:
        is_allday = True

    # --- Home/Away ---
    if re.search(r"\bvs\.?\b", full_text, re.I):
        home_away = "vs."
    elif re.search(r"\b@\b", full_text):
        home_away = "@"
    else:
        home_away = "vs."

    # --- Opponent ---
    opponent = extract_opponent(cells, cell_texts, full_text, home_away)

    # --- Location (field + address) ---
    field_name = None
    address = None
    for cell in cells:
        link = cell.find("a", href=re.compile(r"google\.com/maps|maps\.google", re.I))
        if link:
            address = extract_address_from_maps_link(link.get("href", ""))
            link_text = link.get_text(strip=True)
            if link_text and link_text.lower() not in ("map", "directions", "maps"):
                field_name = link_text
            break

    # Look for field name in cell text if not found via maps link
    if not field_name:
        for text in cell_texts:
            if any(kw in text.lower() for kw in ("field", "park", "complex", "diamond", "stadium", "turf")):
                field_name = text.strip()
                break

    location = None
    if field_name and address:
        location = f"{field_name}, {address}"
    elif field_name:
        location = field_name
    elif address:
        location = address
    if location and "tbd" in location.lower():
        location = None

    # --- Game link ---
    game_url = None
    if game_link:
        href = game_link.get("href", "")
        if href.startswith("/"):
            game_url = f"https://www.perfectgame.org{href}"
        elif href.startswith("http"):
            game_url = href

    # --- Build title ---
    title = f"{team_name} {home_away} {opponent}" if opponent else f"{team_name} - Game"

    return {
        "title": title,
        "date": game_date,
        "time": game_time,
        "is_allday": is_allday,
        "home_away": home_away,
        "opponent": opponent,
        "location": location,
        "field_name": field_name,
        "address": address,
        "event_name": event_name,
        "event_url": event_url,
        "team_name": team_name,
        "team_url": team_url,
        "game_url": game_url,
    }


def extract_opponent(cells, cell_texts, full_text, home_away_indicator):
    """Extract the opponent name from the row cells."""
    # Strategy 1: Find cell right after "vs." or "@"
    for i, t in enumerate(cell_texts):
        if t in ("vs.", "@", "vs"):
            if i + 1 < len(cell_texts):
                opp = cell_texts[i + 1].strip()
                if opp and len(opp) > 1:
                    return opp

    # Strategy 2: Inline "vs." or "@" in a cell
    for t in cell_texts:
        match = re.search(r"(?:vs\.?|@)\s+(.+)", t, re.I)
        if match:
            return match.group(1).strip()

    # Strategy 3: Look for team-name-like text in cells (links to team pages)
    for cell in cells:
        link = cell.find("a", href=re.compile(r"PGBA/Team|team=", re.I))
        if link:
            text = link.get_text(strip=True)
            if text and len(text) > 2:
                return text

    return None


# ---------------------------------------------------------------------------
# Calendar generation
# ---------------------------------------------------------------------------

def team_slug(team_name):
    """Convert a team name to a URL-safe filename slug."""
    return re.sub(r"[^a-z0-9]+", "-", team_name.lower()).strip("-")


def make_calendar(games, config, cal_name="Milton Club Baseball"):
    """Generate an iCal calendar from the list of game dicts."""
    from pytz import timezone as pytz_timezone

    tz = pytz_timezone(config.get("timezone", TIMEZONE))

    cal = Calendar()
    cal.add("prodid", "-//Milton Club Baseball//Perfect Game Schedule//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("method", "PUBLISH")
    cal.add("x-wr-calname", cal_name)
    cal.add("x-wr-timezone", config.get("timezone", TIMEZONE))

    for game in games:
        event = Event()
        event.add("summary", game["title"])

        if game["is_allday"]:
            event.add("dtstart", game["date"].date())
            event.add("dtend", game["date"].date() + timedelta(days=1))
        else:
            start = tz.localize(game["date"].replace(
                hour=game["time"].hour,
                minute=game["time"].minute
            ))
            event.add("dtstart", start)
            event.add("dtend", start + timedelta(hours=2))  # Assume 2-hour games

        if game.get("location"):
            event.add("location", vText(game["location"]))

        # Description
        desc_parts = []
        if game.get("event_name"):
            desc_parts.append(f"Tournament: {game['event_name']}")
        if game.get("event_url"):
            desc_parts.append(f"Event: {game['event_url']}")
        if game.get("team_url"):
            desc_parts.append(f"Team: {game['team_url']}")
        event.add("description", "\n".join(desc_parts))

        # Unique ID
        date_str = game["date"].strftime("%Y%m%d")
        time_str = game["time"].strftime("%H%M") if game["time"] else "ALLDAY"
        safe_team = re.sub(r"[^a-zA-Z0-9]", "", game["team_name"])
        safe_opp = re.sub(r"[^a-zA-Z0-9]", "", game.get("opponent") or "TBD")
        uid = f"{date_str}T{time_str}-{safe_team}-{safe_opp}@milton-club-baseball"
        event.add("uid", uid)

        event.add("dtstamp", datetime.utcnow())

        cal.add_component(event)

    return cal


# ---------------------------------------------------------------------------
# Index HTML generation (matching ssbball style)
# ---------------------------------------------------------------------------

def generate_index_html(all_games, config):
    """Generate an index.html for GitHub Pages with calendar subscribe links."""
    base_url = config.get("base_url", "")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M EST")

    # Group games by team
    teams = {}
    for game in all_games:
        name = game["team_name"]
        if name not in teams:
            teams[name] = []
        teams[name].append(game)

    # Build team sections
    team_sections = ""
    for team_name, games in sorted(teams.items()):
        upcoming = [g for g in games if g["date"] >= datetime.now()]
        upcoming.sort(key=lambda g: g["date"])

        games_html = ""
        for g in upcoming[:5]:
            date_str = g["date"].strftime("%b %d")
            time_str = g["time"].strftime("%I:%M %p").lstrip("0") if g["time"] else "TBD"
            loc_str = g.get("field_name") or g.get("location") or ""
            games_html += f"""
                <div class="game-row">
                    <span class="game-date">{date_str}</span>
                    <span class="game-time">{time_str}</span>
                    <span class="game-matchup">{g['home_away']} {g.get('opponent', 'TBD')}</span>
                    <span class="game-location">{loc_str}</span>
                </div>"""

        event_label = ""
        if games and games[0].get("event_name"):
            event_label = f'<span class="event-tag">{games[0]["event_name"]}</span>'

        slug = team_slug(team_name)
        team_cal_url = f"{base_url}/calendars/{slug}.ics"

        team_sections += f"""
        <div class="grade-section">
            <button class="collapsible" onclick="toggleSection(this)">
                <div>
                    <span class="grade-title">{team_name}</span>
                    <span class="grade-info">{len(upcoming)} upcoming game{'s' if len(upcoming) != 1 else ''}</span>
                </div>
                <span class="arrow">&#9660;</span>
            </button>
            <div class="collapsible-content">
                {event_label}
                <div class="subscribe-url">
                    <code>{team_cal_url}</code>
                    <button onclick="copyUrl('{team_cal_url}')" title="Copy URL">&#128203;</button>
                </div>
                <div class="buttons" style="margin-bottom: 12px;">
                    <a class="btn btn-primary" href="webcal://{team_cal_url.replace('https://', '')}">Subscribe</a>
                    <a class="btn btn-secondary" href="{team_cal_url}" download>Download .ics</a>
                </div>
                <div class="upcoming-games">
                    {games_html if games_html else '<p style="color:#666;">No upcoming games found.</p>'}
                </div>
            </div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Milton Club Baseball Calendars</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#9918;</text></svg>">
    <style>
        * {{ box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 900px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
            color: #333;
        }}
        h1 {{ text-align: center; color: #1a1a2e; }}
        h2 {{ color: #1a1a2e; margin-top: 30px; border-bottom: 2px solid #1e6b3a; padding-bottom: 8px; }}
        .subtitle {{ text-align: center; color: #666; margin-bottom: 30px; }}

        .calendar-card {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 16px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .calendar-card h3 {{ margin: 0 0 8px 0; color: #1a1a2e; }}
        .description {{ color: #666; margin: 0 0 12px 0; font-size: 14px; }}
        .subscribe-url {{
            display: flex;
            align-items: center;
            gap: 8px;
            background: #f0f0f0;
            padding: 10px;
            border-radius: 8px;
            margin-bottom: 12px;
        }}
        .subscribe-url code {{ flex: 1; font-size: 11px; word-break: break-all; }}
        .subscribe-url button {{
            background: none;
            border: none;
            cursor: pointer;
            font-size: 16px;
            padding: 4px;
        }}
        .buttons {{ display: flex; gap: 10px; flex-wrap: wrap; }}
        .btn {{
            display: inline-block;
            padding: 8px 16px;
            border-radius: 6px;
            text-decoration: none;
            font-weight: 500;
            font-size: 13px;
        }}
        .btn-primary {{ background: #1e6b3a; color: white; }}
        .btn-secondary {{ background: #1a1a2e; color: white; }}

        .grade-section {{ margin-bottom: 12px; }}
        .collapsible {{
            width: 100%;
            background: #1a1a2e;
            color: white;
            padding: 16px 20px;
            border: none;
            border-radius: 10px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 16px;
            transition: background 0.2s;
        }}
        .collapsible:hover {{ background: #2a2a4e; }}
        .collapsible.active {{ border-radius: 10px 10px 0 0; }}
        .grade-title {{ font-weight: 700; }}
        .grade-info {{ font-size: 13px; opacity: 0.8; margin-left: 12px; }}
        .arrow {{ transition: transform 0.3s; }}
        .collapsible.active .arrow {{ transform: rotate(180deg); }}
        .collapsible-content {{
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-out;
            background: #e8e8e8;
            border-radius: 0 0 10px 10px;
            padding: 0 16px;
        }}
        .collapsible-content.open {{
            max-height: 5000px;
            padding: 16px;
        }}

        .game-row {{
            display: flex;
            gap: 12px;
            padding: 8px 0;
            border-bottom: 1px solid #ddd;
            font-size: 14px;
        }}
        .game-row:last-child {{ border-bottom: none; }}
        .game-date {{ font-weight: 600; min-width: 50px; }}
        .game-time {{ color: #666; min-width: 70px; }}
        .game-matchup {{ flex: 1; }}
        .game-location {{ color: #888; font-size: 12px; }}
        .event-tag {{
            display: inline-block;
            background: #1e6b3a;
            color: white;
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 12px;
            margin-bottom: 10px;
        }}

        .instructions {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            margin-top: 30px;
        }}
        .instructions h2 {{ margin-top: 0; border: none; }}
        .instructions ul {{ padding-left: 20px; }}
        .instructions li {{ margin-bottom: 10px; }}
        .footer {{
            text-align: center;
            margin-top: 30px;
            color: #666;
            font-size: 13px;
        }}
        .copied {{
            position: fixed;
            top: 20px;
            right: 20px;
            background: #4caf50;
            color: white;
            padding: 12px 24px;
            border-radius: 8px;
            display: none;
            z-index: 1000;
        }}
    </style>
</head>
<body>
    <h1>Milton Club Baseball</h1>
    <p class="subtitle">Subscribe to automatically sync game schedules to your calendar</p>

    <div id="copied" class="copied">URL Copied!</div>

    <h2>Team Schedules</h2>
    <p style="color: #666; font-size: 14px;">Click a team to subscribe to their calendar or see upcoming games.</p>
    {team_sections}

    <div class="instructions">
        <h2>How to Subscribe</h2>
        <ul>
            <li><strong>Google Calendar:</strong> Other calendars (+) &rarr; From URL &rarr; paste URL</li>
            <li><strong>Apple Calendar:</strong> File &rarr; New Calendar Subscription &rarr; paste URL</li>
            <li><strong>iPhone/iPad:</strong> Tap "Subscribe" button, or Settings &rarr; Calendar &rarr; Accounts &rarr; Add Subscribed Calendar</li>
            <li><strong>Outlook:</strong> Add calendar &rarr; Subscribe from web</li>
        </ul>
        <p><strong>Tip:</strong> Calendars auto-update every 24 hours. Schedule data refreshes twice daily.</p>
    </div>

    <p class="footer">
        Last updated: {now_str}<br>
        Data from <a href="https://www.perfectgame.org" style="color:#1e6b3a;">Perfect Game</a>
    </p>

    <script>
        function copyUrl(url) {{
            navigator.clipboard.writeText(url).then(() => {{
                const el = document.getElementById('copied');
                el.style.display = 'block';
                setTimeout(() => el.style.display = 'none', 2000);
            }});
        }}

        function toggleSection(btn) {{
            btn.classList.toggle('active');
            const content = btn.nextElementSibling;
            content.classList.toggle('open');
        }}
    </script>
</body>
</html>"""
    return html


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config = load_config()
    teams = config.get("teams", [])

    if not teams:
        print("ERROR: No teams configured in config.json")
        sys.exit(1)

    all_games = []

    for i, team in enumerate(teams):
        url = team["url"]
        name = team["team_name"]
        print(f"Fetching schedule for {name}...")

        try:
            html = fetch_page(url)
            games = parse_schedule(html, name, url)
            all_games.extend(games)
        except requests.RequestException as e:
            print(f"  ERROR fetching {name}: {e}")
            continue

        # Polite delay between requests
        if i < len(teams) - 1:
            print(f"  Waiting {REQUEST_DELAY}s before next request...")
            time.sleep(REQUEST_DELAY)

    print(f"\nTotal games found: {len(all_games)}")

    if not all_games:
        print("WARNING: No games found. Writing empty calendars.")

    # Group games by team and write per-team ICS files
    os.makedirs("calendars", exist_ok=True)
    games_by_team = {}
    for game in all_games:
        games_by_team.setdefault(game["team_name"], []).append(game)

    for tname, tgames in games_by_team.items():
        cal = make_calendar(tgames, config, cal_name=tname)
        filename = f"calendars/{team_slug(tname)}.ics"
        with open(filename, "wb") as f:
            f.write(cal.to_ical())
        print(f"Wrote {filename}")

    # Generate and write index.html
    index_html = generate_index_html(all_games, config)
    with open("index.html", "w") as f:
        f.write(index_html)
    print("Wrote index.html")

    print("Done!")


if __name__ == "__main__":
    main()
