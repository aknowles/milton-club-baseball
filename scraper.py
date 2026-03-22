#!/usr/bin/env python3
"""
Perfect Game Baseball Schedule Scraper

Scrapes PGBA team schedule pages and generates a merged iCal calendar file.
Designed for GitHub Actions + GitHub Pages deployment.
"""

import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, date, timedelta
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


def parse_roster(html, team_name):
    """
    Parse the TEAM ROSTER section of a Perfect Game team page.

    The roster is in a Telerik RadGrid with ID containing 'rgRoster'.
    Returns a list of player dicts with keys like 'name', 'number', 'position'.
    """
    soup = BeautifulSoup(html, "html.parser")
    players = []

    # Find the rgRoster table (same Telerik RadGrid pattern as rgSchedule)
    roster_table = soup.find("table", id=re.compile(r"rgRoster.*_ctl00$", re.I))
    if not roster_table:
        roster_table = soup.find("table", id=re.compile(r"rgRoster", re.I))
    if not roster_table:
        debug_log(f"Could not find rgRoster table for {team_name}")
        return players

    debug_log(f"Found rgRoster table: id='{roster_table.get('id', '')}'")

    # Find header row to determine column mapping
    header_row = roster_table.find("tr", class_=re.compile(r"rgHeader", re.I))
    if not header_row:
        thead = roster_table.find("thead")
        if thead:
            header_row = thead.find("tr")
    col_map = {}
    if header_row:
        for idx, th in enumerate(header_row.find_all(["th", "td"])):
            text = th.get_text(strip=True).lower()
            if "name" in text or "player" in text:
                col_map["name"] = idx
            elif text in ("#", "no", "no.", "number", "jersey"):
                col_map["number"] = idx
            elif "pos" in text:
                col_map["position"] = idx
            elif "bat" in text or "b/t" in text:
                col_map["bats_throws"] = idx
            elif "throw" in text:
                col_map["throws"] = idx
            elif "grad" in text or "year" in text or "class" in text:
                col_map["grad_year"] = idx
        debug_log(f"Roster column map: {col_map}")

    # Parse data rows
    rows = roster_table.find_all("tr", class_=re.compile(r"rgRow|rgAltRow", re.I))
    if not rows:
        tbody = roster_table.find("tbody")
        if tbody:
            rows = tbody.find_all("tr")

    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue
        cell_texts = [c.get_text(strip=True) for c in cells]

        # Skip header-like or empty rows
        if not any(cell_texts):
            continue

        player = {}
        if col_map:
            if "name" in col_map and col_map["name"] < len(cell_texts):
                player["name"] = cell_texts[col_map["name"]]
            if "number" in col_map and col_map["number"] < len(cell_texts):
                player["number"] = cell_texts[col_map["number"]]
            if "position" in col_map and col_map["position"] < len(cell_texts):
                player["position"] = cell_texts[col_map["position"]]
            if "bats_throws" in col_map and col_map["bats_throws"] < len(cell_texts):
                player["bats_throws"] = cell_texts[col_map["bats_throws"]]
            if "throws" in col_map and col_map["throws"] < len(cell_texts):
                player["throws"] = cell_texts[col_map["throws"]]
            if "grad_year" in col_map and col_map["grad_year"] < len(cell_texts):
                player["grad_year"] = cell_texts[col_map["grad_year"]]
        else:
            # Fallback: guess based on cell count and content patterns
            for i, text in enumerate(cell_texts):
                if re.match(r"^\d{1,3}$", text) and "number" not in player:
                    player["number"] = text
                elif len(text) > 3 and re.search(r"[a-zA-Z]{2,}", text) and "name" not in player:
                    player["name"] = text
                elif text.upper() in ("P", "C", "1B", "2B", "3B", "SS", "LF", "CF", "RF",
                                       "OF", "IF", "DH", "UT", "RHP", "LHP", "MIF", "INF"):
                    player["position"] = text

        if player.get("name"):
            players.append(player)
            debug_log(f"  Roster: {player}")

    print(f"  Found {len(players)} roster players for {team_name}")
    return players


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
# Practices
# ---------------------------------------------------------------------------

def build_practice_events(config):
    """
    Build practice events from the practices block in config.json.

    Each team can have:
      - adhoc: list of {date, time, duration_minutes, location, title}
      - modifications: list of {date, action, ...} where action is "cancel" or
        "reschedule" with new_date/new_time
      - blackout_dates: list of date strings where no practices occur
    """
    from pytz import timezone as pytz_timezone
    tz = pytz_timezone(config.get("timezone", TIMEZONE))
    practices_cfg = config.get("practices", {})
    events = []

    for team_name, pdata in practices_cfg.items():
        blackout = set(pdata.get("blackout_dates", []))
        mods = {m["date"]: m for m in pdata.get("modifications", [])}

        for entry in pdata.get("adhoc", []):
            d = entry["date"]
            if d in blackout:
                continue

            mod = mods.get(d)
            if mod and mod.get("action") == "cancel":
                continue

            actual_date = d
            actual_time = entry.get("time")
            if mod and mod.get("action") == "reschedule":
                actual_date = mod.get("new_date", d)
                actual_time = mod.get("new_time", actual_time)

            title = entry.get("title", f"{team_name} Practice")
            location = entry.get("location")
            duration = entry.get("duration_minutes", 90)

            try:
                dt = datetime.strptime(actual_date, "%Y-%m-%d")
            except ValueError:
                print(f"  WARNING: Invalid practice date '{actual_date}' for {team_name}")
                continue

            practice_time = None
            is_allday = True
            if actual_time:
                try:
                    practice_time = datetime.strptime(actual_time, "%I:%M %p")
                    is_allday = False
                except ValueError:
                    try:
                        practice_time = datetime.strptime(actual_time, "%H:%M")
                        is_allday = False
                    except ValueError:
                        pass

            events.append({
                "title": title,
                "date": dt,
                "time": practice_time,
                "is_allday": is_allday,
                "home_away": "",
                "opponent": None,
                "location": location,
                "field_name": location,
                "address": None,
                "event_name": "Practice",
                "event_url": None,
                "team_name": team_name,
                "team_url": None,
                "game_url": None,
                "is_practice": True,
                "duration_minutes": duration,
            })

    return events


# ---------------------------------------------------------------------------
# Notices
# ---------------------------------------------------------------------------

def get_snack_families(config, team_name, event_date):
    """Return list of family names signed up for snacks on a given date."""
    snacks_cfg = config.get("snacks", {})
    team_snacks = snacks_cfg.get(team_name, [])
    if isinstance(event_date, datetime):
        check_date = event_date.strftime("%Y-%m-%d")
    elif isinstance(event_date, date):
        check_date = event_date.strftime("%Y-%m-%d")
    else:
        return []

    for entry in team_snacks:
        if entry.get("date") == check_date:
            return entry.get("families", [])
    return []


def get_active_notices(config, event_date):
    """Return list of notice messages active for a given date."""
    notices = config.get("notices", [])
    active = []
    if isinstance(event_date, datetime):
        check_date = event_date.date()
    elif isinstance(event_date, date):
        check_date = event_date
    else:
        return active

    for notice in notices:
        try:
            from_date = datetime.strptime(notice["applies_from"], "%Y-%m-%d").date()
            to_date = datetime.strptime(notice["applies_to"], "%Y-%m-%d").date()
        except (ValueError, KeyError):
            continue
        if from_date <= check_date <= to_date:
            active.append(notice["message"])
    return active


# ---------------------------------------------------------------------------
# ntfy.sh Notifications
# ---------------------------------------------------------------------------

NTFY_URL = "https://ntfy.sh"


def load_previous_snapshot(path="calendars/.snapshot.json"):
    """Load the previous calendar snapshot for change detection."""
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {}


def save_snapshot(snapshot, path="calendars/.snapshot.json"):
    """Save the current calendar snapshot."""
    with open(path, "w") as f:
        json.dump(snapshot, f, indent=2, default=str)


def build_snapshot(games_by_team):
    """Build a dict snapshot of games keyed by team, then by uid."""
    snapshot = {}
    for team_name, games in games_by_team.items():
        team_events = {}
        for g in games:
            date_str = g["date"].strftime("%Y-%m-%d")
            time_str = g["time"].strftime("%I:%M %p") if g.get("time") else "TBD"
            uid = f"{date_str}-{g.get('opponent') or 'TBD'}-{time_str}"
            team_events[uid] = {
                "title": g["title"],
                "date": date_str,
                "time": time_str,
                "location": g.get("location") or "",
                "opponent": g.get("opponent") or "",
            }
        snapshot[team_name] = team_events
    return snapshot


def detect_changes(old_snapshot, new_snapshot):
    """
    Compare old and new snapshots. Returns a dict of team_name -> list of
    change description strings.
    """
    changes = {}
    all_teams = set(list(old_snapshot.keys()) + list(new_snapshot.keys()))

    for team in all_teams:
        old_events = old_snapshot.get(team, {})
        new_events = new_snapshot.get(team, {})
        team_changes = []

        # New games
        for uid in new_events:
            if uid not in old_events:
                e = new_events[uid]
                team_changes.append(
                    f"New: {e['title']} on {e['date']} at {e['time']}"
                )

        # Removed games
        for uid in old_events:
            if uid not in new_events:
                e = old_events[uid]
                team_changes.append(
                    f"Removed: {e['title']} on {e['date']}"
                )

        # Changed games (same uid, different details)
        for uid in new_events:
            if uid in old_events:
                old_e = old_events[uid]
                new_e = new_events[uid]
                diffs = []
                if old_e.get("time") != new_e.get("time"):
                    diffs.append(f"time {old_e['time']} -> {new_e['time']}")
                if old_e.get("location") != new_e.get("location"):
                    diffs.append(f"location -> {new_e['location']}")
                if diffs:
                    team_changes.append(
                        f"Changed: {new_e['title']} on {new_e['date']} ({', '.join(diffs)})"
                    )

        if team_changes:
            changes[team] = team_changes

    return changes


def send_ntfy(topic, title, message):
    """Send a push notification via ntfy.sh."""
    try:
        resp = requests.post(
            f"{NTFY_URL}/{topic}",
            data=message.encode("utf-8"),
            headers={
                "Title": title,
                "Priority": "default",
                "Tags": "baseball",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            print(f"  ntfy sent to {topic}: {title}")
        else:
            print(f"  ntfy error ({resp.status_code}): {resp.text[:200]}")
    except requests.RequestException as e:
        print(f"  ntfy send failed for {topic}: {e}")


def notify_changes(changes, config):
    """Send ntfy notifications for detected schedule changes."""
    team_topics = {}
    for team in config.get("teams", []):
        if team.get("ntfy_topic"):
            team_topics[team["team_name"]] = team["ntfy_topic"]

    for team_name, change_list in changes.items():
        topic = team_topics.get(team_name)
        if not topic:
            continue
        body = "\n".join(change_list)
        send_ntfy(topic, f"Schedule Update: {team_name}", body)


# ---------------------------------------------------------------------------
# Calendar generation
# ---------------------------------------------------------------------------

def save_rosters(rosters_by_team, path="calendars/rosters.json"):
    """Save scraped roster data to a JSON file."""
    with open(path, "w") as f:
        json.dump(rosters_by_team, f, indent=2)
    print(f"Wrote {path}")


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

        if game.get("location"):
            event.add("location", vText(game["location"]))

        # Duration: practices use their own duration, games default to 2h
        duration_hrs = 2
        if game.get("is_practice"):
            duration_hrs = game.get("duration_minutes", 90) / 60

        if game["is_allday"]:
            event.add("dtstart", game["date"].date())
            event.add("dtend", game["date"].date() + timedelta(days=1))
        else:
            start = tz.localize(game["date"].replace(
                hour=game["time"].hour,
                minute=game["time"].minute
            ))
            event.add("dtstart", start)
            event.add("dtend", start + timedelta(hours=duration_hrs))

        # Description
        desc_parts = []
        if game.get("event_name") and game["event_name"] != "Practice":
            desc_parts.append(f"Tournament: {game['event_name']}")
        if game.get("event_url"):
            desc_parts.append(f"Event: {game['event_url']}")
        if game.get("team_url"):
            desc_parts.append(f"Team: {game['team_url']}")

        # Append snack signup info
        snack_families = get_snack_families(config, game["team_name"], game["date"])
        if snack_families:
            desc_parts.append(f"Snacks: {', '.join(snack_families)}")

        # Append active notices
        active_notices = get_active_notices(config, game["date"])
        for notice in active_notices:
            desc_parts.append(notice)

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

def generate_index_html(all_games, config, rosters_by_team=None):
    """Generate an index.html for GitHub Pages with calendar subscribe links."""
    rosters_by_team = rosters_by_team or {}
    base_url = config.get("base_url", "")
    from pytz import timezone as pytz_timezone
    tz = pytz_timezone(config.get("timezone", TIMEZONE))
    now_str = datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")

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
        snack_dates_shown = set()
        for g in upcoming[:5]:
            date_str = g["date"].strftime("%b %d")
            date_key = g["date"].strftime("%Y-%m-%d")
            time_str = g["time"].strftime("%I:%M %p").lstrip("0") if g["time"] else "TBD"
            loc_str = g.get("field_name") or g.get("location") or ""

            # Show snack tag on first game of a double-header day
            snack_tag = ""
            if date_key not in snack_dates_shown:
                snack_families = get_snack_families(config, team_name, g["date"])
                if snack_families:
                    snack_tag = f'<span class="snack-tag">Snacks: {", ".join(snack_families)}</span>'
                    snack_dates_shown.add(date_key)

            games_html += f"""
                <div class="game-row">
                    <span class="game-date">{date_str}</span>
                    <span class="game-time">{time_str}</span>
                    <span class="game-matchup">{g['home_away']} {g.get('opponent', 'TBD')}</span>
                    <span class="game-location">{loc_str}</span>
                    {snack_tag}
                </div>"""

        event_label = ""
        if games and games[0].get("event_name"):
            event_label = f'<span class="event-tag">{games[0]["event_name"]}</span>'

        # Build roster HTML for this team
        roster_html = ""
        team_roster = rosters_by_team.get(team_name, [])
        if team_roster:
            roster_rows = ""
            for p in team_roster:
                num = p.get("number", "")
                name = p.get("name", "")
                pos = p.get("position", "")
                bt = p.get("bats_throws", "")
                roster_rows += f"""
                        <tr style="border-bottom:1px solid #ddd;">
                            <td style="padding:4px 8px; text-align:center;">{num}</td>
                            <td style="padding:4px 8px;">{name}</td>
                            <td style="padding:4px 8px; text-align:center;">{pos}</td>
                            <td style="padding:4px 8px; text-align:center;">{bt}</td>
                        </tr>"""
            roster_html = f"""
                <div class="roster-section" style="margin-top:12px;">
                    <strong style="font-size:14px;">Roster</strong>
                    <table style="width:100%; border-collapse:collapse; margin-top:6px; font-size:13px;">
                        <thead>
                            <tr style="border-bottom:2px solid #1e6b3a; text-align:left;">
                                <th style="padding:4px 8px; width:40px;">#</th>
                                <th style="padding:4px 8px;">Name</th>
                                <th style="padding:4px 8px; width:50px;">Pos</th>
                                <th style="padding:4px 8px; width:50px;">B/T</th>
                            </tr>
                        </thead>
                        <tbody>{roster_rows}
                        </tbody>
                    </table>
                </div>"""

        slug = team_slug(team_name)
        team_cal_url = f"{base_url}/calendars/{slug}.ics"

        # Build snack signup family checkboxes from roster
        snack_picker_html = ""
        family_names = sorted(set(
            p.get("name", "").strip().split()[-1]
            for p in team_roster
            if p.get("name", "").strip()
        ))
        if family_names:
            picker_id = f"snack-picker-{slug}"
            cbs = ""
            for fam in family_names:
                cb_id = f"snack-{slug}-{fam.lower()}"
                cbs += f"""
                        <label style="display:inline-block; margin:3px 10px 3px 0; cursor:pointer;">
                            <input type="checkbox" class="snack-cb" data-picker="{picker_id}" value="{fam}"> {fam}
                        </label>"""
            # Build game date options from upcoming games
            date_options = ""
            seen_dates = set()
            for g in upcoming:
                d = g["date"].strftime("%Y-%m-%d")
                if d not in seen_dates:
                    label = g["date"].strftime("%b %d")
                    date_options += f'<option value="{d}">{label}</option>'
                    seen_dates.add(d)
            snack_picker_html = f"""
                <div class="snack-picker" id="{picker_id}" style="display:none; background:#fef9f0; border:1px solid #e67e22; border-radius:6px; padding:10px 12px; margin-bottom:12px;">
                    <strong style="font-size:13px;">Snack Signup</strong>
                    <div style="margin:6px 0;">
                        <label style="font-size:12px; color:#555;">Game Date:</label>
                        <select class="snack-date" data-picker="{picker_id}" style="margin-left:4px; padding:2px 6px; font-size:13px;">
                            <option value="">Select date...</option>
                            {date_options}
                        </select>
                    </div>
                    <div style="margin:6px 0; font-size:13px;">
                        <label style="font-size:12px; color:#555;">Family:</label><br>
                        {cbs}
                    </div>
                    <div style="margin:6px 0; font-size:13px;">
                        <label style="font-size:12px; color:#555;">Other (not listed above):</label><br>
                        <input type="text" class="snack-other" data-picker="{picker_id}" placeholder="e.g. Smith, Jones" style="padding:3px 6px; font-size:13px; width:200px; margin-top:2px;">
                    </div>
                    <button class="btn btn-snack" style="font-size:12px; padding:4px 12px; margin-top:4px;" onclick="submitSnackSignup('{picker_id}', '{team_name}')">Submit Signup</button>
                </div>"""

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
                    <button class="btn btn-snack" onclick="toggleSnackPicker('snack-picker-{slug}')">Sign Up for Snacks</button>
                </div>
                {snack_picker_html}
                <div class="upcoming-games">
                    {games_html if games_html else '<p style="color:#666;">No upcoming games found.</p>'}
                </div>
                {roster_html}
            </div>
        </div>"""

    # Build ntfy topic rows for the notifications table
    ntfy_rows = ""
    for team_cfg in config.get("teams", []):
        topic = team_cfg.get("ntfy_topic", "")
        if topic:
            ntfy_rows += f"""
                <tr style="border-bottom:1px solid #ddd;">
                    <td style="padding:8px 6px;">{team_cfg['team_name']}</td>
                    <td style="padding:8px 6px;"><code>{topic}</code></td>
                </tr>"""

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
        .btn-snack {{ background: #e67e22; color: white; }}
        .snack-tag {{
            display: inline-block;
            background: #e67e22;
            color: white;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 11px;
            margin-left: 8px;
        }}

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

    <div class="instructions" style="margin-top: 16px;">
        <h2>Push Notifications</h2>
        <p>Get notified on your phone when games are added, removed, or changed (time/location updates).</p>
        <ol>
            <li>Install the free <a href="https://ntfy.sh" style="color:#1e6b3a; font-weight:600;">ntfy app</a> (<a href="https://apps.apple.com/us/app/ntfy/id1625396347" style="color:#1e6b3a;">iOS</a> / <a href="https://play.google.com/store/apps/details?id=io.heckel.ntfy" style="color:#1e6b3a;">Android</a>)</li>
            <li>Open the app and tap <strong>+</strong> to subscribe to a topic</li>
            <li>Enter your team's topic from the table below</li>
        </ol>
        <table style="width:100%; border-collapse:collapse; margin-top:12px; font-size:14px;">
            <thead>
                <tr style="border-bottom:2px solid #1e6b3a; text-align:left;">
                    <th style="padding:8px 6px;">Team</th>
                    <th style="padding:8px 6px;">ntfy Topic</th>
                </tr>
            </thead>
            <tbody>
                {ntfy_rows}
            </tbody>
        </table>
    </div>

    <div class="instructions" style="margin-top: 16px;">
        <h2>FAQ</h2>
        <details style="margin-bottom:12px;">
            <summary style="cursor:pointer; font-weight:600;">How often does the calendar update?</summary>
            <p style="margin:8px 0 0 0; color:#555;">The schedule is copied from Perfect Game automatically. Your calendar app may take a few hours to pull the latest file &mdash; most apps refresh subscriptions every 12&ndash;24 hours.</p>
        </details>
        <details style="margin-bottom:12px;">
            <summary style="cursor:pointer; font-weight:600;">What triggers a push notification?</summary>
            <p style="margin:8px 0 0 0; color:#555;">Notifications are sent when Perfect Game data changes between runs: new games added, games removed, or time/location updates. Practice additions via GitHub Issues also trigger a calendar update on the next run.</p>
        </details>
        <details style="margin-bottom:12px;">
            <summary style="cursor:pointer; font-weight:600;">Why don't I see practices on the calendar?</summary>
            <p style="margin:8px 0 0 0; color:#555;">Practices must be added to the schedule by a coach or admin. Only explicitly scheduled practices appear &mdash; there is no auto-generated weekly cadence. If your coach has scheduled a practice and it's not showing up, give it up to 24 hours for your calendar app to refresh.</p>
        </details>
        <details style="margin-bottom:12px;">
            <summary style="cursor:pointer; font-weight:600;">How do I add, cancel, or change a practice?</summary>
            <p style="margin:8px 0 0 0; color:#555;">Coaches and team admins can submit practice changes through our <a href="https://github.com/aknowles/milton-club-baseball/issues/new/choose" style="color:#1e6b3a;">GitHub Issues page</a>. Choose the appropriate template (Add Practice, Cancel Practice, or Modify Practice), fill in the details, and submit. Each issue is for a single team. Once submitted, an admin reviews and comments <code>/approve</code> on the issue to apply the change. The calendar updates automatically after that.</p>
        </details>
        <details style="margin-bottom:12px;">
            <summary style="cursor:pointer; font-weight:600;">What kinds of events can be added as practices?</summary>
            <p style="margin:8px 0 0 0; color:#555;">Anything the team does outside of scheduled games: team practices, bullpen sessions, hitting sessions, fielding clinics, team meetings, etc. Each entry includes a date, time, duration, location, and optional custom title.</p>
        </details>
        <details style="margin-bottom:12px;">
            <summary style="cursor:pointer; font-weight:600;">A game time or location looks wrong &mdash; what do I do?</summary>
            <p style="margin:8px 0 0 0; color:#555;">This calendar pulls directly from Perfect Game. If something looks off, check the <a href="https://www.perfectgame.org" style="color:#1e6b3a;">PG website</a> first &mdash; if it's wrong there, contact your tournament director. If PG is correct but the calendar is wrong, let your team admin know so they can investigate.</p>
        </details>
        <details style="margin-bottom:12px;">
            <summary style="cursor:pointer; font-weight:600;">Is ntfy.sh free? Is it private?</summary>
            <p style="margin:8px 0 0 0; color:#555;">Yes, ntfy.sh is free and open-source. Topics are public by default &mdash; anyone who knows the topic name can subscribe. The topic names used here are specific enough that random discovery is unlikely, but notifications only contain schedule data (team names, dates, times, locations), not any personal information.</p>
        </details>
    </div>

    <div class="instructions" style="margin-top: 16px; background: #fff8e1; border-left: 4px solid #f9a825;">
        <h2 style="border:none; margin-top:0; color:#5d4037;">Disclaimer</h2>
        <p style="font-size:13px; color:#555; line-height:1.6;">
            This is an unofficial, volunteer-run tool. Schedule data is copied from
            <a href="https://www.perfectgame.org" style="color:#1e6b3a;">Perfect Game</a> and may
            be delayed, incomplete, or occasionally incorrect. <strong>Always confirm game times and
            locations with your coach or team admin before traveling.</strong> This site is not
            affiliated with or endorsed by Perfect Game, MDB, or any league organization.
            Notifications are provided on a best-effort basis &mdash; do not rely solely on push
            alerts for schedule changes.
        </p>
        <p style="font-size:13px; color:#555; line-height:1.6;">
            We're grateful to <a href="https://www.perfectgame.org" style="color:#1e6b3a; font-weight:600;">Perfect Game</a>
            for everything they do to support youth baseball. If you enjoy using this calendar,
            please consider supporting PG by attending their events, following them on social media,
            and spreading the word about the opportunities they create for young athletes.
        </p>
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

        function toggleSnackPicker(pickerId) {{
            const el = document.getElementById(pickerId);
            if (el) el.style.display = el.style.display === 'none' ? 'block' : 'none';
        }}

        function submitSnackSignup(pickerId, teamName) {{
            const picker = document.getElementById(pickerId);
            const dateSelect = picker.querySelector('.snack-date');
            const dateVal = dateSelect ? dateSelect.value : '';
            if (!dateVal) {{ alert('Please select a game date.'); return; }}

            const checked = picker.querySelectorAll('.snack-cb:checked');
            const families = Array.from(checked).map(cb => cb.value);

            const otherInput = picker.querySelector('.snack-other');
            if (otherInput && otherInput.value.trim()) {{
                otherInput.value.split(',').forEach(f => {{
                    const trimmed = f.trim();
                    if (trimmed) families.push(trimmed);
                }});
            }}

            if (families.length === 0) {{ alert('Please select at least one family.'); return; }}

            const familyStr = families.join(', ');
            const title = encodeURIComponent('[Snacks] Signup: ' + familyStr + ' - ' + dateVal);
            const body = encodeURIComponent(
                '### Team\\n\\n' + teamName +
                '\\n\\n### Game Date\\n\\n' + dateVal +
                '\\n\\n### Family Name(s)\\n\\n' + familyStr
            );
            const url = 'https://github.com/aknowles/milton-club-baseball/issues/new?labels=snack-signup&title=' + title + '&body=' + body;
            window.open(url, '_blank');
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
    rosters_by_team = {}

    for i, team in enumerate(teams):
        url = team["url"]
        name = team["team_name"]
        print(f"Fetching schedule for {name}...")

        try:
            html = fetch_page(url)
            games = parse_schedule(html, name, url)
            all_games.extend(games)

            # Also scrape roster from the same page
            roster = parse_roster(html, name)
            if roster:
                rosters_by_team[name] = roster
        except requests.RequestException as e:
            print(f"  ERROR fetching {name}: {e}")
            continue

        # Polite delay between requests
        if i < len(teams) - 1:
            print(f"  Waiting {REQUEST_DELAY}s before next request...")
            time.sleep(REQUEST_DELAY)

    # Add practice events from config
    practice_events = build_practice_events(config)
    if practice_events:
        print(f"Added {len(practice_events)} practice events from config")
        all_games.extend(practice_events)

    print(f"\nTotal events: {len(all_games)} (games + practices)")

    if not all_games:
        print("WARNING: No events found. Writing empty calendars.")

    # Group games by team and write per-team ICS files
    os.makedirs("calendars", exist_ok=True)
    games_by_team = {}
    for game in all_games:
        games_by_team.setdefault(game["team_name"], []).append(game)

    # Change detection and ntfy notifications
    old_snapshot = load_previous_snapshot()
    new_snapshot = build_snapshot(games_by_team)
    changes = detect_changes(old_snapshot, new_snapshot)
    if changes:
        print("\nSchedule changes detected:")
        for team_name, change_list in changes.items():
            for c in change_list:
                print(f"  [{team_name}] {c}")
        notify_changes(changes, config)
    else:
        print("\nNo schedule changes detected.")
    save_snapshot(new_snapshot)

    # Save rosters
    if rosters_by_team:
        save_rosters(rosters_by_team)
    else:
        print("No roster data found.")

    for tname, tgames in games_by_team.items():
        cal = make_calendar(tgames, config, cal_name=tname)
        filename = f"calendars/{team_slug(tname)}.ics"
        with open(filename, "wb") as f:
            f.write(cal.to_ical())
        print(f"Wrote {filename}")

    # Generate and write index.html
    index_html = generate_index_html(all_games, config, rosters_by_team)
    with open("index.html", "w") as f:
        f.write(index_html)
    print("Wrote index.html")

    print("Done!")


if __name__ == "__main__":
    main()
