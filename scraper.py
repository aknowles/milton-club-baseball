#!/usr/bin/env python3
"""
Perfect Game Baseball Schedule Scraper

Scrapes PGBA team schedule pages and generates a merged iCal calendar file
with lunch/snack signup and swap management.
Deployed via GitHub Actions to GitHub Pages.
"""

import hashlib
import json
import math
import os
import re
import sys
import time
from collections import OrderedDict
from datetime import datetime, date, timedelta
from urllib.parse import parse_qs, quote, urljoin, urlparse, unquote_plus

import requests
from bs4 import BeautifulSoup
from icalendar import Calendar, Event, vText

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TIMEZONE = "US/Eastern"
USER_AGENT = "Milton-Club-Baseball-Scraper/1.0 (GitHub Actions; +https://github.com/aknowles/milton-club-baseball)"
REQUEST_DELAY = 5.0  # seconds between requests – be polite
DEBUG = os.environ.get("SCRAPER_DEBUG", "0") == "1"


def debug_log(msg):
    """Print debug messages when SCRAPER_DEBUG=1."""
    if DEBUG:
        print(f"  [DEBUG] {msg}")


def load_config(path="config.json"):
    with open(path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Travel distance
# ---------------------------------------------------------------------------

GEOCODE_CACHE_PATH = "calendars/geocode_cache.json"


def haversine(lat1, lon1, lat2, lon2):
    """Return the great-circle distance in miles between two coordinates."""
    R = 3958.8  # Earth radius in miles
    lat1, lon1, lat2, lon2 = (math.radians(v) for v in (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def geocode_address(address):
    """Geocode an address string via Nominatim. Returns (lat, lon) or None."""
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "json", "limit": "1"},
            headers={"User-Agent": USER_AGENT},
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except (requests.RequestException, KeyError, ValueError, IndexError) as e:
        debug_log(f"Geocode failed for '{address}': {e}")
    return None


def load_geocode_cache():
    """Load the geocode cache from disk."""
    if os.path.exists(GEOCODE_CACHE_PATH):
        try:
            with open(GEOCODE_CACHE_PATH, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_geocode_cache(cache):
    """Write the geocode cache to disk."""
    with open(GEOCODE_CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)


def get_travel_distance(config, address, geocode_cache):
    """Return distance in miles from home_location to *address*, or None.

    Uses *geocode_cache* (dict) for lookups and stores new results in it.
    The caller is responsible for persisting the cache after all lookups.
    Returns a tuple of (distance_miles_or_None, did_geocode_bool).
    """
    home = config.get("home_location")
    if not home or not address:
        return None, False

    # Skip placeholder / meaningless addresses
    skip = {"tbd", "tba", ""}
    if address.strip().lower() in skip:
        return None, False

    # Check cache (None value means previously failed lookup)
    if address in geocode_cache:
        cached = geocode_cache[address]
        if cached is None:
            return None, False
        dist = haversine(home["lat"], home["lon"], cached["lat"], cached["lon"])
        return dist, False

    # Geocode the address
    result = geocode_address(address)
    if result:
        lat, lon = result
        geocode_cache[address] = {"lat": lat, "lon": lon}
        dist = haversine(home["lat"], home["lon"], lat, lon)
        return dist, True
    else:
        geocode_cache[address] = None
        return None, True


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def fetch_page(url, retries=2, backoff=3.0, session=None):
    """Fetch a page with a standard User-Agent and return the HTML text.

    Retries transient network errors (timeouts, connection errors) a few
    times with linear backoff before giving up. HTTP 4xx/5xx responses are
    raised immediately via raise_for_status.
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    getter = session.get if session is not None else requests.get
    last_exc = None
    for attempt in range(retries + 1):
        try:
            resp = getter(url, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.text
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if attempt < retries:
                wait = backoff * (attempt + 1)
                print(f"  transient fetch error ({e}); retrying in {wait:.0f}s...")
                time.sleep(wait)
                continue
            raise
    # Unreachable, but keep linters happy
    raise last_exc  # pragma: no cover


def fetch_team_schedule_html(url):
    """Fetch a Perfect Game team schedule page, expanding all event sections.

    Perfect Game's team page is an ASP.NET WebForms page whose event
    sections are collapsed by default. A team in multiple events (e.g. a
    tournament + a league) only has one event's nested rgEvent table
    rendered in the initial HTML; the rest are revealed by clicking
    "See All Games", which fires a __doPostBack to the server.

    We detect the collapsed condition (more event-header links than
    nested rgEvent tables) and replay that postback ourselves, reusing
    the session cookies and the __VIEWSTATE / __VIEWSTATEGENERATOR
    hidden fields from the initial GET. If anything fails along the way
    we fall back to the original HTML so single-event teams are
    unaffected.
    """
    session = requests.Session()
    html = fetch_page(url, session=session)

    soup = BeautifulSoup(html, "html.parser")
    event_links = soup.find_all(
        "a", href=re.compile(r"/events/Default\.aspx\?event=", re.I)
    )
    event_tables = soup.find_all("table", id=re.compile(r"rgEvent", re.I))

    # If the page already renders at least as many rgEvent tables as it
    # advertises event-header links, nothing is collapsed — return as-is.
    if len(event_links) <= max(1, len(event_tables)):
        return html

    # Locate the "See All Games" anchor and pull its __doPostBack target
    target = None
    for a in soup.find_all("a"):
        if a.get_text(strip=True).lower() != "see all games":
            continue
        m = re.search(
            r"__doPostBack\(['\"]([^'\"]+)['\"]",
            a.get("href", "") or a.get("onclick", ""),
        )
        if m:
            target = m.group(1)
            break
    if not target:
        print("  [expand] no 'See All Games' postback target found; skipping expand")
        return html

    # Locate the enclosing <form> and harvest every form field so our POST
    # looks like a real browser submission. Missing fields are the most
    # common reason an ASP.NET postback no-ops silently.
    form = soup.find("form")
    if form is None:
        print("  [expand] no <form> element found; skipping expand")
        return html

    form_data = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        itype = (inp.get("type") or "text").lower()
        if itype in ("submit", "button", "image", "reset"):
            continue
        if itype in ("checkbox", "radio"):
            if inp.has_attr("checked"):
                form_data[name] = inp.get("value", "on")
            continue
        form_data[name] = inp.get("value", "") or ""
    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        selected = sel.find("option", selected=True) or sel.find("option")
        if selected is not None:
            form_data[name] = selected.get("value", selected.get_text(strip=True))
    for ta in form.find_all("textarea"):
        name = ta.get("name")
        if name:
            form_data[name] = ta.get_text() or ""

    if "__VIEWSTATE" not in form_data:
        print("  [expand] no __VIEWSTATE in initial page; skipping expand")
        return html

    form_data["__EVENTTARGET"] = target
    form_data["__EVENTARGUMENT"] = ""

    # Resolve the form's action URL (usually "./default.aspx?..." which is
    # equivalent to the team URL).
    post_url = urljoin(url, form.get("action") or url)

    print(
        f"  [expand] replaying See All Games postback "
        f"(target={target}, fields={len(form_data)}, post_url={post_url})"
    )
    time.sleep(1.0)  # polite pause between GET and follow-up POST

    post_headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": url,
        "Origin": "https://www.perfectgame.org",
    }
    try:
        resp = session.post(post_url, data=form_data, headers=post_headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [expand] postback failed: {e} — using initial HTML")
        return html

    expanded_html = resp.text
    # Sanity check: confirm the postback actually expanded something.
    new_soup = BeautifulSoup(expanded_html, "html.parser")
    new_event_tables = new_soup.find_all("table", id=re.compile(r"rgEvent", re.I))
    new_game_links = new_soup.find_all(
        "a", href=re.compile(r"DiamondKast/Game\.aspx\?gameid=", re.I)
    )
    # Also look at whether the "Hide All Games" link is now present and
    # "See All Games" isn't — another reliable signal that the server
    # actually toggled the state.
    has_hide = bool(
        new_soup.find(lambda t: t.name == "a"
                      and t.get_text(strip=True).lower() == "hide all games")
    )
    has_see = bool(
        new_soup.find(lambda t: t.name == "a"
                      and t.get_text(strip=True).lower() == "see all games")
    )
    print(
        f"  [expand] post-expand rgEvent={len(new_event_tables)} "
        f"game_links={len(new_game_links)} "
        f"has_hide_link={has_hide} has_see_link={has_see} "
        f"bytes={len(expanded_html)}"
    )
    if len(new_event_tables) <= len(event_tables):
        print("  [expand] postback did not increase event tables — keeping initial HTML")
        return html
    return expanded_html


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

    # One-line structural diagnostics — helps identify teams whose schedule
    # page has more events than rendered game tables (typically caused by
    # collapsed event sections in Perfect Game's RadGrid, which
    # fetch_team_schedule_html automatically expands via postback).
    _diag_sched_tables = soup.find_all("table", id=re.compile(r"rgSchedule", re.I))
    _diag_event_tables = soup.find_all("table", id=re.compile(r"rgEvent", re.I))
    _diag_event_links = soup.find_all(
        "a", href=re.compile(r"/events/Default\.aspx\?event=", re.I)
    )
    _diag_game_links = soup.find_all(
        "a", href=re.compile(r"DiamondKast/Game\.aspx\?gameid=", re.I)
    )
    print(
        f"  [diag] {team_name}: rgSchedule={len(_diag_sched_tables)} "
        f"rgEvent={len(_diag_event_tables)} "
        f"event_links={len(_diag_event_links)} "
        f"game_links={len(_diag_game_links)}"
    )

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

    # --- Step 2: Walk all rows of the outer schedule table, tracking the
    # current event/league context as we go. Teams that play in multiple
    # leagues or tournaments have multiple event-header rows, each followed
    # by their own nested rgEvent table of game rows. We visit every <tr>
    # descendant in document order, skip wrapper rows that merely contain
    # a nested rgEvent table (their inner rows are visited separately),
    # update the event context when we see a header row, and otherwise try
    # to parse the row as a game.
    current_event = "Unknown Event"
    current_event_url = None

    def _resolve_event_url(href):
        if not href:
            return None
        if href.startswith("/"):
            return f"https://www.perfectgame.org{href}"
        if href.startswith("http"):
            return href
        return f"https://www.perfectgame.org/{href}"

    all_rows = schedule_table.find_all("tr")
    debug_log(f"Walking {len(all_rows)} rows in rgSchedule")

    for row_idx, row in enumerate(all_rows):
        # Skip wrapper rows containing a nested rgEvent table — the inner
        # <tr>s are enumerated separately by find_all("tr").
        if row.find("table", id=re.compile(r"rgEvent", re.I)):
            # But still pick up an event header link if it lives in this row.
            ev_link = row.find("a", href=re.compile(r"/events/Default\.aspx\?event=", re.I))
            if ev_link:
                current_event = ev_link.get_text(strip=True)
                current_event_url = _resolve_event_url(ev_link.get("href", ""))
                debug_log(f"Event: '{current_event}' URL: {current_event_url}")
            continue

        cells = row.find_all("td", recursive=False)
        if not cells:
            continue

        # Use " " as the separator so PG's frequent <br> tags become real
        # whitespace in the extracted text. Without this, BeautifulSoup
        # squashes "W, 10-16<br>vs. Opponent" down to "W,10-16vs.Opponent",
        # which breaks the trailing word boundary on the score regex and
        # also fuses date parts ("Apr<br>11<br>Sat" → "Apr11Sat").
        cell_texts = [c.get_text(" ", strip=True) for c in cells]
        full_text = " ".join(cell_texts)
        if not full_text.strip():
            continue

        # Event header row: update context and move on.
        ev_link = row.find("a", href=re.compile(r"/events/Default\.aspx\?event=", re.I))
        if ev_link:
            current_event = ev_link.get_text(strip=True)
            current_event_url = _resolve_event_url(ev_link.get("href", ""))
            debug_log(f"Event: '{current_event}' URL: {current_event_url}")
            continue

        if DEBUG and row_idx < 60:
            debug_log(f"  Row[{row_idx}] cells={len(cells)}: {[t[:60] for t in cell_texts]}")

        # Check if this row has a DiamondKast game link (indicates a game row)
        game_link = row.find("a", href=re.compile(r"DiamondKast/Game\.aspx\?gameid=", re.I))

        game = parse_game_row(cells, cell_texts, full_text, game_link,
                              current_event, current_event_url, team_name, team_url)
        if game:
            games.append(game)
            debug_log(f"  -> Parsed game: {game['title']} on {game['date']} [{current_event}]")

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
    # Match a standalone "@" (as a word — preceded and followed by non-word
    # characters). \b@\b doesn't work because \b requires a word/non-word
    # boundary, and "@" is non-word on both sides of any surrounding space.
    if re.search(r"\bvs\.?\b", full_text, re.I):
        home_away = "vs."
    elif re.search(r"(?<!\w)@(?!\w)", full_text):
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

    # --- Score parsing ---
    # Perfect Game formats completed-game scores as "W, 10-16 vs. Opp" or
    # "L, 3-7 @ Opp" — letter result, comma, then *visitor score - home
    # score* (regardless of which side is the subject team). We require the
    # W/L/T prefix to avoid matching incidental dashes (time ranges like
    # "3:00 PM - 5:00 PM", age divisions like "9-10U", W-L-T records like
    # "(3-5-0)", doubleheader labels, etc.), and we flip the score based on
    # the home/away context so the displayed score reads "ours-theirs".
    score = None
    score_result = None
    score_match = re.search(
        r"(?:^|\s)([WLT])[,\s]+(\d{1,2})\s*[-–]\s*(\d{1,2})\b", full_text
    )
    if score_match:
        score_result = score_match.group(1).upper()
        runs_visitor = int(score_match.group(2))
        runs_home = int(score_match.group(3))
        if home_away == "@":
            runs_for, runs_against = runs_visitor, runs_home
        else:
            runs_for, runs_against = runs_home, runs_visitor
        score = f"{runs_for}-{runs_against}"
        debug_log(f"  Score found: {score} ({score_result})")

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
        "score": score,
        "score_result": score_result,
    }


def parse_roster(html, team_name):
    """
    Parse the TEAM ROSTER section of a Perfect Game team page.

    The roster is in a Telerik RadGrid with ID containing 'rgRoster'.
    Returns a list of player dicts with keys like 'name', 'number', 'position'.
    """
    soup = BeautifulSoup(html, "html.parser")
    players = []

    # Find the roster table — try multiple patterns used by PG
    roster_table = soup.find("table", id=re.compile(r"rgRoster.*_ctl00$", re.I))
    if not roster_table:
        roster_table = soup.find("table", id=re.compile(r"rgRoster", re.I))
    if not roster_table:
        # Try any table with "Roster" in an ancestor's text or nearby heading
        for heading in soup.find_all(["h2", "h3", "h4", "span", "div", "label"]):
            txt = heading.get_text(strip=True).lower()
            if "roster" in txt and "schedule" not in txt:
                roster_table = heading.find_next("table")
                if roster_table:
                    debug_log(f"[roster] Found roster table via heading for {team_name}")
                    break
    if not roster_table:
        # Try any RadGrid table that isn't the schedule
        for tbl in soup.find_all("table", id=re.compile(r"_ctl00$", re.I)):
            tid = tbl.get("id", "")
            if "schedule" not in tid.lower() and "event" not in tid.lower():
                roster_table = tbl
                debug_log(f"[roster] Using fallback RadGrid table for {team_name}: id='{tid}'")
                break
    if not roster_table:
        # Log all table IDs to help debug
        all_tables = soup.find_all("table", id=True)
        table_ids = [t.get("id", "") for t in all_tables]
        print(f"  [roster] No roster table found for {team_name}. Table IDs on page: {table_ids}")
        return players

    debug_log(f"[roster] Found roster table for {team_name}: id='{roster_table.get('id', '')}'")

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

    debug_log(f"Found {len(players)} roster players for {team_name}")
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

def get_game_result(config, team_name, event_date):
    """Return game result ('W', 'L', 'T', or None) for a given team and date."""
    results_cfg = config.get("game_results", {})
    team_results = results_cfg.get(team_name, [])
    if isinstance(event_date, datetime):
        check_date = event_date.strftime("%Y-%m-%d")
    elif isinstance(event_date, date):
        check_date = event_date.strftime("%Y-%m-%d")
    else:
        return None

    for entry in team_results:
        if entry.get("date") == check_date:
            return entry.get("result")  # "W", "L", or "T"
    return None


def get_game_override(config, team_name, event_date):
    """Return game override dict for a given team and date, or None.

    Override entries live under config["game_overrides"][team_name] and look
    like: {"date": "YYYY-MM-DD", "status": "postponed"|"cancelled",
           "reason": "optional explanation"}
    """
    overrides_cfg = config.get("game_overrides", {})
    team_overrides = overrides_cfg.get(team_name, [])
    if isinstance(event_date, datetime):
        check_date = event_date.strftime("%Y-%m-%d")
    elif isinstance(event_date, date):
        check_date = event_date.strftime("%Y-%m-%d")
    else:
        return None

    for entry in team_overrides:
        if entry.get("date") == check_date:
            return entry
    return None


def get_event_emoji(game, config=None, is_past=False):
    """Return emoji prefix for an event based on type and result.

    - Practice: 🏋️
    - Game (postponed): ⚠️
    - Game (cancelled): 🚫
    - Game (upcoming): ⚾
    - Game (past, won): ✅
    - Game (past, lost): ❌
    - Game (past, tied): 🤝
    - Game (past, no result): ⚾
    """
    if game.get("is_practice"):
        return "🏋️"

    # Check for manual override (postponed / cancelled)
    if config:
        override = get_game_override(config, game["team_name"], game["date"])
        if override:
            status = override.get("status", "").lower()
            if status == "postponed":
                return "⚠️"
            elif status == "cancelled":
                return "🚫"

    if is_past:
        # Check scraped score first, then fall back to config
        result = game.get("score_result")
        if not result and config:
            result = get_game_result(config, game["team_name"], game["date"])
        if result == "W":
            return "✅"
        elif result == "L":
            return "❌"
        elif result == "T":
            return "🤝"

    return "⚾"


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


def load_previous_snapshot(path="calendars/snapshot.json"):
    """Load the previous calendar snapshot from a local file.

    In CI this file is restored from the GitHub Actions cache before the
    scraper runs, so we just read it from disk.  Falls back to an empty
    dict when the file doesn't exist (first run / cache miss).
    """
    if not os.path.exists(path):
        print(f"No previous snapshot found at {path} — first run or cache miss.")
        return {}

    try:
        with open(path, "r") as f:
            raw = f.read()
        snapshot = json.loads(raw)
    except (json.JSONDecodeError, OSError) as e:
        print(f"Could not read previous snapshot: {e}")
        return {}

    total_events = sum(len(v) for v in snapshot.values()) if snapshot else 0
    print(f"Loaded previous snapshot: {len(snapshot)} teams, {total_events} events")

    # Normalise opponent field ("" → "TBD") for consistent comparison.
    for team, events in snapshot.items():
        for uid, data in events.items():
            if not data.get("opponent"):
                data["opponent"] = "TBD"

    return snapshot


def save_snapshot(snapshot, path="calendars/snapshot.json"):
    """Save the current calendar snapshot."""
    with open(path, "w") as f:
        json.dump(snapshot, f, indent=2, default=str)


def build_snapshot(games_by_team, config=None):
    """Build a dict snapshot of games keyed by team, then by uid.

    UIDs are based on date + opponent only (not time), so that time changes
    are detected as modifications rather than remove + add pairs.  When
    multiple games share the same date + opponent, a sequence suffix is
    appended to keep UIDs unique.
    """
    snapshot = {}
    for team_name, games in games_by_team.items():
        team_events = {}
        # Count occurrences of each base key to disambiguate duplicates
        key_counts = {}
        for g in games:
            date_str = g["date"].strftime("%Y-%m-%d")
            opponent = g.get("opponent") or "TBD"
            base_key = f"{date_str}-{opponent}"
            key_counts[base_key] = key_counts.get(base_key, 0) + 1
            seq = key_counts[base_key]
            uid = base_key if seq == 1 else f"{base_key}-{seq}"
            time_str = g["time"].strftime("%I:%M %p") if g.get("time") else "TBD"
            override_status = ""
            if config is not None and not g.get("is_practice"):
                override = get_game_override(config, team_name, g["date"])
                if override:
                    override_status = (override.get("status") or "").lower()
            team_events[uid] = {
                "title": g["title"],
                "date": date_str,
                "time": time_str,
                "location": g.get("location") or "",
                "opponent": opponent,
                "override_status": override_status,
            }
        snapshot[team_name] = team_events
    return snapshot


def detect_changes(old_snapshot, new_snapshot):
    """
    Compare old and new snapshots. Returns a tuple of:
      (changes, reschedules)
    where *changes* is a dict of team_name -> list of change description
    strings and *reschedules* is a list of
    ``{"team": str, "old_date": str, "new_date": str, "opponent": str}``
    dicts representing games that moved to a different date (same opponent).
    """
    today_str = date.today().strftime("%Y-%m-%d")
    changes = {}
    reschedules = []
    all_teams = set(list(old_snapshot.keys()) + list(new_snapshot.keys()))

    for team in all_teams:
        old_events = old_snapshot.get(team, {})
        new_events = new_snapshot.get(team, {})
        team_changes = []

        added_uids = [uid for uid in new_events if uid not in old_events]
        removed_uids = [uid for uid in old_events if uid not in new_events]

        # Try to match removed + added on the same date as modifications
        # (e.g. opponent changed from TBD to a real name)
        matched_adds = set()
        matched_removes = set()
        for r_uid in removed_uids:
            old_e = old_events[r_uid]
            if old_e["date"] < today_str:
                matched_removes.add(r_uid)  # skip past games silently
                continue
            for a_uid in added_uids:
                if a_uid in matched_adds:
                    continue
                new_e = new_events[a_uid]
                if old_e["date"] == new_e["date"]:
                    # Same date — treat as a modification
                    diffs = []
                    old_opp = old_e.get("opponent") or "TBD"
                    new_opp = new_e.get("opponent") or "TBD"
                    if old_opp != new_opp:
                        diffs.append(f"opponent {old_opp} -> {new_opp}")
                    if old_e.get("time") != new_e.get("time"):
                        diffs.append(f"time {old_e['time']} -> {new_e['time']}")
                    if old_e.get("location") != new_e.get("location"):
                        diffs.append(f"location -> {new_e['location']}")
                    if diffs:
                        team_changes.append(
                            f"Changed: {new_e['title']} on {new_e['date']} ({', '.join(diffs)})"
                        )
                    matched_adds.add(a_uid)
                    matched_removes.add(r_uid)
                    break

        # Match removed + added with the same opponent but different date
        # as reschedules (game moved to a new date).
        unmatched_removes = [u for u in removed_uids if u not in matched_removes]
        unmatched_adds = [u for u in added_uids if u not in matched_adds]
        for r_uid in unmatched_removes:
            old_e = old_events[r_uid]
            old_opp = (old_e.get("opponent") or "TBD").strip().lower()
            if old_opp == "tbd":
                continue  # can't reliably match TBD opponents
            for a_uid in unmatched_adds:
                if a_uid in matched_adds:
                    continue
                new_e = new_events[a_uid]
                new_opp = (new_e.get("opponent") or "TBD").strip().lower()
                if old_opp == new_opp and old_e["date"] != new_e["date"]:
                    # Same opponent, different date — reschedule
                    team_changes.append(
                        f"Rescheduled: {new_e['title']} moved from "
                        f"{old_e['date']} to {new_e['date']}"
                    )
                    reschedules.append({
                        "team": team,
                        "old_date": old_e["date"],
                        "new_date": new_e["date"],
                        "opponent": new_e.get("opponent") or old_e.get("opponent"),
                    })
                    matched_adds.add(a_uid)
                    matched_removes.add(r_uid)
                    break

        # Remaining new games (truly new, not just modified)
        for uid in added_uids:
            if uid in matched_adds:
                continue
            e = new_events[uid]
            if e["date"] < today_str:
                continue  # skip past games
            team_changes.append(
                f"New: {e['title']} on {e['date']} at {e['time']}"
            )

        # Remaining removed games (truly removed, not just modified)
        for uid in removed_uids:
            if uid in matched_removes:
                continue
            e = old_events[uid]
            if e["date"] < today_str:
                continue  # skip past games
            team_changes.append(
                f"Removed: {e['title']} on {e['date']}"
            )

        # Changed games (same uid, different details)
        for uid in new_events:
            if uid in old_events:
                old_e = old_events[uid]
                new_e = new_events[uid]
                if new_e["date"] < today_str:
                    continue  # skip past games
                old_status = (old_e.get("override_status") or "").lower()
                new_status = (new_e.get("override_status") or "").lower()
                if old_status != new_status:
                    if new_status == "cancelled":
                        team_changes.append(
                            f"Cancelled: {new_e['title']} on {new_e['date']}"
                        )
                    elif new_status == "postponed":
                        team_changes.append(
                            f"Postponed: {new_e['title']} on {new_e['date']}"
                        )
                    elif not new_status and old_status in ("cancelled", "postponed"):
                        team_changes.append(
                            f"Reinstated: {new_e['title']} on {new_e['date']} at {new_e['time']}"
                        )
                diffs = []
                if old_e.get("time") != new_e.get("time"):
                    diffs.append(f"time {old_e['time']} -> {new_e['time']}")
                if old_e.get("location") != new_e.get("location"):
                    diffs.append(f"location -> {new_e['location']}")
                if diffs:
                    team_changes.append(
                        f"Changed: {new_e['title']} on {new_e['date']} ({', '.join(diffs)})"
                    )

        # Deduplicate identical lines (e.g. doubleheaders both showing "at TBD")
        seen = set()
        deduped = []
        for c in team_changes:
            if c not in seen:
                seen.add(c)
                deduped.append(c)
            else:
                # Mark existing entry as doubleheader
                for i, d in enumerate(deduped):
                    if d == c and "(DH)" not in d:
                        deduped[i] = d.replace("New: ", "New: (DH) ").replace("Removed: ", "Removed: (DH) ").replace("Changed: ", "Changed: (DH) ").replace("Rescheduled: ", "Rescheduled: (DH) ").replace("Cancelled: ", "Cancelled: (DH) ").replace("Postponed: ", "Postponed: (DH) ").replace("Reinstated: ", "Reinstated: (DH) ")
                        break

        if deduped:
            changes[team] = deduped

    return changes, reschedules


def migrate_snack_assignments(config, reschedules):
    """Move snack assignments from old dates to new dates for rescheduled games.

    Updates config["snacks"] in place. Returns a list of human-readable
    descriptions of migrations performed.
    """
    snacks_cfg = config.get("snacks", {})
    log = []
    for rs in reschedules:
        team = rs["team"]
        old_date = rs["old_date"]
        new_date = rs["new_date"]
        team_snacks = snacks_cfg.get(team, [])

        # Find the entry for the old date
        old_entry = None
        for entry in team_snacks:
            if entry.get("date") == old_date:
                old_entry = entry
                break

        if not old_entry:
            continue  # no snack assignment on the old date

        families = old_entry["families"]

        # Check if the new date already has an assignment
        new_entry = None
        for entry in team_snacks:
            if entry.get("date") == new_date:
                new_entry = entry
                break

        if new_entry:
            # New date already has assignments — don't overwrite
            log.append(
                f"  [{team}] Skipped snack migration {old_date} -> {new_date}: "
                f"new date already has assignments ({', '.join(new_entry['families'])})"
            )
            continue

        # Move the assignment: update the date on the existing entry
        old_entry["date"] = new_date
        log.append(
            f"  [{team}] Migrated snack assignment {old_date} -> {new_date} "
            f"({', '.join(families)})"
        )

    return log


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
    """Send ntfy notifications for detected schedule changes.

    Respects the ``notify_level`` setting in config.json:
      - "all"       — notify on any change (default)
      - "important" — only new games and cancellations, skip minor updates
                      like time tweaks or location text changes
      - "none"      — suppress all push notifications
    """
    notify_level = config.get("notify_level", "all")
    if notify_level == "none":
        print("Notifications suppressed (notify_level=none)")
        return

    team_topics = {}
    for team in config.get("teams", []):
        if team.get("ntfy_topic"):
            team_topics[team["team_name"]] = team["ntfy_topic"]

    for team_name, change_list in changes.items():
        topic = team_topics.get(team_name)
        if not topic:
            continue

        if notify_level == "important":
            # Only keep new and removed games; drop minor "Changed:" updates
            change_list = [c for c in change_list if not c.startswith("Changed:")]

        if not change_list:
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
        is_past = game["date"] < datetime.now()
        emoji = get_event_emoji(game, config=config, is_past=is_past)
        override = get_game_override(config, game["team_name"], game["date"])
        override_status = override.get("status", "").upper() if override else ""
        summary_suffix = f" [{override_status}]" if override_status else ""
        event.add("summary", f"{emoji} {game['title']}{summary_suffix}")

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
        if override_status:
            reason = override.get("reason", "")
            label = override_status.capitalize()
            desc_parts.append(f"⚠️ {label}" + (f": {reason}" if reason else ""))
        if game.get("score"):
            result_word = {"W": "Win", "L": "Loss", "T": "Tie"}.get(game.get("score_result"), "")
            desc_parts.append(f"Result: {result_word} {game['score']}")
        if not game.get("is_practice"):
            if game.get("home_away") == "@":
                desc_parts.append("🟤 Wear dark jerseys (away)")
            else:
                desc_parts.append("⚪ Wear white jersey (home)")
        if game.get("event_name") and game["event_name"] != "Practice":
            desc_parts.append(f"Tournament: {game['event_name']}")
        if game.get("event_url"):
            desc_parts.append(f"Event: {game['event_url']}")
        if game.get("team_url"):
            desc_parts.append(f"Team: {game['team_url']}")

        # Travel distance
        travel_threshold = config.get("travel_threshold_miles", 40)
        travel_miles = game.get("travel_miles")
        if travel_miles is not None and travel_miles >= travel_threshold:
            home_name = config.get("home_location", {}).get("name", "home")
            desc_parts.append(f"\U0001F697 Travel: ~{round(travel_miles)} miles from {home_name}")

        # Append snack signup info
        snack_families = get_snack_families(config, game["team_name"], game["date"])
        if snack_families:
            desc_parts.append(f"Lunch/Snacks: {', '.join(snack_families)}")

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

        # iCal STATUS + SEQUENCE so calendar clients reflect overrides
        # (strikethrough for CANCELLED, tentative styling for postponed) and
        # refresh cached event titles instead of keeping stale summaries.
        if override_status == "CANCELLED":
            event.add("status", "CANCELLED")
            event.add("sequence", 2)
        elif override_status == "POSTPONED":
            event.add("status", "TENTATIVE")
            event.add("sequence", 1)
        else:
            event.add("sequence", 0)

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

    # Build lookup for team config
    team_configs = {t["team_name"]: t for t in config.get("teams", [])}

    # Build team sections
    team_sections = ""
    for team_name, games in sorted(teams.items()):
        upcoming = [g for g in games if g["date"].date() >= date.today()]
        upcoming.sort(key=lambda g: g["date"])

        # Check snack signup config for this team (needed in the games loop below)
        snack_signup_enabled = team_configs.get(team_name, {}).get("snack_signup", False)

        games_html = ""
        snack_dates_shown = set()

        # Show recent past games with results (last 3)
        past = [g for g in games if g["date"] < datetime.now() and not g.get("is_practice")]
        past.sort(key=lambda g: g["date"], reverse=True)
        past_with_results = [g for g in past if g.get("score_result") or get_game_result(config, team_name, g["date"])]
        for g in past_with_results[:3]:
            date_str = g["date"].strftime("%b %d")
            emoji = get_event_emoji(g, config=config, is_past=True)
            result = g.get("score_result") or get_game_result(config, team_name, g["date"])
            result_label = {"W": "Win", "L": "Loss", "T": "Tie"}.get(result, "")
            score_str = f" ({g['score']})" if g.get("score") else ""
            past_link = g.get("game_url") or g.get("event_url") or g.get("team_url") or ""
            past_opp = g.get('opponent', 'TBD')
            past_matchup = f'{g["home_away"]} <a href="{past_link}" target="_blank" class="game-link">{past_opp}</a>' if past_link else f'{g["home_away"]} {past_opp}'
            games_html += f"""
                <div class="game-row game-past">
                    <span class="game-emoji">{emoji}</span>
                    <span class="game-date">{date_str}</span>
                    <span class="game-matchup">{past_matchup}</span>
                    <span class="game-result-label">{result_label}{score_str}</span>
                </div>"""

        # Group upcoming games by date for doubleheader collapsing
        games_by_date = OrderedDict()
        for g in upcoming:
            date_key = g["date"].strftime("%Y-%m-%d")
            games_by_date.setdefault(date_key, []).append(g)

        for date_key, day_games in games_by_date.items():
            first = day_games[0]
            date_str = first["date"].strftime("%b %d")
            emoji = get_event_emoji(first, config=config, is_past=False)
            is_dh = len(day_games) > 1

            if is_dh:
                # Collapse doubleheader into one row
                times = []
                for g in day_games:
                    t = g["time"].strftime("%I:%M %p").lstrip("0") if g.get("time") else "TBD"
                    times.append(t)
                time_str = " / ".join(times)
                # Use location from first game (usually same venue for DH)
                loc_str = first.get("field_name") or first.get("location") or ""
                opponent = first.get("opponent", "TBD")
                dh_label = f'<span class="dh-tag" title="Doubleheader — {len(day_games)} games on this day">DH</span>'
            else:
                g = first
                time_str = g["time"].strftime("%I:%M %p").lstrip("0") if g.get("time") else "TBD"
                loc_str = g.get("field_name") or g.get("location") or ""
                opponent = g.get("opponent", "TBD")
                dh_label = ""

            # Show snack tag once per day
            snack_tag = ""
            if snack_signup_enabled and date_key not in snack_dates_shown:
                snack_families = get_snack_families(config, team_name, first["date"])
                if snack_families:
                    snack_tag = f'<span class="snack-tag">Lunch/Snacks: {", ".join(snack_families)}</span>'
                else:
                    snack_tag = '<span class="snack-tag snack-needed">Needs lunch/snacks</span>'
                snack_dates_shown.add(date_key)
            elif not snack_signup_enabled and date_key not in snack_dates_shown:
                snack_families = get_snack_families(config, team_name, first["date"])
                if snack_families:
                    snack_tag = f'<span class="snack-tag">Lunch/Snacks: {", ".join(snack_families)}</span>'
                    snack_dates_shown.add(date_key)

            # Travel distance tag
            travel_tag = ""
            travel_threshold = config.get("travel_threshold_miles", 40)
            travel_miles = first.get("travel_miles")
            if travel_miles is not None and travel_miles >= travel_threshold:
                home_name = config.get("home_location", {}).get("name", "home")
                travel_tag = f'<span class="travel-tag" title="~{round(travel_miles)} miles from {home_name}">\U0001F697 ~{round(travel_miles)} mi</span>'

            # Check for game override (postponed / cancelled)
            override = get_game_override(config, team_name, first["date"])
            status_tag = ""
            row_class = "game-row"
            if override:
                status = override.get("status", "").lower()
                reason = override.get("reason", "")
                if status == "postponed":
                    label = "POSTPONED"
                    tooltip = reason if reason else "This game has been postponed"
                    if reason:
                        label += f": {reason}"
                    status_tag = f'<span class="status-tag status-postponed" title="{tooltip}">{label}</span>'
                    row_class = "game-row game-postponed"
                elif status == "cancelled":
                    label = "CANCELLED"
                    tooltip = reason if reason else "This game has been cancelled"
                    if reason:
                        label += f": {reason}"
                    status_tag = f'<span class="status-tag status-cancelled" title="{tooltip}">{label}</span>'
                    row_class = "game-row game-cancelled"

            # Pick the best PG link: game-specific > event > team page
            pg_link = first.get("game_url") or first.get("event_url") or first.get("team_url") or ""

            if pg_link:
                matchup_html = f'{first["home_away"]} <a href="{pg_link}" target="_blank" class="game-link">{opponent}</a> {dh_label}'
            else:
                matchup_html = f'{first["home_away"]} {opponent} {dh_label}'

            # Jersey color tag
            jersey_tag = ""
            if not first.get("is_practice"):
                if first.get("home_away") == "@":
                    jersey_tag = '<span class="jersey-tag jersey-dark" title="Away game — dark jerseys">🟤 Dark</span>'
                else:
                    jersey_tag = '<span class="jersey-tag jersey-white" title="Home game — white jersey">⚪ White</span>'

            games_html += f"""
                <div class="{row_class}">
                    <span class="game-emoji">{emoji}</span>
                    <span class="game-date">{date_str}</span>
                    <span class="game-time">{time_str}</span>
                    <span class="game-matchup">{matchup_html}</span>
                    <span class="game-location">{loc_str}</span>
                    {jersey_tag}
                    {travel_tag}
                    {status_tag}
                    {snack_tag}
                </div>"""

        event_label = ""
        if games and games[0].get("event_name"):
            event_label = f'<span class="event-tag">{games[0]["event_name"]}</span>'

        # Get roster data for snack picker (not displayed on page)
        team_roster = rosters_by_team.get(team_name, [])

        slug = team_slug(team_name)
        team_cal_url = f"{base_url}/calendars/{slug}.ics"

        # Build snack signup picker (only for teams with snack_signup enabled)
        snack_signup_enabled = team_configs.get(team_name, {}).get("snack_signup", False)
        snack_button_html = ""
        snack_picker_html = ""
        swap_picker_html = ""
        picker_id = f"snack-picker-{slug}"
        if snack_signup_enabled:
            family_names = sorted(set(
                p.get("name", "").strip().split()[-1]
                for p in team_roster
                if p.get("name", "").strip()
            ))
            cbs_html = ""
            if family_names:
                cbs = ""
                for fam in family_names:
                    cbs += f"""
                            <label style="display:inline-block; margin:3px 10px 3px 0; cursor:pointer;">
                                <input type="checkbox" class="snack-cb" data-picker="{picker_id}" value="{fam}"> {fam}
                            </label>"""
                cbs_html = f"""
                        <div style="margin:6px 0; font-size:13px;">
                            <label style="font-size:12px; color:#555;">Family:</label><br>
                            {cbs}
                        </div>"""

            date_options = ""
            seen_dates = set()
            for g in upcoming:
                d = g["date"].strftime("%Y-%m-%d")
                if d not in seen_dates:
                    label = g["date"].strftime("%b %d")
                    date_options += f'<option value="{d}">{label}</option>'
                    seen_dates.add(d)

            family_input_label = "Other (not listed above):" if family_names else "Family Name(s):"
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
                        {cbs_html}
                        <div style="margin:6px 0; font-size:13px;">
                            <label style="font-size:12px; color:#555;">{family_input_label}</label><br>
                            <input type="text" class="snack-other" data-picker="{picker_id}" placeholder="e.g. Smith, Jones" style="padding:3px 6px; font-size:13px; width:200px; margin-top:2px;">
                        </div>
                        <button class="btn btn-snack" style="font-size:12px; padding:4px 12px; margin-top:4px;" onclick="submitSnackSignup('{picker_id}', '{team_name}')">Submit Signup</button>
                    </div>"""
            snack_button_html = f'<button class="btn btn-snack" onclick="toggleSnackPicker(\'{picker_id}\')">Sign Up for Snacks</button>'

            # Build swap picker with cascading dropdowns
            swap_picker_id = f"swap-picker-{slug}"
            family_options = ""
            for fam in family_names:
                family_options += f'<option value="{fam}">{fam}</option>'
            swap_picker_html = f"""
                    <div class="snack-picker" id="{swap_picker_id}" style="display:none; background:#f0f4fe; border:1px solid #3b82f6; border-radius:6px; padding:10px 12px; margin-bottom:12px;">
                        <strong style="font-size:13px;">Swap Lunch Day</strong>
                        <div style="margin:6px 0;">
                            <label style="font-size:12px; color:#555;">Your Family Name:</label>
                            <select class="swap-your-family" data-picker="{swap_picker_id}" data-team="{team_name}" style="margin-left:4px; padding:2px 6px; font-size:13px;" onchange="onSwapFamilyChange(this)">
                                <option value="">Select family...</option>
                                {family_options}
                            </select>
                        </div>
                        <div style="margin:6px 0;">
                            <label style="font-size:12px; color:#555;">Date:</label>
                            <select class="swap-new-date" data-picker="{swap_picker_id}" style="margin-left:4px; padding:2px 6px; font-size:13px;" onchange="onSwapNewDateChange(this)" disabled>
                                <option value="">Select date...</option>
                            </select>
                        </div>
                        <div style="margin:6px 0;">
                            <label style="font-size:12px; color:#555;">Replacing:</label>
                            <select class="swap-with-family" data-picker="{swap_picker_id}" style="margin-left:4px; padding:2px 6px; font-size:13px;" disabled>
                                <option value="">Select family...</option>
                            </select>
                        </div>
                        <div style="margin:6px 0;">
                            <label style="font-size:12px; color:#555;">They take your date:</label>
                            <select class="swap-current-date" data-picker="{swap_picker_id}" style="margin-left:4px; padding:2px 6px; font-size:13px;" disabled>
                                <option value="">None (no return swap)</option>
                            </select>
                            <div style="font-size:11px; color:#888; margin-top:2px;">Optional &mdash; if they&rsquo;re taking one of your dates in return</div>
                        </div>
                        <div style="margin:6px 0; font-size:13px;">
                            <label style="font-size:12px; color:#555;">Notes (optional):</label><br>
                            <input type="text" class="swap-notes" data-picker="{swap_picker_id}" placeholder="e.g. Phillips can't make April 18" style="padding:3px 6px; font-size:13px; width:280px; margin-top:2px;">
                        </div>
                        <button class="btn btn-snack" style="font-size:12px; padding:4px 12px; margin-top:4px; background:#3b82f6;" onclick="submitSwapRequest('{swap_picker_id}', '{team_name}')">Submit Swap Request</button>
                    </div>"""
            snack_button_html += f' <button class="btn btn-snack" style="background:#3b82f6;" onclick="toggleSnackPicker(\'{swap_picker_id}\')">Swap Lunch Day</button>'

        # Build "Report Game Status" button link (pre-fills the issue template)
        game_status_url = f"https://github.com/aknowles/milton-club-baseball/issues/new?template=game-status.yml&title=%5BGame+Status%5D+{quote(team_name)}"
        game_status_btn = f'<a class="btn btn-status" href="{game_status_url}" target="_blank">Report Postponed / Cancelled</a>'

        # Extract org and age group from team name (e.g. "MDB Knights 11U Gold")
        age_match = re.search(r"\b(\d+U)\b", team_name, re.IGNORECASE)
        team_age = age_match.group(1) if age_match else ""
        team_org = team_name[:age_match.start()].strip() if age_match else team_name

        team_sections += f"""
        <div class="grade-section" data-team="{team_name}" data-org="{team_org}" data-age="{team_age}">
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
                    {snack_button_html}
                    {game_status_btn}
                </div>
                {snack_picker_html}
                {swap_picker_html}
                <div class="upcoming-games">
                    {games_html if games_html else '<p style="color:#666;">No upcoming games found.</p>'}
                </div>
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

    # Build snack assignments JSON for swap picker JavaScript
    snacks_json = json.dumps(config.get("snacks", {}))

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
        .btn-status {{ background: #64748b; color: white; }}
        .snack-tag {{
            display: inline-block;
            background: #e67e22;
            color: white;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 11px;
            margin-left: 8px;
        }}
        .snack-needed {{
            background: transparent;
            color: #999;
            border: 1px dashed #ccc;
        }}
        .dh-tag {{
            display: inline-block;
            background: #6366f1;
            color: white;
            padding: 1px 6px;
            border-radius: 4px;
            font-size: 10px;
            font-weight: 600;
            margin-left: 4px;
            vertical-align: middle;
        }}
        .status-tag {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 600;
            margin-left: 6px;
        }}
        .status-postponed {{
            background: #f59e0b;
            color: white;
        }}
        .status-cancelled {{
            background: #ef4444;
            color: white;
        }}
        .game-row.game-postponed {{
            opacity: 0.7;
        }}
        .game-row.game-cancelled {{
            opacity: 0.5;
            text-decoration: line-through;
        }}
        .jersey-tag {{
            display: inline-block;
            padding: 1px 6px;
            border-radius: 4px;
            font-size: 10px;
            font-weight: 600;
            margin-left: 4px;
            vertical-align: middle;
        }}
        .jersey-white {{
            background: #f0f0f0;
            color: #333;
        }}
        .jersey-dark {{
            background: #333;
            color: #f0f0f0;
        }}

        .travel-tag {{
            display: inline-block;
            background: #64748b;
            color: white;
            padding: 1px 6px;
            border-radius: 4px;
            font-size: 10px;
            font-weight: 600;
            margin-left: 4px;
            vertical-align: middle;
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
        .game-past {{ opacity: 0.7; }}
        .game-emoji {{ min-width: 24px; text-align: center; }}
        .game-date {{ font-weight: 600; min-width: 50px; }}
        .game-time {{ color: #666; min-width: 70px; }}
        .game-matchup {{ flex: 1; }}
        .game-link {{ color: #1e6b3a; text-decoration: none; }}
        .game-link:hover {{ text-decoration: underline; }}
        .game-location {{ color: #888; font-size: 12px; }}
        .game-result-label {{ font-size: 12px; font-weight: 600; color: #666; }}
        .event-tag {{
            display: inline-block;
            background: #1e6b3a;
            color: white;
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 12px;
            margin-bottom: 10px;
        }}

        .filter-bar {{
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
            margin-bottom: 16px;
            align-items: center;
        }}
        .filter-group {{
            display: flex;
            align-items: center;
            gap: 4px;
        }}
        .filter-group label {{
            font-size: 12px;
            color: #666;
            font-weight: 600;
        }}
        .filter-group select {{
            padding: 4px 8px;
            border-radius: 6px;
            border: 1px solid #ccc;
            font-size: 13px;
            background: white;
        }}
        .filter-clear {{
            font-size: 12px;
            color: #1e6b3a;
            cursor: pointer;
            text-decoration: underline;
            background: none;
            border: none;
            padding: 0;
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

    <div class="filter-bar" id="filter-bar"></div>

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
            <summary style="cursor:pointer; font-weight:600;">How do I mark a game as postponed or cancelled?</summary>
            <p style="margin:8px 0 0 0; color:#555;">Click the <strong>Report Postponed / Cancelled</strong> button under your team, fill in the date and status, and submit the issue. An admin will review and apply the change. The game will then show a &#9888;&#65039; (postponed) or &#128683; (cancelled) icon on the calendar and this page.</p>
        </details>
        <details style="margin-bottom:12px;">
            <summary style="cursor:pointer; font-weight:600;">What do the icons on the schedule mean?</summary>
            <p style="margin:8px 0 0 0; color:#555;">
                &#9918; Upcoming game &bull;
                &#9989; Win &bull;
                &#10060; Loss &bull;
                &#129309; Tie &bull;
                &#9888;&#65039; Postponed &bull;
                &#128683; Cancelled &bull;
                &#127947;&#65039; Practice &bull;
                &#128663; Far away game (distance from Milton, MA shown)
            </p>
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
        var snackAssignments = {snacks_json};

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

        // --- Filters (persisted in localStorage) ---
        (function() {{
            const sections = document.querySelectorAll('.grade-section[data-team]');
            const orgs = [...new Set([...sections].map(s => s.dataset.org).filter(Boolean))].sort();
            const ages = [...new Set([...sections].map(s => s.dataset.age).filter(Boolean))].sort(
                (a, b) => parseInt(a) - parseInt(b)
            );
            const teamNames = [...new Set([...sections].map(s => s.dataset.team).filter(Boolean))].sort();

            // Only show filter bar if there's more than one option to filter
            if (orgs.length + ages.length + teamNames.length <= 1) return;

            const bar = document.getElementById('filter-bar');
            if (!bar) return;

            function makeSelect(id, label, options) {{
                const saved = localStorage.getItem('mcb_filter_' + id) || '';
                let html = '<div class="filter-group"><label>' + label + ':</label><select id="filter-' + id + '">';
                html += '<option value="">All</option>';
                options.forEach(function(o) {{
                    html += '<option value="' + o + '"' + (o === saved ? ' selected' : '') + '>' + o + '</option>';
                }});
                html += '</select></div>';
                return html;
            }}

            let barHtml = '';
            if (orgs.length > 1) barHtml += makeSelect('org', 'Organization', orgs);
            if (ages.length > 1) barHtml += makeSelect('age', 'Age Group', ages);
            if (teamNames.length > 1) barHtml += makeSelect('team', 'Team', teamNames);
            barHtml += '<button class="filter-clear" onclick="clearFilters()">Clear filters</button>';
            bar.innerHTML = barHtml;

            function applyFilters() {{
                const org = document.getElementById('filter-org');
                const age = document.getElementById('filter-age');
                const team = document.getElementById('filter-team');
                const orgVal = org ? org.value : '';
                const ageVal = age ? age.value : '';
                const teamVal = team ? team.value : '';

                localStorage.setItem('mcb_filter_org', orgVal);
                localStorage.setItem('mcb_filter_age', ageVal);
                localStorage.setItem('mcb_filter_team', teamVal);

                sections.forEach(function(s) {{
                    let show = true;
                    if (orgVal && s.dataset.org !== orgVal) show = false;
                    if (ageVal && s.dataset.age !== ageVal) show = false;
                    if (teamVal && s.dataset.team !== teamVal) show = false;
                    s.style.display = show ? '' : 'none';
                }});
            }}

            bar.addEventListener('change', applyFilters);
            window.clearFilters = function() {{
                bar.querySelectorAll('select').forEach(function(sel) {{ sel.value = ''; }});
                localStorage.removeItem('mcb_filter_org');
                localStorage.removeItem('mcb_filter_age');
                localStorage.removeItem('mcb_filter_team');
                sections.forEach(function(s) {{ s.style.display = ''; }});
            }};

            // Apply saved filters on load
            applyFilters();
        }})();

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

        function onSwapFamilyChange(sel) {{
            const picker = sel.closest('.snack-picker');
            const teamName = sel.dataset.team;
            const familyName = sel.value;
            const newDateSel = picker.querySelector('.swap-new-date');
            const swapWithSel = picker.querySelector('.swap-with-family');
            const currentDateSel = picker.querySelector('.swap-current-date');

            // Reset downstream
            newDateSel.innerHTML = '<option value="">Select date...</option>';
            newDateSel.disabled = true;
            swapWithSel.innerHTML = '<option value="">Select family...</option>';
            swapWithSel.disabled = true;
            currentDateSel.innerHTML = '<option value="">None (no return swap)</option>';
            currentDateSel.disabled = true;

            if (!familyName || !teamName) return;

            // Show all snack dates that have other families assigned
            const teamSnacks = snackAssignments[teamName] || [];
            const dates = teamSnacks.filter(function(entry) {{
                return entry.families && entry.families.some(function(f) {{ return f !== familyName; }});
            }});
            if (dates.length === 0) {{
                newDateSel.innerHTML = '<option value="">No dates available</option>';
                return;
            }}
            newDateSel.innerHTML = '<option value="">Select date...</option>';
            dates.forEach(function(entry) {{
                const d = new Date(entry.date + 'T12:00:00');
                const label = d.toLocaleDateString('en-US', {{ month: 'short', day: 'numeric' }});
                const famList = entry.families.join(', ');
                newDateSel.innerHTML += '<option value="' + entry.date + '">' + label + ' (' + famList + ')</option>';
            }});
            newDateSel.disabled = false;
        }}

        function onSwapNewDateChange(sel) {{
            const picker = sel.closest('.snack-picker');
            const familySel = picker.querySelector('.swap-your-family');
            const teamName = familySel.dataset.team;
            const familyName = familySel.value;
            const newDateVal = sel.value;
            const swapWithSel = picker.querySelector('.swap-with-family');
            const currentDateSel = picker.querySelector('.swap-current-date');

            swapWithSel.innerHTML = '<option value="">Select family...</option>';
            swapWithSel.disabled = true;
            currentDateSel.innerHTML = '<option value="">None (no return swap)</option>';
            currentDateSel.disabled = true;

            if (!newDateVal) return;

            // Show families on this date (excluding the requesting family)
            const teamSnacks = snackAssignments[teamName] || [];
            const targetEntry = teamSnacks.find(function(entry) {{ return entry.date === newDateVal; }});
            if (!targetEntry || !targetEntry.families) return;

            const candidates = targetEntry.families.filter(function(f) {{ return f !== familyName; }});
            if (candidates.length === 0) return;

            swapWithSel.innerHTML = '<option value="">Select family...</option>';
            candidates.forEach(function(f) {{
                swapWithSel.innerHTML += '<option value="' + f + '">' + f + '</option>';
            }});
            swapWithSel.disabled = false;

            // Populate "They take your date" with requesting family's assigned dates
            currentDateSel.innerHTML = '<option value="">None (no return swap)</option>';
            const assignedDates = teamSnacks.filter(function(entry) {{
                return entry.date !== newDateVal && entry.families && entry.families.indexOf(familyName) !== -1;
            }});
            assignedDates.forEach(function(entry) {{
                const d = new Date(entry.date + 'T12:00:00');
                const label = d.toLocaleDateString('en-US', {{ month: 'short', day: 'numeric' }});
                currentDateSel.innerHTML += '<option value="' + entry.date + '">' + label + '</option>';
            }});
            if (assignedDates.length > 0) {{
                currentDateSel.disabled = false;
            }}
        }}

        function submitSwapRequest(pickerId, teamName) {{
            const picker = document.getElementById(pickerId);
            const familySel = picker.querySelector('.swap-your-family');
            const familyName = familySel ? familySel.value : '';
            if (!familyName) {{ alert('Please select your family name.'); return; }}

            const newDate = picker.querySelector('.swap-new-date');
            const newDateVal = newDate ? newDate.value : '';
            if (!newDateVal) {{ alert('Please select a date.'); return; }}

            const swapWith = picker.querySelector('.swap-with-family');
            const swapWithVal = swapWith ? swapWith.value : '';
            if (!swapWithVal) {{ alert('Please select the family you are replacing.'); return; }}

            const currentDate = picker.querySelector('.swap-current-date');
            const currentDateVal = currentDate ? currentDate.value : '';

            const notesInput = picker.querySelector('.swap-notes');
            const notes = notesInput ? notesInput.value.trim() : '';

            var title;
            if (currentDateVal) {{
                title = '[Snacks] Swap: ' + familyName + ' (' + currentDateVal + ') \u2194 ' + swapWithVal + ' (' + newDateVal + ')';
            }} else {{
                title = '[Snacks] Swap: ' + familyName + ' replaces ' + swapWithVal + ' on ' + newDateVal;
            }}
            const body =
                '### Team\\n\\n' + teamName +
                '\\n\\n### Family Name\\n\\n' + familyName +
                '\\n\\n### Currently Assigned Date\\n\\n' + (currentDateVal || '') +
                '\\n\\n### Swap To Date\\n\\n' + newDateVal +
                '\\n\\n### Swap With Family\\n\\n' + swapWithVal +
                (notes ? '\\n\\n### Notes\\n\\n' + notes : '');

            const url = 'https://github.com/aknowles/milton-club-baseball/issues/new?labels=snack-swap&title=' + encodeURIComponent(title) + '&body=' + encodeURIComponent(body);
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
    # Teams whose fetch failed this run. We preserve their previous snapshot
    # entries so we don't fire false "removed" change notifications, and we
    # rehydrate their previous ICS from Pages so the published calendar
    # survives the next deploy.
    failed_teams = set()

    for i, team in enumerate(teams):
        url = team["url"]
        name = team["team_name"]
        print(f"Fetching schedule for {name}...")

        try:
            html = fetch_team_schedule_html(url)
            games = parse_schedule(html, name, url)
            all_games.extend(games)

            # Also scrape roster from the same page
            roster = parse_roster(html, name)
            if roster:
                print(f"  Found {len(roster)} roster entries for {name}")
                rosters_by_team[name] = roster
            else:
                print(f"  No roster found on page for {name}")
                # Save HTML for debugging roster parsing
                debug_path = f"debug_roster_{team_slug(name)}.html"
                with open(debug_path, "w") as df:
                    df.write(html)
                print(f"  Saved page HTML to {debug_path} for debugging")
        except requests.RequestException as e:
            print(f"  ERROR fetching {name}: {e}")
            failed_teams.add(name)
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

    # --- Travel distance lookups ---
    if config.get("home_location"):
        geocode_cache = load_geocode_cache()
        # Collect unique addresses to minimise API calls
        addr_to_games = {}
        for game in all_games:
            addr = game.get("address") or game.get("location")
            if addr:
                addr_to_games.setdefault(addr, []).append(game)

        geocoded_count = 0
        for addr in addr_to_games:
            dist, did_geocode = get_travel_distance(config, addr, geocode_cache)
            for game in addr_to_games[addr]:
                if dist is not None:
                    game["travel_miles"] = dist
            if did_geocode:
                geocoded_count += 1
                time.sleep(1)  # respect Nominatim rate limits

        save_geocode_cache(geocode_cache)
        threshold = config.get("travel_threshold_miles", 40)
        far_count = sum(1 for g in all_games if g.get("travel_miles", 0) >= threshold)
        print(f"Travel distances: {geocoded_count} new geocode lookups, "
              f"{far_count} games >= {threshold} mi from {config['home_location']['name']}")

    if not all_games:
        print("WARNING: No events found. Writing empty calendars.")

    # Group games by team and write per-team ICS files
    os.makedirs("calendars", exist_ok=True)
    games_by_team = {}
    for game in all_games:
        games_by_team.setdefault(game["team_name"], []).append(game)

    # Change detection and ntfy notifications
    old_snapshot = load_previous_snapshot()
    new_snapshot = build_snapshot(games_by_team, config=config)

    # For teams whose fetch failed this run, carry their previous snapshot
    # entries forward so detect_changes doesn't flag every event as removed
    # and spam ntfy. Also re-fetch the previously published ICS file so the
    # Pages deploy doesn't drop the team's calendar.
    if failed_teams:
        base_url = config.get("base_url", "")
        for team_name in sorted(failed_teams):
            if team_name in old_snapshot:
                new_snapshot[team_name] = old_snapshot[team_name]
                print(f"  Preserving previous snapshot for {team_name} (fetch failed)")
            if base_url:
                slug = team_slug(team_name)
                ics_path = f"calendars/{slug}.ics"
                if os.path.exists(ics_path):
                    continue
                try:
                    resp = requests.get(f"{base_url}/calendars/{slug}.ics", timeout=10)
                    if resp.status_code == 200 and resp.content:
                        with open(ics_path, "wb") as f:
                            f.write(resp.content)
                        print(f"  Preserved previous calendar {slug}.ics from Pages")
                    else:
                        print(f"  Could not fetch previous calendar for {team_name} (HTTP {resp.status_code})")
                except Exception as e:
                    print(f"  Could not fetch previous calendar for {team_name}: {e}")

    if not old_snapshot:
        # No previous snapshot — seed run. Save snapshot without sending
        # notifications so we don't blast every game as "New".
        print("\nNo previous snapshot — seeding. Skipping notifications.")
    else:
        changes, reschedules = detect_changes(old_snapshot, new_snapshot)
        if changes:
            print("\nSchedule changes detected:")
            for team_name, change_list in changes.items():
                for c in change_list:
                    print(f"  [{team_name}] {c}")
            notify_changes(changes, config)
        else:
            print("\nNo schedule changes detected.")

        # Migrate snack assignments for rescheduled games
        if reschedules:
            migration_log = migrate_snack_assignments(config, reschedules)
            if migration_log:
                print("\nSnack assignment migrations:")
                for msg in migration_log:
                    print(msg)
                # Persist updated snack assignments
                with open("config.json", "w") as f:
                    json.dump(config, f, indent=2)
                    f.write("\n")
                print("Updated config.json with migrated snack assignments.")
    save_snapshot(new_snapshot)

    # Save rosters — fall back to previously published rosters from Pages
    if rosters_by_team:
        save_rosters(rosters_by_team)
    else:
        print("No roster data scraped — loading previous rosters from Pages...")
        try:
            base_url = config.get("base_url", "")
            if base_url:
                resp = requests.get(f"{base_url}/calendars/rosters.json", timeout=10)
                if resp.status_code == 200:
                    rosters_by_team = resp.json()
                    save_rosters(rosters_by_team)
                    print(f"  Loaded {len(rosters_by_team)} team rosters from Pages")
        except Exception as e:
            print(f"  Could not fetch previous rosters: {e}")

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
