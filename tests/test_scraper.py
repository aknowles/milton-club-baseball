import json
import re
from datetime import date, datetime

import pytest

import scraper
from ntfy import encode_header

HOSTILE = '<img src=x onerror=alert(1)>"\'</script><script>alert(2)</script>'


def make_game(tid, team_name, day, opponent, **extra):
    game = {
        "title": f"{team_name} vs. {opponent}", "date": datetime.strptime(day, "%Y-%m-%d"),
        "time": datetime.strptime("1:00 PM", "%I:%M %p"), "is_allday": False,
        "home_away": "vs.", "opponent": opponent, "location": "Park, 1 Main St",
        "field_name": "Park", "address": "1 Main St", "event_name": "League",
        "event_url": "https://www.perfectgame.org/events/Default.aspx?event=1",
        "team_name": team_name, "team_url": None, "game_url": None,
        "score": None, "score_result": None, "pg_status": "", "team_id": tid,
    }
    game.update(extra)
    return game


@pytest.fixture
def config():
    return {
        "teams": [{"id": "knights-11u", "team_name": "Knights 11U", "year": 2099,
                   "coach": "O'Neil", "ntfy_topic": "knights-11u",
                   "snack_signup": True}],
        "base_url": "https://example.github.io/site",
        "timezone": "US/Eastern",
        "snacks": {"knights-11u": [{"date": "2099-05-09", "families": ["Dow"]}]},
        "game_overrides": {},
    }


# --- HTML output -------------------------------------------------------

def test_index_escapes_untrusted_text(config):
    """Nothing scraped or typed into an issue may reach the page as markup."""
    config["teams"][0]["team_name"] = f"Knights 11U {HOSTILE}"
    config["teams"][0]["coach"] = HOSTILE
    config["snacks"]["knights-11u"][0]["families"] = [HOSTILE]
    config["game_overrides"]["knights-11u"] = [
        {"date": "2099-05-10", "status": "postponed", "reason": HOSTILE}]
    games = [
        make_game("knights-11u", config["teams"][0]["team_name"], "2099-05-09",
                  HOSTILE, location=HOSTILE, field_name=HOSTILE, address=HOSTILE,
                  event_name=HOSTILE),
        make_game("knights-11u", config["teams"][0]["team_name"], "2099-05-10", "Sharks"),
    ]
    rosters = {"knights-11u": [{"name": f"Sam {HOSTILE}"}]}

    html = scraper.generate_index_html(games, config, rosters)

    assert "<img" not in html
    assert "onerror=alert(1)>" not in html
    assert html.count("<script>") == 1 and html.count("</script>") == 1
    # The snack data embedded for the swap picker still round-trips
    embedded = re.search(r"var snackAssignments = (.*);", html).group(1)
    assert json.loads(embedded)["knights-11u"][0]["families"] == [HOSTILE]


def test_buttons_carry_the_team_label_not_a_loop_variable(config):
    """Regression: the snack/status buttons once sent a date ('May 09') or
    'POSTPONED' as the team, because a loop reused the variable name."""
    config["game_overrides"]["knights-11u"] = [
        {"date": "2099-05-09", "status": "postponed"}]
    games = [make_game("knights-11u", "Knights 11U", "2099-05-09", "Sharks")]
    html = scraper.generate_index_html(games, config, {})
    label = "Knights 11U – Coach O&#x27;Neil (2099)"
    assert html.count(f'data-team="{label}"') == 3  # section, signup, swap
    assert "title=%5BGame+Status%5D+Knights%2011U" in html


def test_json_for_script_cannot_close_the_script_element():
    out = scraper.json_for_script({"a": "</script><!--"})
    assert "<" not in out and ">" not in out
    assert json.loads(out) == {"a": "</script><!--"}


# --- calendar ----------------------------------------------------------

def test_calendar_times_are_eastern_across_dst(config):
    games = [make_game("knights-11u", "Knights 11U", "2026-01-10", "A"),
             make_game("knights-11u", "Knights 11U", "2026-07-10", "B")]
    ics = scraper.make_calendar(games, config).to_ical().decode()
    assert "DTSTART;TZID=US/Eastern:20260110T130000" in ics
    assert "DTSTART;TZID=US/Eastern:20260710T130000" in ics
    starts = [e.decoded("dtstart").utcoffset().total_seconds() / 3600
              for e in scraper.Calendar.from_ical(ics).walk("VEVENT")]
    assert starts == [-5.0, -4.0]


# --- parsing -----------------------------------------------------------

def test_clean_opponent_name_strips_metadata():
    raw = ("Rip City USA Palumbo (10-7-0) 11U Major Massachusetts (Major) Pool A "
           "Probable Pitchers Beach Road Field @ Beach Road Field GameID: 1399385")
    assert scraper.clean_opponent_name(raw) == "Rip City USA Palumbo"


def test_parse_practice_sheet():
    html = """<table>
      <tr><td>Week 5 (December 28 – January 3, 2027)</td></tr>
      <tr><td>Time</td><td>Monday</td><td>Tuesday</td><td>Weekend Time</td><td>Saturday</td></tr>
      <tr><td>5:00 PM</td><td>9U</td><td></td><td>9:00 AM</td><td>14U</td></tr>
    </table>"""
    assert scraper.parse_practice_sheet(html) == [
        {"date": "2026-12-28", "time": "5:00 PM", "label": "9U"},
        {"date": "2027-01-02", "time": "9:00 AM", "label": "14U"},
    ]


def test_roster_family_names_keeps_surnames_only():
    roster = [{"name": "Sam Knowles", "number": "7", "grad_year": "2033"},
              {"name": "Al Dow"}, {"name": "Jo Knowles"}, {"name": " "}]
    assert scraper.roster_family_names(roster) == ["Dow", "Knowles"]


# --- change detection --------------------------------------------------

def event(day, time="01:00 PM", opponent="Sharks", status=""):
    return {"title": f"Knights vs. {opponent}", "date": day, "time": time,
            "location": "Park", "opponent": opponent, "override_status": status}


def test_detect_changes():
    today = date(2026, 5, 1)
    old = {"t": {"a": event("2026-05-09"), "b": event("2026-05-10", opponent="Tigers"),
                 "old": event("2026-04-01", opponent="Past")}}
    new = {"t": {"a": event("2026-05-09", time="03:00 PM"),
                 "c": event("2026-05-17", opponent="Tigers")}}
    changes, reschedules = scraper.detect_changes(old, new, today=today)
    assert changes["t"] == [
        "Rescheduled: Knights vs. Tigers moved from 2026-05-10 to 2026-05-17",
        "Changed: Knights vs. Sharks on 2026-05-09 (time 01:00 PM -> 03:00 PM)",
    ]
    assert reschedules == [{"team": "t", "old_date": "2026-05-10",
                            "new_date": "2026-05-17", "opponent": "Tigers"}]


# --- ntfy --------------------------------------------------------------

def test_ntfy_header_encoding():
    assert encode_header("Snack Signup: 2026-04-18") == "Snack Signup: 2026-04-18"
    assert encode_header("a\r\nX-Injected: 1") == "a X-Injected: 1"
    encoded = encode_header("Knowles ↔ Dow")
    assert encoded.startswith("=?UTF-8?B?") and encoded.isascii()
