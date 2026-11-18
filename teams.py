"""
Team identity helpers shared by the scraper and the issue processors.

Each entry in config.json's ``teams`` list has:
  - ``id``: stable key. It names the calendar file (calendars/<id>.ics) and
    keys every per-team block in config.json (practices, snacks,
    game_overrides, game_results, practice_sheets). Never change it once
    families have subscribed.
  - ``team_name``: display name. Safe to change at any time.
  - ``year``: season. Two seasons may reuse the same team_name.
"""

import re


def slugify(text):
    """Convert text to a URL-safe slug."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def team_id(team):
    """Stable key for a team config entry (falls back to the name's slug)."""
    return team.get("id") or slugify(team["team_name"])


def team_label(team):
    """Unambiguous human label, e.g. 'MDB Knights 12U Blue (2027)'."""
    year = team.get("year")
    return f"{team['team_name']} ({year})" if year else team["team_name"]


def resolve_team_id(config, text):
    """Resolve what a person or issue form supplied (an id, a label such as
    'MDB Knights 12U Blue (2027)', or a bare team name) to a team id.

    A bare name shared by several seasons resolves to the newest one.
    Returns None when nothing matches.
    """
    wanted = (text or "").strip().lower()
    if not wanted:
        return None
    teams = config.get("teams", [])
    for team in teams:
        if wanted in (team_id(team).lower(), team_label(team).lower()):
            return team_id(team)
    by_name = [t for t in teams if t["team_name"].lower() == wanted]
    if by_name:
        return team_id(max(by_name, key=lambda t: t.get("year", 0)))
    return None
