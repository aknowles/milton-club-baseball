#!/usr/bin/env python3
"""
Process a GitHub issue for snack signups and swaps.

Parses the issue body (GitHub issue form format) and updates config.json.
Triggered by the process-snack-signups workflow when a repo admin comments
/approve on the issue.

Issues arrive in two shapes with different headings: the pickers on the
web page pre-fill one set, the GitHub issue templates produce another.
Both are accepted.
"""

import json
import os

from issue_utils import (IssueError, fail, first_field, load_config,
                         parse_date, parse_family_names, parse_issue_body,
                         save_config)
from ntfy import send_ntfy
from teams import resolve_team_id, team_id


def _resolve_team(fields, config):
    raw_team = fields.get("team", "")
    team = resolve_team_id(config, raw_team)
    if not team:
        raise IssueError(f"Unknown or missing team '{raw_team[:80]}'")
    return team


def _one_family(fields, what, *keys):
    names = parse_family_names(first_field(fields, *keys), what)
    if len(names) != 1:
        raise IssueError(f"{what}: exactly one family name is required")
    return names[0]


def _entry_for(team_snacks, date_val):
    return next((e for e in team_snacks if e.get("date") == date_val), None)


def apply_signup(fields, config):
    """Apply a snack signup. Returns (team, ntfy_title, ntfy_body)."""
    team = _resolve_team(fields, config)
    date_val = parse_date(first_field(fields, "game_date", "date"), "Game date")
    families = parse_family_names(
        first_field(fields, "family_names", "families"))
    if not families:
        raise IssueError("No families specified")

    team_snacks = config.setdefault("snacks", {}).setdefault(team, [])
    existing = _entry_for(team_snacks, date_val)
    if existing:
        for fam in families:
            if fam not in existing.setdefault("families", []):
                existing["families"].append(fam)
        print(f"Merged families into existing snack entry for {team} on {date_val}")
    else:
        team_snacks.append({"date": date_val, "families": families})
        print(f"Added snack signup for {team} on {date_val}: {families}")

    family_str = ", ".join(families)
    return (team, f"Snack Signup: {date_val}",
            f"{family_str} signed up for snacks on {date_val}")


def apply_swap(fields, config):
    """Apply a snack swap: one family replaces another, with an optional
    return swap. Returns (team, ntfy_title, ntfy_body)."""
    team = _resolve_team(fields, config)
    family_name = _one_family(fields, "Your family name",
                              "family_name", "your_family_name")
    swap_with = _one_family(fields, "Family to replace",
                            "swap_with_family", "replacing")
    swap_to_date = parse_date(
        first_field(fields, "swap_to_date", "date"), "Date")
    current_raw = first_field(
        fields, "currently_assigned_date", "they_take_your_date")
    current_date = parse_date(current_raw, "Return date") if current_raw else ""

    if family_name == swap_with:
        raise IssueError("A family can't swap with itself")
    if current_date and current_date == swap_to_date:
        raise IssueError("The two swap dates are the same")

    team_snacks = config.setdefault("snacks", {}).setdefault(team, [])

    target_entry = _entry_for(team_snacks, swap_to_date)
    if not target_entry:
        raise IssueError(f"No snack entry found for {team} on {swap_to_date}")
    if swap_with not in target_entry.get("families", []):
        raise IssueError(f"{swap_with} is not assigned to {swap_to_date}")

    current_entry = None
    if current_date:
        current_entry = _entry_for(team_snacks, current_date)
        if not current_entry:
            raise IssueError(f"No snack entry found for {team} on {current_date}")
        if family_name not in current_entry.get("families", []):
            raise IssueError(f"{family_name} is not assigned to {current_date}")

    # On the target date: replace swap_with with family_name
    target_entry["families"].remove(swap_with)
    if family_name not in target_entry["families"]:
        target_entry["families"].append(family_name)

    if current_entry:
        # Return swap: swap_with takes family_name's old date
        current_entry["families"].remove(family_name)
        if swap_with not in current_entry["families"]:
            current_entry["families"].append(swap_with)
        print(f"Swapped: {family_name} ({current_date}) <-> {swap_with} ({swap_to_date})")
        return (team, f"Snack Swap: {family_name} ↔ {swap_with}",
                f"{family_name} ({current_date}) swapped with {swap_with} ({swap_to_date})")

    print(f"Swap: {family_name} replaces {swap_with} on {swap_to_date}")
    return (team, f"Snack Swap: {family_name} replaces {swap_with}",
            f"{family_name} replaces {swap_with} on {swap_to_date}")


def main():
    issue_body = os.environ.get("ISSUE_BODY", "")
    if not issue_body:
        fail("No issue body found")
    issue_title = os.environ.get("ISSUE_TITLE", "")

    fields = parse_issue_body(issue_body)
    print(f"Parsed fields: {json.dumps(fields, indent=2)}")

    config = load_config()
    is_swap = issue_title.strip().lower().startswith("[snacks] swap")
    try:
        team, ntfy_title, ntfy_body = (
            apply_swap(fields, config) if is_swap else apply_signup(fields, config))
    except IssueError as e:
        fail(str(e))

    save_config(config)
    print("config.json updated successfully")

    team_cfg = next((t for t in config.get("teams", []) if team_id(t) == team), {})
    if team_cfg.get("ntfy_topic"):
        send_ntfy(team_cfg["ntfy_topic"], ntfy_title, ntfy_body)


if __name__ == "__main__":
    main()
