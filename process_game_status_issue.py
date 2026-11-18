#!/usr/bin/env python3
"""
Process a GitHub issue for game status overrides (postponed/cancelled).

Parses the issue body (GitHub issue form format) and updates config.json.
Triggered by the process-game-status workflow when a repo admin comments
/approve on the issue.
"""

import json
import os

from issue_utils import (IssueError, clean_text, fail, first_field,
                         load_config, parse_date, parse_issue_body,
                         save_config)
from teams import resolve_team_id


def apply_game_status(fields, config):
    """Apply the override described by *fields* to *config* in place."""
    raw_team = fields.get("team", "")
    if not raw_team:
        raise IssueError("No team specified")
    team = resolve_team_id(config, raw_team)
    if not team:
        raise IssueError(f"Unknown team '{raw_team[:80]}'")

    date_val = parse_date(first_field(fields, "date_of_game", "date"),
                          "Date of game")

    status = fields.get("status", "").strip().lower()
    if status not in ("postponed", "cancelled"):
        raise IssueError(
            f"Invalid status '{status[:40]}' (expected postponed or cancelled)")

    reason = clean_text(first_field(fields, "reason_optional", "reason"),
                        "Reason")

    team_overrides = config.setdefault("game_overrides", {}).setdefault(team, [])

    # Update an existing override on the same date rather than adding another
    for entry in team_overrides:
        if entry.get("date") == date_val:
            entry["status"] = status
            if reason:
                entry["reason"] = reason
            else:
                entry.pop("reason", None)
            return f"Updated existing override for {team} on {date_val}: {status}"

    entry = {"date": date_val, "status": status}
    if reason:
        entry["reason"] = reason
    team_overrides.append(entry)
    return f"Added game override for {team} on {date_val}: {status}"


def main():
    issue_body = os.environ.get("ISSUE_BODY", "")
    if not issue_body:
        fail("No issue body found")

    fields = parse_issue_body(issue_body)
    print(f"Parsed fields: {json.dumps(fields, indent=2)}")

    config = load_config()
    try:
        print(apply_game_status(fields, config))
    except IssueError as e:
        fail(str(e))

    save_config(config)
    print("config.json updated successfully")


if __name__ == "__main__":
    main()
