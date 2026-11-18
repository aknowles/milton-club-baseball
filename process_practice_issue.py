#!/usr/bin/env python3
"""
Process a GitHub issue for practice changes (add/cancel/modify).

Parses the issue body (GitHub issue form format) and updates config.json.
Triggered by the process-practice-changes workflow when a repo admin
comments /approve on the issue.
"""

import json
import os
import re

from issue_utils import (IssueError, clean_text, fail, first_field,
                         load_config, parse_date, parse_issue_body,
                         parse_time, save_config)
from teams import resolve_team_id

DEFAULT_DURATION = 90
MAX_DURATION = 600


def action_from_title(title):
    """Return 'add', 'cancel' or 'modify' from an issue title such as
    '[Practice] Cancel: Tuesday'.

    Only the word straight after the prefix counts, so free text later in
    the title ("Add: makeup for cancelled practice") can't change the action.
    """
    m = re.match(r"\s*\[practice\]\s*(add|cancel|modify)\b", title or "", re.I)
    return m.group(1).lower() if m else None


def apply_practice_change(fields, title, config):
    """Apply the change described by *fields* to *config* in place."""
    raw_team = fields.get("team", "")
    if not raw_team:
        raise IssueError("No team specified")
    team = resolve_team_id(config, raw_team)
    if not team:
        raise IssueError(f"Unknown team '{raw_team[:80]}'")

    action = action_from_title(title)
    if not action:
        raise IssueError(
            "Could not determine action from the title "
            "(expected '[Practice] Add:', 'Cancel:' or 'Modify:')")

    team_practices = config.setdefault("practices", {}).setdefault(team, {
        "adhoc": [],
        "modifications": [],
        "blackout_dates": [],
    })

    if action == "cancel":
        date_val = parse_date(
            first_field(fields, "date_of_practice_to_cancel", "date"),
            "Date of practice to cancel")
        mod = {"date": date_val, "action": "cancel"}
        reason = clean_text(first_field(fields, "reason_optional", "reason"),
                            "Reason")
        if reason:
            mod["reason"] = reason
        team_practices.setdefault("modifications", []).append(mod)
        return f"Added cancellation for {team} on {date_val}"

    if action == "modify":
        original_date = parse_date(fields.get("original_date", ""),
                                   "Original date")
        mod = {"date": original_date, "action": "reschedule"}
        new_date = first_field(fields, "new_date_leave_blank_if_unchanged", "new_date")
        new_time = first_field(fields, "new_time_leave_blank_if_unchanged", "new_time")
        new_loc = first_field(fields, "new_location_leave_blank_if_unchanged", "new_location")
        if new_date:
            mod["new_date"] = parse_date(new_date, "New date")
        if new_time:
            mod["new_time"] = parse_time(new_time, "New time")
        new_loc = clean_text(new_loc, "New location")
        if new_loc:
            mod["new_location"] = new_loc
        if len(mod) == 2:
            raise IssueError("Nothing to change: give a new date, time or location")
        team_practices.setdefault("modifications", []).append(mod)
        return f"Added modification for {team} on {original_date}"

    # add
    date_val = parse_date(fields.get("date", ""), "Date")
    entry = {"date": date_val}
    if fields.get("time"):
        entry["time"] = parse_time(fields["time"])
    duration = fields.get("duration_minutes", "").strip()
    if duration:
        if not duration.isdigit() or not 0 < int(duration) <= MAX_DURATION:
            raise IssueError(
                f"Duration must be a number of minutes from 1 to {MAX_DURATION}")
        entry["duration_minutes"] = int(duration)
    else:
        entry["duration_minutes"] = DEFAULT_DURATION
    location = clean_text(fields.get("location", ""), "Location")
    if location:
        entry["location"] = location
    event_title = clean_text(fields.get("event_title", ""), "Event title",
                             max_len=80)
    if event_title:
        entry["title"] = event_title
    team_practices.setdefault("adhoc", []).append(entry)
    return f"Added practice for {team} on {date_val}"


def main():
    issue_body = os.environ.get("ISSUE_BODY", "")
    issue_title = os.environ.get("ISSUE_TITLE", "")
    if not issue_body:
        fail("No issue body found")

    fields = parse_issue_body(issue_body)
    print(f"Parsed fields: {json.dumps(fields, indent=2)}")

    config = load_config()
    try:
        print(apply_practice_change(fields, issue_title, config))
    except IssueError as e:
        fail(str(e))

    save_config(config)
    print("config.json updated successfully")


if __name__ == "__main__":
    main()
