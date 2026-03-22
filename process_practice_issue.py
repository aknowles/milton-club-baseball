#!/usr/bin/env python3
"""
Process a GitHub issue for practice changes (add/cancel/modify).

Parses the issue body (GitHub issue form format) and updates config.json.
Triggered by the process-practice-changes workflow when an issue gets the
'approved' label.
"""

import json
import os
import re
import sys


def parse_issue_body(body):
    """Parse GitHub issue form body into field dict.

    Issue form bodies use this format:
    ### Field Label

    value
    """
    fields = {}
    current_key = None
    current_value_lines = []

    for line in body.split("\n"):
        header_match = re.match(r"^###\s+(.+)$", line.strip())
        if header_match:
            if current_key is not None:
                fields[current_key] = "\n".join(current_value_lines).strip()
            current_key = header_match.group(1).strip().lower().replace(" ", "_")
            # Normalize common field names
            current_key = current_key.replace("(", "").replace(")", "")
            current_key = re.sub(r"_+", "_", current_key).strip("_")
            current_value_lines = []
        elif current_key is not None:
            current_value_lines.append(line)

    if current_key is not None:
        fields[current_key] = "\n".join(current_value_lines).strip()

    return fields


def load_config(path="config.json"):
    with open(path, "r") as f:
        return json.load(f)


def save_config(config, path="config.json"):
    with open(path, "w") as f:
        json.dump(config, f, indent=2)
        f.write("\n")


def main():
    issue_body = os.environ.get("ISSUE_BODY", "")
    issue_title = os.environ.get("ISSUE_TITLE", "")

    if not issue_body:
        print("ERROR: No issue body found")
        sys.exit(1)

    fields = parse_issue_body(issue_body)
    print(f"Parsed fields: {json.dumps(fields, indent=2)}")

    team = fields.get("team", "")
    if not team:
        print("ERROR: No team specified")
        sys.exit(1)

    config = load_config()
    practices = config.setdefault("practices", {})
    team_practices = practices.setdefault(team, {
        "adhoc": [],
        "modifications": [],
        "blackout_dates": [],
    })

    # Determine action from title
    title_lower = issue_title.lower()

    if "cancel" in title_lower:
        date_val = fields.get("date_of_practice_to_cancel", fields.get("date", ""))
        if not date_val:
            print("ERROR: No date for cancellation")
            sys.exit(1)
        mod = {"date": date_val, "action": "cancel"}
        reason = fields.get("reason_optional", fields.get("reason", ""))
        if reason and reason != "_No response_":
            mod["reason"] = reason
        team_practices.setdefault("modifications", []).append(mod)
        print(f"Added cancellation for {team} on {date_val}")

    elif "modify" in title_lower:
        original_date = fields.get("original_date", "")
        if not original_date:
            print("ERROR: No original date for modification")
            sys.exit(1)
        mod = {"date": original_date, "action": "reschedule"}
        new_date = fields.get("new_date_leave_blank_if_unchanged", fields.get("new_date", ""))
        new_time = fields.get("new_time_leave_blank_if_unchanged", fields.get("new_time", ""))
        new_loc = fields.get("new_location_leave_blank_if_unchanged", fields.get("new_location", ""))
        if new_date and new_date != "_No response_":
            mod["new_date"] = new_date
        if new_time and new_time != "_No response_":
            mod["new_time"] = new_time
        if new_loc and new_loc != "_No response_":
            mod["new_location"] = new_loc
        team_practices.setdefault("modifications", []).append(mod)
        print(f"Added modification for {team} on {original_date}")

    elif "add" in title_lower:
        date_val = fields.get("date", "")
        time_val = fields.get("time", "")
        if not date_val:
            print("ERROR: No date for new practice")
            sys.exit(1)
        entry = {"date": date_val}
        if time_val and time_val != "_No response_":
            entry["time"] = time_val
        duration = fields.get("duration_minutes", "")
        if duration and duration != "_No response_":
            try:
                entry["duration_minutes"] = int(duration)
            except ValueError:
                pass
        location = fields.get("location", "")
        if location and location != "_No response_":
            entry["location"] = location
        title = fields.get("event_title", "")
        if title and title != "_No response_":
            entry["title"] = title
        team_practices.setdefault("adhoc", []).append(entry)
        print(f"Added practice for {team} on {date_val}")

    else:
        print(f"ERROR: Could not determine action from title: {issue_title}")
        sys.exit(1)

    save_config(config)
    print("config.json updated successfully")


if __name__ == "__main__":
    main()
