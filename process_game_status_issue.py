#!/usr/bin/env python3
"""
Process a GitHub issue for game status overrides (postponed/cancelled).

Parses the issue body (GitHub issue form format) and updates config.json.
Triggered by the process-game-status workflow when an admin comments /approve.
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

    if not issue_body:
        print("ERROR: No issue body found")
        sys.exit(1)

    fields = parse_issue_body(issue_body)
    print(f"Parsed fields: {json.dumps(fields, indent=2)}")

    team = fields.get("team", "")
    if not team:
        print("ERROR: No team specified")
        sys.exit(1)

    date_val = fields.get("date_of_game", fields.get("date", ""))
    if not date_val:
        print("ERROR: No date specified")
        sys.exit(1)

    status = fields.get("status", "").strip().lower()
    if status not in ("postponed", "cancelled"):
        print(f"ERROR: Invalid status '{status}' (expected postponed or cancelled)")
        sys.exit(1)

    reason = fields.get("reason_optional", fields.get("reason", ""))
    if reason == "_No response_":
        reason = ""

    config = load_config()
    overrides = config.setdefault("game_overrides", {})
    team_overrides = overrides.setdefault(team, [])

    # Check for existing override on the same date and update it
    for entry in team_overrides:
        if entry.get("date") == date_val:
            entry["status"] = status
            if reason:
                entry["reason"] = reason
            elif "reason" in entry:
                del entry["reason"]
            print(f"Updated existing override for {team} on {date_val}: {status}")
            save_config(config)
            print("config.json updated successfully")
            return

    # Add new override
    entry = {"date": date_val, "status": status}
    if reason:
        entry["reason"] = reason
    team_overrides.append(entry)
    print(f"Added game override for {team} on {date_val}: {status}")

    save_config(config)
    print("config.json updated successfully")


if __name__ == "__main__":
    main()
