#!/usr/bin/env python3
"""
Process a GitHub issue for snack signups.

Parses the issue body (GitHub issue form format) and updates config.json.
Triggered by the process-snack-signups workflow when an admin comments /approve.
"""

import json
import os
import re
import sys


def parse_issue_body(body):
    """Parse GitHub issue form body into field dict."""
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

    date_val = fields.get("game_date", fields.get("date", ""))
    if not date_val:
        print("ERROR: No date specified")
        sys.exit(1)

    families_raw = fields.get("family_names", fields.get("families", ""))
    if not families_raw or families_raw == "_No response_":
        print("ERROR: No families specified")
        sys.exit(1)

    families = [f.strip() for f in families_raw.split(",") if f.strip()]
    if not families:
        print("ERROR: No valid family names after parsing")
        sys.exit(1)

    config = load_config()
    snacks = config.setdefault("snacks", {})
    team_snacks = snacks.setdefault(team, [])

    # Check if there's already an entry for this date — merge families
    existing = None
    for entry in team_snacks:
        if entry.get("date") == date_val:
            existing = entry
            break

    if existing:
        for fam in families:
            if fam not in existing["families"]:
                existing["families"].append(fam)
        print(f"Merged families into existing snack entry for {team} on {date_val}")
    else:
        team_snacks.append({"date": date_val, "families": families})
        print(f"Added snack signup for {team} on {date_val}: {families}")

    save_config(config)
    print("config.json updated successfully")


if __name__ == "__main__":
    main()
