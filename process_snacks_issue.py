#!/usr/bin/env python3
"""
Process a GitHub issue for snack signups and swaps.

Parses the issue body (GitHub issue form format) and updates config.json.
Triggered by the process-snack-signups workflow when an admin comments /approve.
"""

import json
import os
import re
import sys
import urllib.request


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


def send_ntfy(config, team, message_title, message_body):
    """Send ntfy notification for a team."""
    team_cfg = next((t for t in config.get("teams", []) if t["team_name"] == team), None)
    ntfy_topic = team_cfg.get("ntfy_topic") if team_cfg else None
    if ntfy_topic:
        try:
            req = urllib.request.Request(
                f"https://ntfy.sh/{ntfy_topic}",
                data=message_body.encode("utf-8"),
                headers={"Title": message_title, "Priority": "default", "Tags": "baseball"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                print(f"ntfy sent to {ntfy_topic}: {message_title}")
        except Exception as e:
            print(f"ntfy send failed for {ntfy_topic}: {e}")


def process_signup(fields, config):
    """Process a snack signup issue."""
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

    family_str = ", ".join(families)
    send_ntfy(config, team, f"Snack Signup: {date_val}", f"{family_str} signed up for snacks on {date_val}")


def process_swap(fields, config):
    """Process a snack swap — one family replaces another, with optional return swap."""
    team = fields.get("team", "")
    if not team:
        print("ERROR: No team specified")
        sys.exit(1)

    family_name = fields.get("family_name", "").strip()
    if not family_name:
        print("ERROR: No family name specified")
        sys.exit(1)

    swap_to_date = fields.get("swap_to_date", "").strip()
    if not swap_to_date:
        print("ERROR: No date specified")
        sys.exit(1)

    swap_with_family = fields.get("swap_with_family", "").strip()
    if not swap_with_family or swap_with_family == "_No response_":
        print("ERROR: No family to replace specified")
        sys.exit(1)

    current_date = fields.get("currently_assigned_date", "").strip()
    if current_date == "_No response_":
        current_date = ""

    snacks = config.setdefault("snacks", {})
    team_snacks = snacks.setdefault(team, [])

    # Find entries
    target_entry = None
    current_entry = None
    for entry in team_snacks:
        if entry.get("date") == swap_to_date:
            target_entry = entry
        if current_date and entry.get("date") == current_date:
            current_entry = entry

    if not target_entry:
        print(f"ERROR: No snack entry found for {team} on {swap_to_date}")
        sys.exit(1)

    if swap_with_family not in target_entry.get("families", []):
        print(f"ERROR: {swap_with_family} is not assigned to {swap_to_date}")
        sys.exit(1)

    if current_date:
        if not current_entry:
            print(f"ERROR: No snack entry found for {team} on {current_date}")
            sys.exit(1)
        if family_name not in current_entry.get("families", []):
            print(f"ERROR: {family_name} is not assigned to {current_date}")
            sys.exit(1)

    # On the target date: replace swap_with_family with family_name
    target_entry["families"].remove(swap_with_family)
    target_entry["families"].append(family_name)

    # If return swap: also move swap_with_family onto the current date, remove family_name
    if current_date and current_entry:
        current_entry["families"].remove(family_name)
        current_entry["families"].append(swap_with_family)
        print(f"Swapped: {family_name} ({current_date}) <-> {swap_with_family} ({swap_to_date})")
        ntfy_title = f"Snack Swap: {family_name} \u2194 {swap_with_family}"
        ntfy_body = f"{family_name} ({current_date}) swapped with {swap_with_family} ({swap_to_date})"
    else:
        print(f"Swap: {family_name} replaces {swap_with_family} on {swap_to_date}")
        ntfy_title = f"Snack Swap: {family_name} replaces {swap_with_family}"
        ntfy_body = f"{family_name} replaces {swap_with_family} on {swap_to_date}"

    save_config(config)
    print("config.json updated successfully")
    send_ntfy(config, team, ntfy_title, ntfy_body)


def main():
    issue_body = os.environ.get("ISSUE_BODY", "")
    if not issue_body:
        print("ERROR: No issue body found")
        sys.exit(1)

    issue_title = os.environ.get("ISSUE_TITLE", "")

    fields = parse_issue_body(issue_body)
    print(f"Parsed fields: {json.dumps(fields, indent=2)}")

    config = load_config()

    # Determine if this is a swap or a signup based on title
    if "[Snacks] Swap" in issue_title:
        process_swap(fields, config)
    else:
        process_signup(fields, config)


if __name__ == "__main__":
    main()
