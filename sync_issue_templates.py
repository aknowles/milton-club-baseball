#!/usr/bin/env python3
"""
Rewrite the Team dropdown in every issue template from config.json.

Run after adding, renaming or removing a team:

    python sync_issue_templates.py
"""

import glob
import json
import re

from teams import team_label


def main():
    with open("config.json") as f:
        teams = json.load(f).get("teams", [])
    # Newest season first, config order within a season
    teams = sorted(teams, key=lambda t: -t.get("year", 0))
    labels = [team_label(t) for t in teams]

    for path in sorted(glob.glob(".github/ISSUE_TEMPLATE/*.yml")):
        with open(path) as f:
            lines = f.read().split("\n")
        try:
            start = lines.index("    id: team")
        except ValueError:
            continue
        opts = next(i for i in range(start, len(lines))
                    if lines[i].strip() == "options:")
        end = opts + 1
        while end < len(lines) and re.match(r"\s+- ", lines[end]):
            end += 1
        indent = re.match(r"\s*", lines[opts]).group(0) + "  "
        lines[opts + 1:end] = [f'{indent}- "{label}"' for label in labels]
        with open(path, "w") as f:
            f.write("\n".join(lines))
        print(f"Updated {path} ({len(labels)} teams)")


if __name__ == "__main__":
    main()
