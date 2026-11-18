"""
Helpers shared by the issue processors: parsing GitHub issue-form bodies,
validating what people typed, and reading/writing config.json.

Issue text is untrusted input that ends up in config.json, the published
calendars and the public web page, so every value is validated here before
it is stored.
"""

import json
import re
import sys
from datetime import datetime

NO_RESPONSE = "_No response_"
MAX_TEXT_LEN = 200


class IssueError(Exception):
    """The issue can't be processed; the message is safe to show the user."""


def fail(message):
    """Print an error for the workflow log and exit non-zero."""
    print(f"ERROR: {message}")
    sys.exit(1)


def parse_issue_body(body):
    """Parse a GitHub issue form body into a {field_key: value} dict.

    Issue form bodies look like:

        ### Field Label

        value

    Labels are normalised to snake_case with brackets dropped, so
    "Duration (minutes)" becomes ``duration_minutes``. If a heading appears
    twice, the first one wins: form fields come in template order, so a
    "### Team" typed into a later free-text box can't replace the real one.
    """
    fields = {}
    current_key = None
    current_lines = []

    def flush():
        if current_key is not None and current_key not in fields:
            value = "\n".join(current_lines).strip()
            fields[current_key] = "" if value == NO_RESPONSE else value

    for line in body.replace("\r\n", "\n").split("\n"):
        header = re.match(r"^###\s+(.+)$", line.strip())
        if header:
            flush()
            key = header.group(1).strip().lower()
            key = re.sub(r"[()]", "", key)
            key = re.sub(r"[^a-z0-9]+", "_", key).strip("_")
            current_key = key
            current_lines = []
        elif current_key is not None:
            current_lines.append(line)
    flush()
    return fields


def first_field(fields, *keys):
    """Return the first non-empty value among *keys* ('' when none)."""
    for key in keys:
        value = fields.get(key, "")
        if value:
            return value
    return ""


def clean_text(value, what, max_len=MAX_TEXT_LEN):
    """Collapse whitespace, drop control characters and enforce a length cap."""
    text = "".join(ch for ch in (value or "") if ch.isprintable() or ch.isspace())
    text = " ".join(text.split())
    if len(text) > max_len:
        raise IssueError(f"{what} is too long (max {max_len} characters)")
    return text


def parse_date(value, what="Date"):
    """Validate a YYYY-MM-DD date and return it in canonical form."""
    text = (value or "").strip()
    try:
        return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise IssueError(f"{what} '{text[:40]}' is not a valid YYYY-MM-DD date") from None


def parse_time(value, what="Time"):
    """Validate a time such as '5:30 PM' or '17:30'; returns e.g. '5:30 PM'."""
    text = " ".join((value or "").upper().replace(".", "").split())
    text = re.sub(r"(?<=\d)(AM|PM)$", r" \1", text)
    for fmt in ("%I:%M %p", "%H:%M"):
        try:
            return datetime.strptime(text, fmt).strftime("%I:%M %p").lstrip("0")
        except ValueError:
            continue
    raise IssueError(f"{what} '{text[:40]}' is not a valid time (use H:MM AM/PM)")


def parse_family_names(value, what="Family name"):
    """Split a comma-separated list of family names and validate each one."""
    names = []
    for raw in (value or "").split(","):
        name = clean_text(raw, what, max_len=40)
        if not name:
            continue
        if not re.fullmatch(r"[^\W\d_](?:[^\W\d_]|[ '’.\-])*", name):
            raise IssueError(
                f"{what} '{name}' may only contain letters, spaces, "
                f"apostrophes, periods and hyphens")
        if name not in names:
            names.append(name)
    return names


def load_config(path="config.json"):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_config(config, path="config.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
        f.write("\n")
