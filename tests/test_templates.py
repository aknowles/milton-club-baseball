"""The issue templates and the processors must agree on field names."""
import re
from pathlib import Path

from issue_utils import parse_issue_body

TEMPLATES = Path(__file__).resolve().parent.parent / ".github" / "ISSUE_TEMPLATE"

# For each template: the keys its processor needs to find.
REQUIRED_KEYS = {
    "add-practice.yml": {"team", "date", "time", "duration_minutes", "location", "event_title"},
    "cancel-practice.yml": {"team", "date_of_practice_to_cancel", "reason_optional"},
    "modify-practice.yml": {"team", "original_date", "new_date_leave_blank_if_unchanged",
                            "new_time_leave_blank_if_unchanged",
                            "new_location_leave_blank_if_unchanged"},
    "game-status.yml": {"team", "date_of_game", "status", "reason_optional"},
    "snack-signup.yml": {"team", "game_date", "family_names"},
    "snack-swap.yml": {"team", "your_family_name", "date", "replacing", "they_take_your_date"},
}


def template_keys(path):
    labels = re.findall(r"^\s+label:\s*(.+)$", path.read_text(encoding="utf-8"), re.M)
    rendered = "\n\n".join(f"### {label.strip().strip(chr(34))}\n\nx" for label in labels)
    return set(parse_issue_body(rendered))


def test_every_template_is_covered():
    assert {p.name for p in TEMPLATES.glob("*.yml")} == set(REQUIRED_KEYS)


def test_template_labels_match_processor_keys():
    for name, required in REQUIRED_KEYS.items():
        missing = required - template_keys(TEMPLATES / name)
        assert not missing, f"{name}: processor expects {missing}"
