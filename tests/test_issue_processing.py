import copy

import pytest

from issue_utils import (IssueError, parse_date, parse_family_names,
                         parse_issue_body, parse_time)
from process_game_status_issue import apply_game_status
from process_practice_issue import action_from_title, apply_practice_change
from process_snacks_issue import apply_signup, apply_swap

CONFIG = {
    "teams": [
        {"id": "knights-11u", "team_name": "Knights 11U", "year": 2026},
        {"id": "knights-11u-2027", "team_name": "Knights 11U", "year": 2027,
         "coach": "Wagner"},
    ],
    "snacks": {"knights-11u": [
        {"date": "2026-04-18", "families": ["Phillips", "Dow"]},
        {"date": "2026-03-28", "families": ["Knowles"]},
    ]},
}


@pytest.fixture
def config():
    return copy.deepcopy(CONFIG)


def body(**fields):
    return "\n\n".join(f"### {k}\n\n{v}" for k, v in fields.items())


# --- parsing -----------------------------------------------------------

def test_parse_issue_body_normalises_labels():
    fields = parse_issue_body(
        "### Team\n\nKnights 11U (2026)\n\n### Duration (minutes)\n\n90\n\n"
        "### Family Name(s)\n\nSmith\n\n### Reason (optional)\n\n_No response_")
    assert fields == {"team": "Knights 11U (2026)", "duration_minutes": "90",
                      "family_names": "Smith", "reason_optional": ""}


def test_parse_issue_body_first_heading_wins():
    """A heading typed into a later free-text box can't override a field."""
    fields = parse_issue_body(
        "### Team\n\nKnights 11U (2026)\n\n### Notes\n\nhi\n### Team\n\nOther")
    assert fields["team"] == "Knights 11U (2026)"


@pytest.mark.parametrize("bad", ["", "04/18/2026", "2026-13-01", "2026-04-18<b>", "tomorrow"])
def test_parse_date_rejects(bad):
    with pytest.raises(IssueError):
        parse_date(bad)


def test_parse_time_formats():
    assert parse_time("5:30 PM") == "5:30 PM"
    assert parse_time("5:30pm") == "5:30 PM"
    assert parse_time("17:30") == "5:30 PM"
    with pytest.raises(IssueError):
        parse_time("soon")


def test_parse_family_names():
    assert parse_family_names("Smith,  O'Brien , St. Pierre, Smith-Jones") == [
        "Smith", "O'Brien", "St. Pierre", "Smith-Jones"]
    for bad in ["<script>alert(1)</script>", 'a" onmouseover="x', "x" * 41]:
        with pytest.raises(IssueError):
            parse_family_names(bad)


# --- game status -------------------------------------------------------

def test_game_status_add_then_update(config):
    fields = parse_issue_body(body(**{
        "Team": "Knights 11U (2026)", "Date of game": "2026-05-09",
        "Status": "Postponed", "Reason (optional)": "Rain"}))
    apply_game_status(fields, config)
    assert config["game_overrides"]["knights-11u"] == [
        {"date": "2026-05-09", "status": "postponed", "reason": "Rain"}]

    fields["status"] = "cancelled"
    fields["reason_optional"] = ""
    apply_game_status(fields, config)
    assert config["game_overrides"]["knights-11u"] == [
        {"date": "2026-05-09", "status": "cancelled"}]


@pytest.mark.parametrize("override", [
    {"Team": "Nobody"}, {"Date of game": "May 9"}, {"Status": "moved"},
    {"Reason (optional)": "x" * 500}])
def test_game_status_rejects_bad_input(config, override):
    data = {"Team": "knights-11u", "Date of game": "2026-05-09",
            "Status": "postponed", "Reason (optional)": ""}
    data.update(override)
    with pytest.raises(IssueError):
        apply_game_status(parse_issue_body(body(**data)), config)
    assert "game_overrides" not in config or not config["game_overrides"].get("knights-11u")


# --- practices ---------------------------------------------------------

def test_action_comes_from_title_prefix_only():
    assert action_from_title("[Practice] Add: makeup for cancelled practice") == "add"
    assert action_from_title("[Practice] Cancel: Tuesday") == "cancel"
    assert action_from_title("[Practice] Modify: add 30 minutes") == "modify"
    assert action_from_title("[Practice] something else") is None


def test_practice_add(config):
    fields = parse_issue_body(body(**{
        "Team": "Knights 11U – Coach Wagner (2027)", "Date": "2027-04-15",
        "Time": "5:30 pm", "Duration (minutes)": "60",
        "Location": "Cunningham  Park", "Event Title": "_No response_"}))
    apply_practice_change(fields, "[Practice] Add: bullpen", config)
    assert config["practices"]["knights-11u-2027"]["adhoc"] == [{
        "date": "2027-04-15", "time": "5:30 PM", "duration_minutes": 60,
        "location": "Cunningham Park"}]


def test_practice_cancel_and_modify(config):
    apply_practice_change(
        parse_issue_body(body(**{"Team": "knights-11u",
                                 "Date of practice to cancel": "2026-04-01"})),
        "[Practice] Cancel: x", config)
    apply_practice_change(
        parse_issue_body(body(**{
            "Team": "knights-11u", "Original Date": "2026-04-02",
            "New Date (leave blank if unchanged)": "_No response_",
            "New Time (leave blank if unchanged)": "6:00 PM",
            "New Location (leave blank if unchanged)": ""})),
        "[Practice] Modify: x", config)
    assert config["practices"]["knights-11u"]["modifications"] == [
        {"date": "2026-04-01", "action": "cancel"},
        {"date": "2026-04-02", "action": "reschedule", "new_time": "6:00 PM"}]


@pytest.mark.parametrize("fields", [
    {"Team": "knights-11u", "Date": "soon", "Time": "5:30 PM"},
    {"Team": "knights-11u", "Date": "2026-04-15", "Time": "25:99"},
    {"Team": "knights-11u", "Date": "2026-04-15", "Time": "5:30 PM",
     "Duration (minutes)": "-5"},
])
def test_practice_add_rejects_bad_input(config, fields):
    with pytest.raises(IssueError):
        apply_practice_change(parse_issue_body(body(**fields)),
                              "[Practice] Add: x", config)


# --- snacks ------------------------------------------------------------

def test_signup_merges_into_existing_date(config):
    apply_signup(parse_issue_body(body(**{
        "Team": "Knights 11U (2026)", "Game Date": "2026-04-18",
        "Family Name(s)": "Dow, Hynes"})), config)
    assert config["snacks"]["knights-11u"][0]["families"] == [
        "Phillips", "Dow", "Hynes"]


def test_swap_from_web_page_picker(config):
    """Headings as pre-filled by the page's swap picker."""
    apply_swap(parse_issue_body(body(**{
        "Team": "Knights 11U (2026)", "Family Name": "Knowles",
        "Currently Assigned Date": "2026-03-28", "Swap To Date": "2026-04-18",
        "Swap With Family": "Phillips"})), config)
    snacks = {e["date"]: e["families"] for e in config["snacks"]["knights-11u"]}
    assert snacks == {"2026-04-18": ["Dow", "Knowles"], "2026-03-28": ["Phillips"]}


def test_swap_from_github_issue_template(config):
    """Headings as produced by .github/ISSUE_TEMPLATE/snack-swap.yml."""
    apply_swap(parse_issue_body(body(**{
        "Team": "Knights 11U (2026)", "Your Family Name": "Knowles",
        "Date": "2026-04-18", "Replacing": "Phillips",
        "They Take Your Date": "_No response_", "Notes": "_No response_"})),
        config)
    assert config["snacks"]["knights-11u"][0]["families"] == ["Dow", "Knowles"]
    assert config["snacks"]["knights-11u"][1]["families"] == ["Knowles"]


def test_swap_rejects_unassigned_family(config):
    before = copy.deepcopy(config)
    with pytest.raises(IssueError):
        apply_swap(parse_issue_body(body(**{
            "Team": "knights-11u", "Family Name": "Knowles",
            "Swap To Date": "2026-04-18", "Swap With Family": "Nobody"})), config)
    assert config == before
