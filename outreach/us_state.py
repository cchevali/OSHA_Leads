from __future__ import annotations

US_STATE_NAME_TO_ABBR: dict[str, str] = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "district of columbia": "DC",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}
US_STATE_ABBREVIATIONS: set[str] = set(US_STATE_NAME_TO_ABBR.values())
DEFAULT_OUTREACH_STATES: tuple[str, ...] = (
    "TX",
    "CA",
    "FL",
    "PA",
    "OH",
    "IL",
    "NJ",
    "LA",
    "MI",
    "GA",
    "AL",
    "WI",
    "TN",
)
DEFAULT_OUTREACH_STATE_CSV = ",".join(DEFAULT_OUTREACH_STATES)


def normalize_state_token(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    direct = text.upper()
    if len(direct) == 2 and direct.isalpha():
        return direct
    normalized_name = " ".join(text.lower().split())
    return US_STATE_NAME_TO_ABBR.get(normalized_name, direct)


def normalize_us_state(value: str) -> str:
    token = normalize_state_token(value)
    if token in US_STATE_ABBREVIATIONS:
        return token
    return ""


def parse_state_csv(raw: str, *, strict_us: bool = True) -> list[str]:
    out: list[str] = []
    for item in str(raw or "").split(","):
        state = normalize_us_state(item) if strict_us else normalize_state_token(item)
        if not state:
            continue
        if state not in out:
            out.append(state)
    return out
