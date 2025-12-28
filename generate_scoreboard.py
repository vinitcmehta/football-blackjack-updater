import json
import os
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# -----------------------------
# Config
# -----------------------------
FPL_BASE = "https://fantasy.premierleague.com/api"
BOOTSTRAP_URL = f"{FPL_BASE}/bootstrap-static/"
ELEMENT_SUMMARY_URL = f"{FPL_BASE}/element-summary/{{element_id}}/"

OUTPUT_JSON_PATH = os.environ.get("OUTPUT_JSON_PATH", "scoreboard.json")
CUTOFF_ISO = os.environ.get("CUTOFF_ISO", "2025-12-25T00:00:00Z")

PICKS: Dict[str, List[str]] = {
    "Tommy": ["Grealish", "Neto", "Trossard", "Saka"],
    "Tiz":   ["Gordon", "Rice", "MGW", "Rogers"],
    "Matt":  ["Guehi", "Rice", "Gordon", "DCL"],
    "Vinit": ["Bowen", "Gyokeres", "Bruno G", "Pedro"],
}

# Aliases: shorthand -> something that exists in FPL (after normalization)
ALIASES: Dict[str, str] = {
    "mgw": "morgan gibbs-white",
    "dcl": "dominic calvert-lewin",
    "bruno g": "bruno guimaraes",
    "guehi": "marc guehi",

    # Fixes for your screenshot:
    # - Neto is ambiguous: choose the one you mean (most likely Pedro Neto)
    "neto": "pedro neto",
    # - Pedro alone won't match João Pedro
    "pedro": "joao pedro",
}

# If you ever want to hard-pin a pick to a specific FPL element id, do it here.
# This overrides all matching logic.
PLAYER_ID_OVERRIDES: Dict[str, int] = {
    # Example:
    # "Neto": 999,
    # "Pedro": 123,
    # "Bruno G": 456,
}

SLEEP_BETWEEN_REQUESTS_SEC = float(os.environ.get("SLEEP_BETWEEN_REQUESTS_SEC", "0.35"))

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://fantasy.premierleague.com/",
    "Origin": "https://fantasy.premierleague.com",
})


def fetch_json(url: str, max_retries: int = 6) -> Any:
    backoff = 1.2
    for attempt in range(1, max_retries + 1):
        try:
            r = SESSION.get(url, timeout=25)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == max_retries:
                raise
            sleep_s = backoff ** attempt
            print(f"[warn] fetch failed (attempt {attempt}/{max_retries}) url={url} err={e} -> sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)


def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    keep = []
    for ch in s:
        if ch.isalnum() or ch.isspace() or ch in "-'":
            keep.append(ch)
    return " ".join("".join(keep).split())


def parse_iso_z(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def tokenize(s: str) -> List[str]:
    return [t for t in norm(s).split() if t]


def build_player_data(elements: List[Dict[str, Any]]) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """
    Returns:
      - idx: normalized key -> list of candidate players
      - roster: list of players with normalized searchable fields for fuzzy matching
    """
    idx: Dict[str, List[Dict[str, Any]]] = {}
    roster: List[Dict[str, Any]] = []

    for e in elements:
        element_id = e.get("id")
        first = e.get("first_name") or ""
        second = e.get("second_name") or ""
        web = e.get("web_name") or ""

        full = f"{first} {second}".strip()
        display = full or web or second

        n_full = norm(full)
        n_web = norm(web)
        n_second = norm(second)

        roster.append({
            "id": element_id,
            "display": display,
            "n_full": n_full,
            "n_web": n_web,
            "n_second": n_second,
        })

        keys = {full, web, second}
        for k in keys:
            nk = norm(k)
            if not nk:
                continue
            idx.setdefault(nk, []).append({
                "id": element_id,
                "display": display,
            })

    return idx, roster


def resolve_pick_to_element(pick: str, idx: Dict[str, List[Dict[str, Any]]], roster: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Hard override if set
    if pick in PLAYER_ID_OVERRIDES:
        return {"status": "ok", "id": PLAYER_ID_OVERRIDES[pick], "display_name": pick}

    key = norm(pick)
    key = ALIASES.get(key, key)

    # 1) Exact lookup
    candidates = idx.get(key, [])
    if len(candidates) == 1:
        c = candidates[0]
        return {"status": "ok", "id": c["id"], "display_name": c["display"]}

    if len(candidates) > 1:
        return {
            "status": "ambiguous",
            "id": None,
            "display_name": pick,
            "suggestions": [c["display"] for c in candidates[:8]],
        }

    # 2) Fuzzy token match: all tokens must appear in n_full or n_web
    toks = tokenize(key)
    if not toks:
        return {"status": "not_found", "id": None, "display_name": pick}

    fuzzy = []
    for r in roster:
        haystacks = (r["n_full"], r["n_web"])
        if any(all(t in h.split() for t in toks) for h in haystacks if h):
            fuzzy.append(r)

    # If fuzzy found exactly one, accept it
    if len(fuzzy) == 1:
        return {"status": "ok", "id": fuzzy[0]["id"], "display_name": fuzzy[0]["display"]}

    if len(fuzzy) > 1:
        # Try narrowing: prefer full-name matches
        full_hits = [r for r in fuzzy if all(t in (r["n_full"].split() if r["n_full"] else []) for t in toks)]
        if len(full_hits) == 1:
            return {"status": "ok", "id": full_hits[0]["id"], "display_name": full_hits[0]["display"]}
        sugg = [r["display"] for r in (full_hits[:8] if full_hits else fuzzy[:8])]
        return {"status": "ambiguous", "id": None, "display_name": pick, "suggestions": sugg}

    return {"status": "not_found", "id": None, "display_name": pick}


def goals_since_cutoff(element_id: int, cutoff: datetime) -> int:
    time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)
    data = fetch_json(ELEMENT_SUMMARY_URL.format(element_id=element_id))

    total = 0
    for h in data.get("history", []):
        kt = h.get("kickoff_time")
        if not kt:
            continue
        kick = parse_iso_z(kt)
        if kick >= cutoff:
            total += int(h.get("goals_scored", 0) or 0)

    return total


def compute_scoreboard() -> Dict[str, Any]:
    cutoff = parse_iso_z(CUTOFF_ISO)

    bootstrap = fetch_json(BOOTSTRAP_URL)
    elements = bootstrap.get("elements", [])
    idx, roster = build_player_data(elements)

    participants = []
    for person, picks in PICKS.items():
        rows = []
        total = 0
        all_scored = True

        for pick in picks:
            resolved = resolve_pick_to_element(pick, idx, roster)

            if resolved["status"] != "ok":
                rows.append({
                    "display_name": resolved.get("display_name", pick),
                    "status": resolved["status"],
                    "goals": 0,
                    "suggestions": resolved.get("suggestions"),
                })
                all_scored = False
                continue

            g = goals_since_cutoff(int(resolved["id"]), cutoff)
            total += g
            if g <= 0:
                all_scored = False

            rows.append({
                "display_name": resolved.get("display_name", pick),
                "status": "ok",
                "goals": g,
            })

        is_bust = (total > 21) or (not all_scored)
        participants.append({
            "name": person,
            "players": rows,
            "total": total,
            "all_scored": all_scored,
            "is_bust": is_bust,
        })

    eligible = [p for p in participants if (not p["is_bust"]) and p["total"] <= 21 and p["all_scored"]]
    eligible.sort(key=lambda x: x["total"], reverse=True)
    leaderboard = [{"name": p["name"], "total": p["total"]} for p in eligible]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cutoff_iso": CUTOFF_ISO,
        "participants": participants,
        "leaderboard": leaderboard,
        "notes": [
            "Data source: Fantasy Premier League public endpoints.",
            "If you see 'ambiguous', pin it via ALIASES or PLAYER_ID_OVERRIDES in generate_scoreboard.py.",
        ],
    }


def main() -> None:
    payload = compute_scoreboard()
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Wrote {OUTPUT_JSON_PATH}")


if __name__ == "__main__":
    main()
