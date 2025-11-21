# src/pipeline/score_trends.py

import json
from pathlib import Path

# B-lite weights (realistic MVP)
W_VIEWS = 0.4
W_ENGAGE = 0.3
W_FRESH = 0.1
W_GOOGLE = 0.2


def _root():
    return Path(__file__).resolve().parents[2]


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def min_max_norm(values):
    if not values:
        return {}
    mn, mx = min(values), max(values)
    if mx == mn:
        return {i: 0.0 for i in range(len(values))}
    return {i: (v - mn) / (mx - mn) for i, v in enumerate(values)}


def score_niche(trend_dict):
    trends = list(trend_dict.values())

    views_vals = [t.get("youtube_views_total", 0) for t in trends]
    engage_vals = [t.get("youtube_engagement_avg", 0.0) for t in trends]
    # Convert recency days -> freshness (smaller days = fresher)
    fresh_vals = [1.0 / (1.0 + (t.get("youtube_recency_days_avg", 999.0))) for t in trends]
    google_vals = [t.get("google_rise_total", 0) for t in trends]

    views_norm = min_max_norm(views_vals)
    engage_norm = min_max_norm(engage_vals)
    fresh_norm = min_max_norm(fresh_vals)
    google_norm = min_max_norm(google_vals)

    scored = []
    for i, t in enumerate(trends):
        score = (
            W_VIEWS * views_norm[i] +
            W_ENGAGE * engage_norm[i] +
            W_FRESH * fresh_norm[i] +
            W_GOOGLE * google_norm[i]
        )

        scored.append({
            "trend": t["trend"],
            "trend_score": round(score * 100, 2),
            "youtube_views_total": t.get("youtube_views_total", 0),
            "youtube_engagement_avg": round(t.get("youtube_engagement_avg", 0.0), 4),
            "youtube_recency_days_avg": round(t.get("youtube_recency_days_avg", 999.0), 2),
            "google_rise_total": t.get("google_rise_total", 0),
            "examples": t.get("examples", [])
        })

    scored.sort(key=lambda x: x["trend_score"], reverse=True)
    return scored


def score_trends():
    root = _root()
    merged_path = root / "data" / "processed" / "merged_trends.json"
    merged = load_json(merged_path)

    fashion_scored = score_niche(merged.get("fashion", {}))
    study_scored = score_niche(merged.get("study", {}))

    final = {
        "fashion": fashion_scored,
        "study": study_scored
    }

    out_path = root / "data" / "processed" / "trends.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(final, f, indent=4, ensure_ascii=False)

    print(f"✅ Final scored trends saved → {out_path}")
    return final


if __name__ == "__main__":
    score_trends()
