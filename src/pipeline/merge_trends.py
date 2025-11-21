# src/pipeline/merge_trends.py

import json
import re
from pathlib import Path
from datetime import datetime, timezone

# Keep these aligned with your Phase 1 niches
FASHION_KEYWORDS = [
    "fall outfits", "college outfits", "winter outfits", "aesthetic outfits",
    "streetwear", "grwm", "capsule wardrobe", "pinterest outfits",
    "outfit ideas", "fashion trends"
]

STUDY_KEYWORDS = [
    "study vlog", "study with me", "pomodoro", "notion setup",
    "aesthetic notes", "study desk setup", "productivity",
    "student routines", "study tips"
]


def _root():
    return Path(__file__).resolve().parents[2]


def normalize(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def days_since(iso_time: str) -> float:
    try:
        dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return (now - dt).total_seconds() / (3600 * 24)
    except Exception:
        return 999.0


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def merge_for_niche(youtube_items, google_items, base_keywords):
    """
    Merges YouTube trends + Google rising queries for one niche.
    Returns dict: trend_name -> components
    """
    merged = {}

    # ---- YouTube -> map videos to nearest base keyword (simple substring match) ----
    for v in youtube_items:
        title = normalize(v.get("title", ""))
        tags = " ".join([normalize(t) for t in (v.get("tags") or [])])

        matched_kw = None
        for kw in base_keywords:
            nkw = normalize(kw)
            if nkw in title or nkw in tags:
                matched_kw = kw
                break

        if not matched_kw:
            continue

        trend = merged.setdefault(matched_kw, {
            "trend": matched_kw,
            "youtube_views_total": 0,
            "youtube_engagement_sum": 0.0,
            "youtube_count": 0,
            "youtube_recency_days_sum": 0.0,
            "google_rise_total": 0,
            "examples": []
        })

        views = int(v.get("views", 0) or 0)
        likes = v.get("likes")
        likes = int(likes) if likes is not None else 0

        engagement = (likes / views) if views > 0 else 0.0
        recency_days = days_since(v.get("publishTime", ""))

        trend["youtube_views_total"] += views
        trend["youtube_engagement_sum"] += engagement
        trend["youtube_recency_days_sum"] += recency_days
        trend["youtube_count"] += 1

        if len(trend["examples"]) < 3:
            trend["examples"].append({
                "title": v.get("title"),
                "video_id": v.get("video_id"),
                "channel": v.get("channel"),
                "views": views,
                "likes": likes
            })

    # ---- Google Trends -> map rising queries to base keyword if possible ----
    for row in google_items:
        query = normalize(row.get("query", ""))
        value = int(row.get("value", 0) or 0)

        matched_kw = None
        for kw in base_keywords:
            nkw = normalize(kw)
            if nkw in query:
                matched_kw = kw
                break

        trend_name = matched_kw if matched_kw else row.get("query")
        if not trend_name:
            continue

        trend = merged.setdefault(trend_name, {
            "trend": trend_name,
            "youtube_views_total": 0,
            "youtube_engagement_sum": 0.0,
            "youtube_count": 0,
            "youtube_recency_days_sum": 0.0,
            "google_rise_total": 0,
            "examples": []
        })

        trend["google_rise_total"] += value

    # ---- finalize averages ----
    for t in merged.values():
        if t["youtube_count"] > 0:
            t["youtube_engagement_avg"] = t["youtube_engagement_sum"] / t["youtube_count"]
            t["youtube_recency_days_avg"] = t["youtube_recency_days_sum"] / t["youtube_count"]
        else:
            t["youtube_engagement_avg"] = 0.0
            t["youtube_recency_days_avg"] = 999.0

        # remove internal sums
        t.pop("youtube_engagement_sum", None)
        t.pop("youtube_recency_days_sum", None)

    return merged


def merge_trends():
    root = _root()

    yt_path = root / "data" / "raw" / "youtube.json"
    g_path = root / "data" / "raw" / "google_trends.json"

    youtube_items = load_json(yt_path)
    google_data = load_json(g_path)

    fashion_google = google_data.get("fashion", [])
    study_google = google_data.get("study", [])

    merged_all = {
        "fashion": merge_for_niche(youtube_items, fashion_google, FASHION_KEYWORDS),
        "study": merge_for_niche(youtube_items, study_google, STUDY_KEYWORDS)
    }

    out_path = root / "data" / "processed" / "merged_trends.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(merged_all, f, indent=4, ensure_ascii=False)

    print(f"✅ Merged trends saved → {out_path}")
    return merged_all


if __name__ == "__main__":
    merge_trends()
