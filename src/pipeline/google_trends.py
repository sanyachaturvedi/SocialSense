# src/pipeline/google_trends.py

from pytrends.request import TrendReq
import json
from pathlib import Path

pytrends = TrendReq(hl='en-US', tz=360)

FASHION_KEYWORDS = [
    "fall outfits", "college outfits", "winter outfits", "aesthetic outfits",
    "streetwear", "grwm", "capsule wardrobe", "pinterest outfits"
]

STUDY_KEYWORDS = [
    "study vlog", "study with me", "pomodoro", "notion setup",
    "aesthetic notes", "study desk setup", "productivity", "student routines"
]


def get_rising_queries(keyword):
    """
    Get rising Google search queries for a given keyword.
    """
    try:
        pytrends.build_payload([keyword], timeframe='now 7-d')
        data = pytrends.related_queries()[keyword]["rising"]

        if data is None:
            return []

        results = []
        for _, row in data.iterrows():
            results.append({
                "keyword": keyword,
                "query": row.get("query"),
                "value": row.get("value")
            })

        return results

    except Exception as e:
        print(f"Failed for keyword: {keyword} — {e}")
        return []


def fetch_google_trends():
    """
    Fetch rising trends for fashion + study niches.
    """
    final_data = {
        "fashion": [],
        "study": []
    }

    for kw in FASHION_KEYWORDS:
        final_data["fashion"].extend(get_rising_queries(kw))

    for kw in STUDY_KEYWORDS:
        final_data["study"].extend(get_rising_queries(kw))

    output_path = Path(__file__).resolve().parents[2] / "data" / "raw" / "google_trends.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_data, f, indent=4, ensure_ascii=False)

    print(f"✅ Saved Google Trends data → {output_path}")
    return final_data


if __name__ == "__main__":
    fetch_google_trends()
