#fetch trending youtube videos and save to json
# src/pipeline/youtube_fetch.py

import json
from pathlib import Path

import requests

from src.pipeline.config import YOUTUBE_API_KEY, REGION_CODE, MAX_RESULTS


def fetch_youtube_trending():
    """
    Fetch trending YouTube videos using YouTube Data API v3.
    Saves results to data/raw/youtube.json
    """
    url = "https://www.googleapis.com/youtube/v3/videos"

    params = {
        "part": "snippet,statistics",
        "chart": "mostPopular",
        "regionCode": REGION_CODE,
        "maxResults": MAX_RESULTS,
        "key": YOUTUBE_API_KEY,
    }

    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        raise Exception(f"YouTube API error {response.status_code}: {response.text}")

    data = response.json()

    cleaned = []
    for item in data.get("items", []):
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        thumbs = snippet.get("thumbnails", {})

        cleaned.append(
            {
                "video_id": item.get("id"),
                "title": snippet.get("title"),
                "channel": snippet.get("channelTitle"),
                "publishTime": snippet.get("publishedAt"),
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)) if "likeCount" in stats else None,
                "thumbnail": (thumbs.get("high") or thumbs.get("default") or {}).get("url"),
            }
        )

    # Save to /data/raw/youtube.json
    output_path = Path(__file__).resolve().parents[2] / "data" / "raw" / "youtube.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=4, ensure_ascii=False)

    print(f"✅ Saved {len(cleaned)} trending videos → {output_path}")
    return cleaned


if __name__ == "__main__":
    fetch_youtube_trending()
