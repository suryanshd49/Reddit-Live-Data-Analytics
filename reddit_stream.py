""" reddit_producer.py
Fetches latest Reddit posts and writes them as JSON files to ./reddit_data
"""
import os, time, json, requests
from datetime import datetime

OUTPUT_DIR = "./reddit_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

SUBREDDITS = ["technology", "worldnews", "science", "stocks", "cryptocurrency", "India"]
INTERVAL = 5  # seconds
LIMIT = 10    # posts per subreddit per fetch

def fetch_reddit_posts():
    all_posts = []
    headers = {"User-Agent": "Mozilla/5.0"}
    for sub in SUBREDDITS:
        try:
            url = f"https://www.reddit.com/r/{sub}/new.json?limit={LIMIT}"
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code != 200:
                print(f"[reddit_producer] Warning: {sub} returned {res.status_code}")
                continue

            data = res.json()
            for post in data.get("data", {}).get("children", []):
                p = post["data"]
                all_posts.append({
                    "subreddit": sub,
                    "title": p.get("title", ""),
                    "author": p.get("author", "unknown"),
                    "score": p.get("score", 0),
                    "created_utc": p.get("created_utc", time.time()),
                    "url": p.get("url", ""),
                    "num_comments": p.get("num_comments", 0),
                    "upvote_ratio": p.get("upvote_ratio", 0.0),
                    "domain": p.get("domain", ""),
                    "link_flair_text": p.get("link_flair_text", "None")
                })
        except Exception as e:
            print(f"[reddit_producer] Error fetching {sub}: {e}")
    return all_posts


def main():
    while True:
        posts = fetch_reddit_posts()
        if posts:
            timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            file_path = os.path.join(OUTPUT_DIR, f"reddit_{timestamp}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                for post in posts:
                    f.write(json.dumps(post, ensure_ascii=False) + "\n")
            print(f"[reddit_producer] Wrote {len(posts)} posts → {file_path}")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
