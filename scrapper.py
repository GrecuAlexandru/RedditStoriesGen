import os
import json
import sqlite3
import datetime
import re
import requests
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

SUBREDDITS = ["TrueOffMyChest", "AmItheAsshole",
              "NuclearRevenge", "pettyrevenge"]
DB_PATH = os.path.join(os.path.dirname(__file__), "posts.db")
ABBREVIATIONS_PATH = os.path.join(
    os.path.dirname(__file__), "abbreviations.json")

MIN_LENGTH = 500
MAX_LENGTH = 3000

REMOVAL_STRINGS = {
    '[removed]',
    '[deleted]',
    '[ Removed by moderator ]',
    'Comment removed by moderator',
    '[Removed by Reddit]',
    '[Removed by Reddit filters]'
}

XAI_API_KEY = os.getenv("XAI_API_KEY")
QUEUE_SCORE_THRESHOLD = int(os.getenv("QUEUE_SCORE_THRESHOLD", "7"))
FALLBACK_SCORE_NO_XAI = int(
    os.getenv("FALLBACK_SCORE_NO_XAI", str(QUEUE_SCORE_THRESHOLD))
)
FALLBACK_SCORE_ON_ERROR = int(
    os.getenv("FALLBACK_SCORE_ON_ERROR", str(QUEUE_SCORE_THRESHOLD))
)

SYSTEM_PROMPT = """You are an expert social media content strategist specializing in YouTube Shorts and TikTok trends. Your task is to evaluate a batch of Reddit stories for viral potential.

Criteria for a high score (7-10):
- High emotional hook in the first 2 sentences.
- Clear conflict or drama.
- Strong pacing with satisfying or shocking developments.
- The post should be mainly in English.

Rules:
- You must evaluate each post and output valid JSON.
- Provide a single integer score between 1 and 10 for each post.
- Treat Reddit/internet abbreviations as their full meaning during evaluation (example: AITA = Am I The Asshole, TIFU = Today I Fucked Up).
- You must respond ONLY with a valid JSON object formatted exactly like this:
{
  "results": [
    {"index": 0, "score": 8},
    {"index": 1, "score": 4}
  ]
}"""

METADATA_SYSTEM_PROMPT = """You are a viral short-form copywriter for YouTube Shorts and TikTok.

Given one Reddit story, create platform-ready metadata.

Rules:
- Return ONLY valid JSON with keys: youtube_title, youtube_description, tiktok_description, hashtags.
- hashtags must be an array of strings, each starting with # and no spaces.
- Keep youtube_title <= 90 characters.
- Keep descriptions concise and engagement-focused.
- Expand abbreviations to full words in outputs when natural for readability.
"""

POLICY_SAFETY_PROMPT = """You are a content safety classifier for short-form social media narration.

Given a Reddit title and story content, decide if the story is safe for general audience short-form posting.

Mark unsafe if it includes severe policy risk such as:
- sexual content involving minors
- explicit graphic violence/gore
- hate speech or slurs targeting protected groups
- detailed self-harm/suicide encouragement
- doxxing/private personal information exposure
- Interpret abbreviations as their full meaning when classifying safety.

Respond ONLY as valid JSON in this exact format:
{
  "safe": true,
  "reason": "short reason"
}
"""


def _load_abbreviations() -> dict[str, str]:
    if not os.path.exists(ABBREVIATIONS_PATH):
        return {}
    try:
        with open(ABBREVIATIONS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        return {str(k): str(v) for k, v in data.items()}
    except Exception:
        return {}


ABBREVIATIONS_MAP = _load_abbreviations()
ABBREVIATION_KEYS_SORTED = sorted(
    ABBREVIATIONS_MAP.keys(), key=len, reverse=True)


def get_abbreviation_reference_text(max_items: int = 40) -> str:
    if not ABBREVIATIONS_MAP:
        return "No abbreviation map provided."
    items = list(ABBREVIATIONS_MAP.items())[:max_items]
    pairs = [f"{key}={value}" for key, value in items]
    return "Known abbreviations: " + "; ".join(pairs)


def _ensure_column_exists(cursor: sqlite3.Cursor, table_name: str, column_name: str, ddl: str):
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    if column_name not in existing_columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {ddl}")


def setup_database():
    """Initializes the SQLite database and creates tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS QueuedPosts (
            title TEXT,
            content TEXT,
            subreddit TEXT,
            score INTEGER,
            metadata_json TEXT,
            usedYet BOOLEAN,
            createdAt TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS BadPosts (
            title TEXT,
            content TEXT,
            subreddit TEXT,
            score INTEGER,
            metadata_json TEXT,
            createdAt TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS SchedulerState (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    _ensure_column_exists(cursor, "QueuedPosts",
                          "metadata_json", "metadata_json TEXT")
    _ensure_column_exists(cursor, "BadPosts",
                          "metadata_json", "metadata_json TEXT")

    conn.commit()
    return conn


def get_scheduler_state(conn: sqlite3.Connection, key: str) -> Optional[str]:
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM SchedulerState WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row[0] if row else None


def set_scheduler_state(conn: sqlite3.Connection, key: str, value: str):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO SchedulerState (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()


def get_last_fetch_time(conn: sqlite3.Connection) -> Optional[datetime.datetime]:
    raw_value = get_scheduler_state(conn, "last_fetch_time")
    if not raw_value:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(
            raw_value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def set_last_fetch_time(conn: sqlite3.Connection, when_utc: Optional[datetime.datetime] = None):
    fetch_time = when_utc or datetime.datetime.now(datetime.timezone.utc)
    iso_value = fetch_time.replace(
        microsecond=0).isoformat().replace("+00:00", "Z")
    set_scheduler_state(conn, "last_fetch_time", iso_value)


def is_removed_or_whitespace(text: str) -> bool:
    """Checks if the text is empty, whitespace, or contains removal strings."""
    if not text:
        return True
    if text.strip() == "":
        return True
    if text in REMOVAL_STRINGS:
        return True
    return False


def expand_abbreviations(text: str) -> str:
    if not text:
        return text
    if not ABBREVIATION_KEYS_SORTED:
        return text

    expanded_text = text
    for key in ABBREVIATION_KEYS_SORTED:
        value = ABBREVIATIONS_MAP[key]
        pattern = re.compile(
            rf"(?<!\\w){re.escape(key)}(?!\\w)", flags=re.IGNORECASE)
        expanded_text = pattern.sub(value, expanded_text)
    return expanded_text


def is_post_policy_safe(title: str, content: str) -> tuple[bool, str]:
    if not XAI_API_KEY:
        return True, "policy_check_skipped_no_xai_key"

    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {XAI_API_KEY}",
    }
    user_prompt = (
        f"Title: {title}\n"
        f"Content: {content}\n\n"
        f"{get_abbreviation_reference_text()}\n\n"
        "Classify this content for policy safety."
    )
    payload = {
        "model": "grok-4-1-fast-non-reasoning",
        "messages": [
            {"role": "system", "content": POLICY_SAFETY_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0,
    }

    try:
        response = requests.post(url, headers=headers,
                                 json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        content_str = data["choices"][0]["message"]["content"]
        parsed = json.loads(content_str)
        safe = bool(parsed.get("safe", False))
        reason = str(parsed.get("reason", ""))[:300]
        return safe, reason or "no_reason"
    except Exception as e:
        print(
            f"Policy safety check failed; allowing post by fallback. Error: {e}")
        return True, "policy_check_failed_fallback_allow"


def post_exists(cursor, title: str) -> bool:
    """Checks if a post with the given title already exists in either table."""
    cursor.execute("SELECT 1 FROM QueuedPosts WHERE title = ?", (title,))
    if cursor.fetchone():
        return True
    cursor.execute("SELECT 1 FROM BadPosts WHERE title = ?", (title,))
    if cursor.fetchone():
        return True
    return False


def get_post_scores(posts_batch: list[dict]) -> dict[int, int]:
    """Calls the xAI API to score a batch of posts based on viral potential. Returns a mapping of index to score."""
    if not posts_batch:
        return {}

    if not XAI_API_KEY:
        print(
            "WARNING: XAI_API_KEY is not set. Using fallback score "
            f"{FALLBACK_SCORE_NO_XAI} for all posts."
        )
        return {i: FALLBACK_SCORE_NO_XAI for i in range(len(posts_batch))}

    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {XAI_API_KEY}"
    }

    combined_prompt = "Analyze the following batch of Reddit stories:\n\n"
    combined_prompt += get_abbreviation_reference_text() + "\n\n"
    for idx, post in enumerate(posts_batch):
        combined_prompt += f"--- POST INDEX: {idx} ---\nTitle: {post['title']}\nContent: {post['content']}\n\n"

    combined_prompt += "Respond ONLY with the exact JSON structure specified in the instructions."

    payload = {
        "model": "grok-4-1-fast-non-reasoning",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": combined_prompt}
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.5
    }

    response = None
    try:
        response = requests.post(url, headers=headers,
                                 json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()

        content_str = data["choices"][0]["message"]["content"]
        result = json.loads(content_str)

        scores_map = {}
        for item in result.get("results", []):
            idx = item.get("index")
            score = item.get("score", 0)
            if idx is not None:
                scores_map[idx] = int(score)

        for i in range(len(posts_batch)):
            if i not in scores_map:
                scores_map[i] = 0

        return scores_map
    except Exception as e:
        print(
            f"Error calling xAI API for batch of size {len(posts_batch)}: {e}")
        try:
            if response is not None:
                print(f"Response details: {response.text}")
        except Exception:
            pass
        print(
            "Falling back to score "
            f"{FALLBACK_SCORE_ON_ERROR} for this batch due to scoring API failure."
        )
        return {i: FALLBACK_SCORE_ON_ERROR for i in range(len(posts_batch))}


def generate_post_metadata(title: str, content: str) -> dict:
    if not XAI_API_KEY:
        fallback = {
            "youtube_title": title[:90],
            "youtube_description": f"Story from r/{'reddit'}\n\n{title}",
            "tiktok_description": title,
            "hashtags": ["#reddit", "#storytime", "#shorts"],
        }
        return fallback

    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {XAI_API_KEY}"
    }
    user_prompt = (
        f"Title: {title}\n"
        f"Story: {content}\n\n"
        f"{get_abbreviation_reference_text()}\n\n"
        "Return ONLY the requested JSON object."
    )
    payload = {
        "model": "grok-4-1-fast-non-reasoning",
        "messages": [
            {"role": "system", "content": METADATA_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.6
    }
    try:
        response = requests.post(url, headers=headers,
                                 json=payload, timeout=45)
        response.raise_for_status()
        data = response.json()
        content_str = data["choices"][0]["message"]["content"]
        parsed = json.loads(content_str)

        hashtags = parsed.get("hashtags", [])
        if not isinstance(hashtags, list):
            hashtags = []
        hashtags = [tag for tag in hashtags if isinstance(
            tag, str) and tag.startswith("#")]

        return {
            "youtube_title": str(parsed.get("youtube_title", title))[:90],
            "youtube_description": str(parsed.get("youtube_description", title)),
            "tiktok_description": str(parsed.get("tiktok_description", title)),
            "hashtags": hashtags or ["#reddit", "#storytime", "#shorts"],
        }
    except Exception as e:
        print(f"Error generating metadata: {e}")
        return {
            "youtube_title": title[:90],
            "youtube_description": title,
            "tiktok_description": title,
            "hashtags": ["#reddit", "#storytime", "#shorts"],
        }


def get_next_queued_post(conn: sqlite3.Connection) -> Optional[dict]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT rowid, title, content, subreddit, score, metadata_json, createdAt
        FROM QueuedPosts
        WHERE usedYet = 0 OR usedYet IS NULL
        ORDER BY score DESC, createdAt ASC
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    if not row:
        return None

    metadata_json = row[5] if row[5] else "{}"
    try:
        metadata = json.loads(metadata_json)
    except Exception:
        metadata = {}

    return {
        "rowid": row[0],
        "title": row[1],
        "content": row[2],
        "subreddit": row[3],
        "score": row[4],
        "metadata": metadata,
        "createdAt": row[6],
    }


def mark_post_as_used(conn: sqlite3.Connection, rowid: int):
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE QueuedPosts SET usedYet = 1 WHERE rowid = ?", (rowid,))
    conn.commit()


def fetch_and_process_posts(conn):
    """Fetches posts from reddit API, filters them, scores them via LLM, and stores them in DB."""
    cursor = conn.cursor()
    base_url = "https://arctic-shift.photon-reddit.com/api/posts/search"

    for subreddit in SUBREDDITS:
        print(f"\nFetching posts from r/{subreddit}...")
        params = {
            "subreddit": subreddit,
            "sort": "desc",
            "limit": 20
        }

        try:
            response = requests.get(base_url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            posts = data.get("data", [])

            valid_posts = []
            for post in posts:
                raw_title = post.get("title", "")
                raw_content = post.get("selftext", "")
                title = expand_abbreviations(raw_title)
                content = expand_abbreviations(raw_content)
                post_subreddit = post.get("subreddit", subreddit)

                # Check for clean text
                is_clean = not is_removed_or_whitespace(
                    title) and not is_removed_or_whitespace(content)

                # Check content length
                is_right_length = MIN_LENGTH <= len(content) <= MAX_LENGTH

                if not (is_clean and is_right_length):
                    continue

                # Check if it already exists in the database
                if post_exists(cursor, title):
                    print(f"Post already exists in DB: '{title[:50]}...'")
                    continue

                policy_safe, policy_reason = is_post_policy_safe(
                    title=title,
                    content=content,
                )
                if not policy_safe:
                    created_at = datetime.datetime.now(
                        datetime.timezone.utc).isoformat()
                    policy_meta = {
                        "policy_safe": False,
                        "policy_reason": policy_reason,
                    }
                    cursor.execute(
                        "INSERT INTO BadPosts (title, content, subreddit, score, metadata_json, createdAt) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            title,
                            content,
                            post_subreddit,
                            -1,
                            json.dumps(policy_meta, ensure_ascii=False),
                            created_at,
                        ),
                    )
                    conn.commit()
                    print(
                        f"--> Rejected by policy safety check: '{title[:30]}...' ({policy_reason})"
                    )
                    continue

                valid_posts.append({
                    "title": title,
                    "content": content,
                    "subreddit": post_subreddit
                })

            # Process valid posts in batches
            BATCH_SIZE = 5
            for i in range(0, len(valid_posts), BATCH_SIZE):
                batch = valid_posts[i:i+BATCH_SIZE]
                print(f"Scoring batch of {len(batch)} posts...")

                scores_map = get_post_scores(batch)

                created_at = datetime.datetime.now(
                    datetime.timezone.utc).isoformat()

                # Store results
                for idx, post in enumerate(batch):
                    title = post["title"]
                    content = post["content"]
                    post_subreddit = post["subreddit"]
                    score = scores_map.get(idx, 0)
                    metadata = generate_post_metadata(
                        title=title, content=content)
                    metadata_json = json.dumps(metadata, ensure_ascii=False)

                    if score >= QUEUE_SCORE_THRESHOLD:
                        cursor.execute(
                            "INSERT INTO QueuedPosts (title, content, subreddit, score, metadata_json, usedYet, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (title, content, post_subreddit, score,
                             metadata_json, False, created_at)
                        )
                        print(
                            f"--> Added '{title[:30]}...' to QueuedPosts with score {score}")
                    else:
                        cursor.execute(
                            "INSERT INTO BadPosts (title, content, subreddit, score, metadata_json, createdAt) VALUES (?, ?, ?, ?, ?, ?)",
                            (title, content, post_subreddit,
                             score, metadata_json, created_at)
                        )
                        print(
                            f"--> Added '{title[:30]}...' to BadPosts with score {score}")

                conn.commit()

        except Exception as e:
            print(f"Error processing subreddit {subreddit}: {e}")


if __name__ == "__main__":
    print("Setting up database...")
    db_conn = setup_database()

    print("Starting Reddit scraper pipeline...")
    fetch_and_process_posts(db_conn)

    db_conn.close()
    print("Pipeline finished!")
