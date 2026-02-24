import os
import json
import sqlite3
import datetime
import requests
from dotenv import load_dotenv

load_dotenv()

SUBREDDITS = ["TrueOffMyChest", "AmItheAsshole", "tifu", "NuclearRevenge", "pettyrevenge"]
DB_PATH = os.path.join(os.path.dirname(__file__), "posts.db")

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

SYSTEM_PROMPT = """You are an expert social media content strategist specializing in YouTube Shorts and TikTok trends. Your task is to evaluate a batch of Reddit stories for viral potential.

Criteria for a high score (7-10):
- High emotional hook in the first 2 sentences.
- Clear conflict or drama.
- Strong pacing with satisfying or shocking developments.
- The post should be mainly in English.

Rules:
- You must evaluate each post and output valid JSON.
- Provide a single integer score between 1 and 10 for each post.
- You must respond ONLY with a valid JSON object formatted exactly like this:
{
  "results": [
    {"index": 0, "score": 8},
    {"index": 1, "score": 4}
  ]
}"""

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
            createdAt TIMESTAMP
        )
    ''')
    
    conn.commit()
    return conn

def is_removed_or_whitespace(text: str) -> bool:
    """Checks if the text is empty, whitespace, or contains removal strings."""
    if not text:
        return True
    if text.strip() == "":
        return True
    if text in REMOVAL_STRINGS:
        return True
    return False

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
        print("WARNING: XAI_API_KEY is not set. Skipping LLM scoring and returning default scores of 0.")
        return {i: 0 for i in range(len(posts_batch))}

    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {XAI_API_KEY}"
    }
    
    combined_prompt = "Analyze the following batch of Reddit stories:\n\n"
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
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
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
                
        # Fill any missing indices with 0 just in case
        for i in range(len(posts_batch)):
            if i not in scores_map:
                scores_map[i] = 0
                
        return scores_map
    except Exception as e:
        print(f"Error calling xAI API for batch of size {len(posts_batch)}: {e}")
        try:
            print(f"Response details: {response.text}")
        except:
            pass
        return {i: 0 for i in range(len(posts_batch))}

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
                title = post.get("title", "")
                content = post.get("selftext", "")
                post_subreddit = post.get("subreddit", subreddit)
                
                # Check for clean text
                is_clean = not is_removed_or_whitespace(title) and not is_removed_or_whitespace(content)
                
                # Check content length
                is_right_length = MIN_LENGTH <= len(content) <= MAX_LENGTH
                
                if not (is_clean and is_right_length):
                    continue
                
                # Check if it already exists in the database
                if post_exists(cursor, title):
                    print(f"Post already exists in DB: '{title[:50]}...'")
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
                
                created_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
                
                # Store results
                for idx, post in enumerate(batch):
                    title = post["title"]
                    content = post["content"]
                    post_subreddit = post["subreddit"]
                    score = scores_map.get(idx, 0)
                    
                    if score >= 7:
                        cursor.execute(
                            "INSERT INTO QueuedPosts (title, content, subreddit, score, usedYet, createdAt) VALUES (?, ?, ?, ?, ?, ?)",
                            (title, content, post_subreddit, score, False, created_at)
                        )
                        print(f"--> Added '{title[:30]}...' to QueuedPosts with score {score}")
                    else:
                        cursor.execute(
                            "INSERT INTO BadPosts (title, content, subreddit, score, createdAt) VALUES (?, ?, ?, ?, ?)",
                            (title, content, post_subreddit, score, created_at)
                        )
                        print(f"--> Added '{title[:30]}...' to BadPosts with score {score}")
                
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
