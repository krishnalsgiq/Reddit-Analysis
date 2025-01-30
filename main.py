import psycopg2
from psycopg2 import extras
import praw
import time
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()

# Initialize the Reddit instance
reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT')
)

def get_last_processed_post(connection):
    with connection.cursor() as cur:
        cur.execute("SELECT MAX(created_utc) FROM reddit_posts;")
        last_timestamp = cur.fetchone()[0]
        return last_timestamp.timestamp() if last_timestamp else 0.0

def fetch_new_posts(last_timestamp):
    new_posts = []
    for submission in reddit.subreddit('all').new(limit=1000):  # Limited to 1000 posts per fetch
        if submission.created_utc > last_timestamp:
            author_name = submission.author.name if submission.author else None
            new_posts.append((
                submission.id,
                submission.title,
                author_name,
                submission.url,
                submission.score,
                submission.created_utc,
                submission.num_comments,
                submission.upvote_ratio
            ))
        else:
            break  # Stop once we reach older posts
    return new_posts

def insert_executemany(connection, data):
    try:
        cur = connection.cursor()
        cur.executemany("""
            INSERT INTO reddit_posts 
            (post_id, title, author, url, score, created_utc, num_comments, upvote_ratio)
            VALUES (%s, %s, %s, %s, %s, TO_TIMESTAMP(%s), %s, %s)
            ON CONFLICT (post_id) DO NOTHING;
        """, data)
        connection.commit()
        print(f"Inserted {cur.rowcount} new rows")
    except Exception as e:
        print(f"Database error: {e}")
        connection.rollback()
    finally:
        cur.close()

def continuous_collection():
    while True:
        conn = None
        try:
            # Establish new connection each iteration
            conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST'),
                database=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD')
            )
            
            last_timestamp = get_last_processed_post(conn)
            print(f"Last timestamp: {datetime.fromtimestamp(last_timestamp)}")
            
            new_posts = fetch_new_posts(last_timestamp)
            print(f"Found {len(new_posts)} new posts")
            
            if new_posts:
                insert_executemany(conn, new_posts)
            
            # Adjust sleep time based on your needs (in seconds)
            time.sleep(5)  # Check every 30 seconds
            
        except KeyboardInterrupt:
            print("Stopping collection...")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(60)  # Wait longer on error
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    print(f"Error closing connection: {e}")

if __name__ == "__main__":
    continuous_collection()