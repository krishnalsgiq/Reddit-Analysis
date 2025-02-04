# import psycopg2
# from psycopg2 import extras
# import praw
# import time
# from datetime import datetime
# import os
# from dotenv import load_dotenv
# load_dotenv()

# # Initialize the Reddit instance
# reddit = praw.Reddit(
#     client_id=os.getenv('REDDIT_CLIENT_ID'),
#     client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
#     user_agent=os.getenv('REDDIT_USER_AGENT')
# )
# def get_last_processed_post(connection):
    
#     """
#     Retrieves the timestamp of the most recently processed post from the database.

#     Args:
#         connection: A psycopg2 connection object to the PostgreSQL database.

#     Returns:
#         A float representing the timestamp of the most recent post in UTC seconds.
#         Returns 0.0 if no posts are found in the database.
#     """

#     with connection.cursor() as cur:
#         cur.execute("SELECT MAX(created_utc) FROM reddit_posts;")
#         last_timestamp = cur.fetchone()[0]
#         return last_timestamp.timestamp() if last_timestamp else 0.0

# def fetch_new_posts(last_timestamp):
#     """
#     Fetches new Reddit posts created after the given timestamp from the 'all' subreddit.

#     Args:
#         last_timestamp (float): The timestamp in UTC seconds of the most recently processed post.

#     Returns:
#         list of tuple: A list of tuples, where each tuple contains the following information about a post:
#             - post_id (str): Unique ID of the Reddit post.
#             - title (str): Title of the post.
#             - author_name (str or None): Username of the post author, or None if not available.
#             - url (str): URL of the post.
#             - score (int): Post score (upvotes - downvotes).
#             - created_utc (float): Timestamp of post creation in UTC seconds.
#             - num_comments (int): Number of comments on the post.
#             - upvote_ratio (float): Ratio of upvotes to total votes.
#     """

#     new_posts = []
#     for submission in reddit.subreddit('all').new(limit=1000):  # Limited to 1000 posts per fetch
#         if submission.created_utc > last_timestamp:
#             author_name = submission.author.name if submission.author else None
#             new_posts.append((
#                 submission.id,
#                 submission.title,
#                 author_name,
#                 submission.url,
#                 submission.score,
#                 submission.created_utc,
#                 submission.num_comments,
#                 submission.upvote_ratio
#             ))
#         else:
#             break  # Stop once we reach older posts
#     return new_posts

# def insert_executemany(connection, data):
#     """
#     Executes an INSERT INTO statement with multiple values in a single query.

#     Args:
#         connection: A psycopg2 connection object to the PostgreSQL database.
#         data (list of tuple): A list of tuples, where each tuple contains the following information about a post:
#             - post_id (str): Unique ID of the Reddit post.
#             - title (str): Title of the post.
#             - author (str or None): Username of the post author, or None if not available.
#             - url (str): URL of the post.
#             - score (int): Post score (upvotes - downvotes).
#             - created_utc (float): Timestamp of post creation in UTC seconds.
#             - num_comments (int): Number of comments on the post.
#             - upvote_ratio (float): Ratio of upvotes to total votes.
#     """
#     try:
#         cur = connection.cursor()
#         cur.executemany("""
#             INSERT INTO reddit_posts 
#             (post_id, title, author, url, score, created_utc, num_comments, upvote_ratio)
#             VALUES (%s, %s, %s, %s, %s, TO_TIMESTAMP(%s), %s, %s)
#             ON CONFLICT (post_id) DO NOTHING;
#         """, data)
#         connection.commit()
#         print(f"Inserted {cur.rowcount} new rows")
#     except Exception as e:
#         print(f"Database error: {e}")
#         connection.rollback()
#     finally:
#         cur.close()

# def continuous_collection():
#     """
#     Continuously collects new Reddit posts and stores them in the PostgreSQL database.

#     This function runs indefinitely, establishing a new connection to the database
#     on each iteration. It fetches new posts from the 'all' subreddit that were created
#     after the last processed post's timestamp. The posts are then inserted into the 
#     `reddit_posts` table. The function handles keyboard interruption for graceful exit
#     and waits longer on errors to prevent rapid retry.

#     Note:
#         - Adjust the sleep time to change the fetch interval.
#         - Ensure the .env file contains the correct database and Reddit API credentials.

#     Raises:
#         Exception: If an error occurs during database connection, fetching posts, or inserting data.
#     """

#     while True:
#         conn = None
#         try:
#             # Establish new connection each iteration
#             conn = psycopg2.connect(
#                 host=os.getenv('DB_HOST'),
#                 database=os.getenv('DB_NAME'),
#                 user=os.getenv('DB_USER'),
#                 password=os.getenv('DB_PASSWORD'),
#                 port=int(os.getenv('DB_PORT'))
#             )
            
#             last_timestamp = get_last_processed_post(conn)
#             print(f"Last timestamp: {datetime.fromtimestamp(last_timestamp)}")
            
#             new_posts = fetch_new_posts(last_timestamp)
#             print(f"Found {len(new_posts)} new posts")
            
#             if new_posts:
#                 insert_executemany(conn, new_posts)
            
#             # Adjust sleep time based on your needs (in seconds)
#             time.sleep(5)  # Check every 30 seconds
            
#         except KeyboardInterrupt:
#             print("Stopping collection...")
#             break
#         except Exception as e:
#             print(f"Error: {e}")
#             time.sleep(60)  # Wait longer on error
#         finally:
#             if conn:
#                 try:
#                     conn.close()
#                 except Exception as e:
#                     print(f"Error closing connection: {e}")

# if __name__ == "__main__":
#     continuous_collection()
# DO NOT MODIFY THIS FILE OR ANYTHING ABOVE! (play from line 165)


#This is another version in which async is not working and a little wierd from line 490 we will hit.

"""
Reddit Data Collection Pipeline

This script collects Reddit posts for a sentiment analysis system.
It supports two modes:

1. Synchronous mode (default):
   - Queries r/all using a timestamp-based approach to fetch new posts.
2. Asynchronous mode (if ASYNC_MODE is set to "true" in the .env file):
   - Discovers relevant subreddits.
   - Hits multiple endpoints concurrently (global r/all and each discovered subreddit)
     across several strategies (different sort methods and time filters).

Deduplication is performed with a global in-memory Python set (seen_ids) so that
only new posts are processed. Database unique constraints (ON CONFLICT DO NOTHING)
act as a final safeguard.

The search query and other configuration options (like sleep intervals) are stored
in the .env file.
"""

import os
import asyncio
import praw
import psycopg2
from psycopg2 import extras, pool
import time
from datetime import datetime
from dotenv import load_dotenv
import logging

load_dotenv()
print(os.getenv('ASYNC_ENABLED'))
# Set up logging for detailed output.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Global in-memory deduplication store for this session.
seen_ids = set()

# Create a database connection pool.
db_pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=5,
    maxconn=20,
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=int(os.getenv("DB_PORT", 5432))
)
print("✅ Database connection pool ready")

def create_reddit_client():
    """
    Create and return an authenticated PRAW Reddit client using credentials from .env.
    """
    return praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT")
    )

class RedditDataEngine:
    def __init__(self):
        print("\n=== REDDIT DATA ENGINE INIT ===")
        self.reddit = create_reddit_client()
        print("✅ Reddit API authenticated")
        self.strategies = self._load_strategies()
        # Optionally, seed seen_ids from recent DB entries to avoid re-processing.
        self._seed_deduplication()

    def _load_strategies(self):
        """
        Load a list of strategies for API calls. Each strategy includes a sort method,
        time_filter, limit of posts per request, and an interval value for potential adaptive sleep.
        """
        strategies = [
            {"sort": "new", "time_filter": "hour", "limit": 100, "interval": 5},
            {"sort": "hot", "time_filter": "day", "limit": 100, "interval": 15},
            {"sort": "top", "time_filter": "week", "limit": 100, "interval": 60},
            {"sort": "relevance", "time_filter": "all", "limit": 100, "interval": 120}
        ]
        print(f"✅ Loaded {len(strategies)} strategies")
        return strategies

    def _seed_deduplication(self):
        """
        Optionally, load recent post_ids (e.g., last 2 days) from the database into seen_ids.
        This minimizes duplicate API processing when the pipeline restarts.
        """
        print("\n🔄 Seeding deduplication cache from database...")
        try:
            with db_pool.getconn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT post_id FROM reddit_posts 
                        WHERE created_utc > NOW() - INTERVAL '2 days'
                    """)
                    count = 0
                    for post_id in cur:
                        seen_ids.add(post_id[0])
                        count += 1
                    print(f"✅ Seeded {count} post IDs from database")
        except Exception as e:
            print(f"❌ Dedupe seeding failed: {e}")
        finally:
            db_pool.putconn(conn)

    def _create_post_tuple(self, submission, strategy):
        """
        Create a standardized tuple of post metadata for insertion into the DB.
        """
        return (
            submission.id,
            submission.title,
            submission.selftext,
            submission.author.name if submission.author else None,
            submission.url,
            submission.score,
            datetime.fromtimestamp(submission.created_utc),
            submission.num_comments,
            submission.upvote_ratio,
            str(submission.subreddit),
            os.getenv("SEARCH_QUERY", "Washington Gas"),
            strategy["sort"],
            strategy["time_filter"]
        )

    async def _fetch_strategy_page(self, subreddit, strategy, after=None):
        """
        Asynchronously fetch a page of posts from a subreddit using a given strategy.
        Uses PRAW’s synchronous API inside asyncio.to_thread.
        """
        try:
            return await asyncio.to_thread(
                lambda: list(self.reddit.subreddit(subreddit).search(
                    query=os.getenv("SEARCH_QUERY", "Washington Gas"),
                    sort=strategy["sort"],
                    time_filter=strategy["time_filter"],
                    limit=strategy["limit"],
                    params={'after': after} if after else {}
                ))
            )
        except Exception as e:
            print(f"❌ Error fetching page from r/{subreddit}: {e}")
            return []

    async def _process_strategy(self, subreddit, strategy):
        """
        Process one strategy for a given subreddit with pagination.
        Checks the in-memory set seen_ids for deduplication.
        Bulk-inserts new posts into the database.
        """
        print(f"\n🚀 Starting strategy for r/{subreddit} with {strategy['sort']}/{strategy['time_filter']}")
        all_posts = []
        after = None
        while True:
            page = await self._fetch_strategy_page(subreddit, strategy, after)
            if not page:
                print(f"🏁 No more results for r/{subreddit} with strategy {strategy}")
                break
            new_posts = []
            for submission in page:
                if submission.id in seen_ids:
                    continue
                seen_ids.add(submission.id)
                new_posts.append(self._create_post_tuple(submission, strategy))
            if new_posts:
                print(f"📥 Fetched {len(new_posts)} new posts from r/{subreddit} (strategy: {strategy['sort']}/{strategy['time_filter']})")
                self._bulk_insert(new_posts)
                all_posts.extend(new_posts)
            after = page[-1].fullname if page else None
            if not after:
                break
        print(f"✅ Completed strategy for r/{subreddit} with {len(all_posts)} posts collected")
        return all_posts

    def _bulk_insert(self, posts):
        """
        Bulk-insert a list of posts into the database.
        Uses executemany with an ON CONFLICT DO NOTHING clause.
        """
        try:
            conn = db_pool.getconn()
            with conn.cursor() as cur:
                extras.execute_batch(cur, """
                    INSERT INTO reddit_posts 
                    (post_id, title, selftext, author, url, score, created_utc, num_comments, upvote_ratio, subreddit, search_topic, sort_method, time_filter)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (post_id) DO NOTHING
                """, posts)
                conn.commit()
                print(f"💾 Inserted {cur.rowcount} posts into the database")
        except Exception as e:
            print(f"❌ Bulk insert failed: {e}")
        finally:
            db_pool.putconn(conn)

    async def _discover_subs(self):
        """
        Discover relevant subreddits dynamically using PRAW's subreddit search.
        If none are found, defaults to ["all"].
        """
        print("🕵️ Discovering subreddits...")
        try:
            subs = await asyncio.to_thread(
                lambda: [sub.display_name for sub in self.reddit.subreddits.search(
                    query=os.getenv("SEARCH_QUERY", "Washington Gas"), limit=50
                )]
            )
            # Use top 25 discovered subreddits and add the global "all" endpoint.
            sub_list = ["all"] + subs[:25]
            print(f"✅ Discovered {len(sub_list)} subreddits")
            return sub_list
        except Exception as e:
            print(f"❌ Subreddit discovery error: {e}")
            return ["all"]

    async def async_collect(self):
        """
        Asynchronous collection pipeline:
          - Discover subreddits.
          - For each subreddit and for each strategy, create an asynchronous task.
          - Wait for all tasks to complete.
          - Sleep for a configurable interval before the next cycle.
        """
        print("\n=== ASYNC MODE ACTIVATED ===")
        while True:
            cycle_start = time.time()
            try:
                subs = await self._discover_subs()
                tasks = []
                # Launch tasks for global endpoint and each discovered subreddit.
                for sub in subs:
                    for strat in self.strategies:
                        tasks.append(self._process_strategy(sub, strat))
                print(f"🚀 Launching {len(tasks)} tasks for this cycle")
                await asyncio.gather(*tasks)
            except Exception as e:
                print(f"❌ Cycle error: {e}")
            elapsed = time.time() - cycle_start
            sleep_time = max(float(os.getenv("COLLECTION_INTERVAL", "30")) - elapsed, 5)
            print(f"💤 Cycle complete in {elapsed:.1f}s. Sleeping for {sleep_time:.1f}s")
            await asyncio.sleep(sleep_time)

    def sync_collect(self):
        """
        Synchronous collection pipeline:
          - Retrieves last processed timestamp from the DB.
          - Fetches new posts from r/all using a timestamp-based approach.
          - Inserts new posts into the database.
          - Sleeps for a configurable interval before repeating.
        """
        print("\n=== SYNC MODE ACTIVATED ===")
        while True:
            conn = None
            try:
                conn = psycopg2.connect(
                    host=os.getenv("DB_HOST"),
                    database=os.getenv("DB_NAME"),
                    user=os.getenv("DB_USER"),
                    password=os.getenv("DB_PASSWORD"),
                    port=int(os.getenv("DB_PORT", 5432))
                )
                with conn.cursor() as cur:
                    cur.execute("SELECT MAX(created_utc) FROM reddit_posts;")
                    last_row = cur.fetchone()[0]
                    last_ts = last_row.timestamp() if last_row else 0.0
                    print(f"⏳ Last post timestamp: {datetime.fromtimestamp(last_ts) if last_ts else 'None'}")
                # Fetch posts from r/all created after last_ts.
                new_posts = []
                for submission in self.reddit.subreddit("all").new(limit=1000):
                    if submission.created_utc <= last_ts:
                        break
                    if submission.id in seen_ids:
                        continue
                    seen_ids.add(submission.id)
                    new_posts.append(self._create_post_tuple(submission, {"sort": "new", "time_filter": "hour"}))
                print(f"📥 Synchronously collected {len(new_posts)} new posts")
                if new_posts:
                    self._bulk_insert(new_posts)
                print("💤 Sleeping for 60 seconds before next sync cycle...")
                time.sleep(float(os.getenv("COLLECTION_INTERVAL", "60")))
            except KeyboardInterrupt:
                print("🚫 Sync mode interrupted by user. Exiting...")
                break
            except Exception as e:
                print(f"❌ Sync error: {e}")
                time.sleep(60)
            finally:
                if conn:
                    conn.close()

def main():
    """
    Main entry point:
      - Reads the ASYNC_MODE env variable.
      - Runs the asynchronous pipeline if ASYNC_MODE is "true", otherwise synchronous.
    """
    async_enabled = os.getenv("ASYNC_MODE", "false").lower() == "true"
    engine = RedditDataEngine()
    if async_enabled:
        logger.info("Starting asynchronous collection pipeline.")
        asyncio.run(engine.async_collect())
    else:
        logger.info("Starting synchronous collection pipeline.")
        engine.sync_collect()

if __name__ == "__main__":
    print("\n=== REDDIT DATA COLLECTOR ===")
    main()
    logger.info("Pipeline finished.")







