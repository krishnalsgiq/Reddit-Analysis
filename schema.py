# import psycopg2
# import os
# from dotenv import load_dotenv

# load_dotenv()

# try:
#     conn = psycopg2.connect(
#         host=os.getenv('DB_HOST'),
#         database=os.getenv('DB_NAME'),
#         user=os.getenv('DB_USER'),
#         password=os.getenv('DB_PASSWORD'),
#         port=int(os.getenv('DB_PORT'))
#     )
#     print(f"Connected to: {conn.dsn}")
    
#     cur = conn.cursor()
    
#     # Drop and create table
#     cur.execute("""
#         DROP TABLE IF EXISTS reddit_posts;       
#         CREATE TABLE reddit_posts (
#             id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
#             post_id VARCHAR(255) NOT NULL UNIQUE,
#             title VARCHAR(500) NOT NULL,
#             author VARCHAR(255) NOT NULL,
#             url VARCHAR(255) NOT NULL,
#             score INTEGER NOT NULL,
#             created_utc TIMESTAMP NOT NULL,
#             num_comments INTEGER NOT NULL,
#             upvote_ratio FLOAT NOT NULL
#         );
#     """)
    
#     # Verify table existence
#     cur.execute("""
#         SELECT COUNT(*) 
#         FROM information_schema.tables 
#         WHERE table_name = 'reddit_posts'
#     """)
#     print(f"Table exists: {bool(cur.fetchone()[0])}")
    
#     conn.commit()
#     print("Transaction committed")

# except Exception as e:
#     print(f"Error: {e}")
#     if 'conn' in locals(): conn.rollback()

# finally:
#     if 'cur' in locals(): cur.close()
#     if 'conn' in locals(): conn.close()
#     print("Connection closed")
# # DO NOT MODIFY THIS FILE OR ANYTHING ABOVE!(Play from line 60)





#!/usr/bin/env python3
"""
PostgreSQL Schema Initialization for Reddit Data Collection

This script creates (or recreates) the reddit_posts table with all required columns:
- Post metadata (title, selftext, author, etc.)
- Strategy metadata (search_topic, sort_method, time_filter)
- The "subreddit" column to record the source community
- A sentiment_score column (for future sentiment analysis)

Indexes are created to speed up queries. Recreating the table ensures that any previous
schema missing columns (e.g., "subreddit") are removed.
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def initialize_database():
    print("\n=== DATABASE INITIALIZATION ===")
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=int(os.getenv("DB_PORT", 5432))
        )
        print("✅ Connected to PostgreSQL")

        with conn.cursor() as cur:
            # Drop table if it exists to ensure our new schema is used.
            print("🔄 Dropping existing reddit_posts table if present...")
            cur.execute("DROP TABLE IF EXISTS reddit_posts;")
            
            print("🔄 Creating reddit_posts table...")
            cur.execute("""
                CREATE TABLE reddit_posts (
                    post_id VARCHAR(255) PRIMARY KEY,
                    created_utc TIMESTAMP NOT NULL,
                    title TEXT NOT NULL,
                    selftext TEXT NOT NULL,
                    author VARCHAR(512),
                    url TEXT NOT NULL,
                    score INTEGER NOT NULL,
                    num_comments INTEGER NOT NULL,
                    upvote_ratio FLOAT NOT NULL,
                    subreddit VARCHAR(255) NOT NULL,
                    search_topic VARCHAR(255) NOT NULL,
                    sort_method VARCHAR(50) NOT NULL,
                    time_filter VARCHAR(50) NOT NULL,
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sentiment_score FLOAT
                );
            """)
            print("✅ Table created with required columns.")

            print("🔄 Creating indexes...")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_subreddit_strategy 
                ON reddit_posts (subreddit, sort_method, time_filter);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_temporal 
                ON reddit_posts USING BRIN (created_utc);
            """)
            print("✅ Indexes created.")

        conn.commit()
        print("🚀 Database initialized successfully")
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()
            print("🔌 Connection closed\n")

if __name__ == "__main__":
    print("=== Starting Database Setup ===")
    initialize_database()
    print("=== Database Setup Complete ===\n")

    print("=== Starting Database Setup ===")
    initialize_database()
    print("=== Database Setup Complete ===\n")
