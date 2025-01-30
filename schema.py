import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        port=int(os.getenv('DB_PORT'))
    )
    print(f"Connected to: {conn.dsn}")
    
    cur = conn.cursor()
    
    # Drop and create table
    cur.execute("""
        DROP TABLE IF EXISTS reddit_posts;       
        CREATE TABLE reddit_posts (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            post_id VARCHAR(255) NOT NULL UNIQUE,
            title VARCHAR(500) NOT NULL,
            author VARCHAR(255) NOT NULL,
            url VARCHAR(255) NOT NULL,
            score INTEGER NOT NULL,
            created_utc TIMESTAMP NOT NULL,
            num_comments INTEGER NOT NULL,
            upvote_ratio FLOAT NOT NULL
        );
    """)
    
    # Verify table existence
    cur.execute("""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_name = 'reddit_posts'
    """)
    print(f"Table exists: {bool(cur.fetchone()[0])}")
    
    conn.commit()
    print("Transaction committed")

except Exception as e:
    print(f"Error: {e}")
    if 'conn' in locals(): conn.rollback()

finally:
    if 'cur' in locals(): cur.close()
    if 'conn' in locals(): conn.close()
    print("Connection closed")