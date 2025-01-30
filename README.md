# Reddit Data Analysis

This Python script continuously collects Reddit posts from the `all` subreddit and stores them in a PostgreSQL database. It is designed to run indefinitely, fetching new posts at regular intervals.

## Features

- Fetches Reddit posts in real-time
- Stores data in a PostgreSQL database
- Handles duplicate posts gracefully
- Robust error handling and connection management
- Configurable fetch interval

## Prerequisites

Before running the script, ensure you have the following:

1. **Python 3.9+** installed
2. **PostgreSQL** installed and running
3. A **Reddit API account** with a registered application
4. Required Python libraries (listed in `requirements.txt`)

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/BharathRajuPalla/reddit_analysis.git
cd reddit_analysis
```

### 2. Install Dependencies

Install the required Python libraries:

```bash
pip install -r requirements.txt
```
### 3. Set Up PostgreSQL Database

Create a database and table in localhost for storing Reddit posts:
Change the database name, username, and password in `schema.py` to match your PostgreSQL setup.

```bash
python run schema.py
```

### 4. Configure Reddit API

Create a Reddit API account and register an application. Obtain the client ID and secret key.
Replace the placeholders in the script with your Reddit API credentials:

```bash
reddit = praw.Reddit(
    client_id='YOUR_CLIENT_ID',
    client_secret='YOUR_CLIENT_SECRET',
    user_agent='YOUR_USER_AGENT'
)
```

### 5. Run the Script

Run the script to collect and store Reddit posts:
```bash
python main.py
```
This will collect all new posts from all users in all subreddits and store them in the PostgreSQL database.
The script will run indefinitely, fetching new posts every 5 seconds. To stop the script, press `Ctrl+C`.

## Configuration

You can customize the script by modifying the following:

- **Fetch Interval**: Change the `time.sleep(5)` value in the script to adjust how often new posts are fetched.

- **Subreddit**: Modify `reddit.subreddit('all')` to collect posts from a specific subreddit.

- **Post Limit**: Adjust the `limit=1000` parameter in `fetch_new_posts` to control how many posts are fetched per iteration.

## Database Schema

The collected data is stored in the `reddit_posts` table with the following schema:

| Column Name     | Data Type   | Description                                                 |
|-----------------|-------------|-------------------------------------------------------------|
| `id`            | `UUID`      | Unique identifier for each record in table (Primary Key)    |
| `post_id`       | `TEXT`      | Unique ID of the Reddit post (Unique Key)                   |
| `title`         | `TEXT`      | Title of the post                                           |
| `author`        | `TEXT`      | Username of the post author                                 |
| `url`           | `TEXT`      | URL of the post                                             |
| `score`         | `INTEGER`   | Post score (upvotes - downvotes)                            |
| `created_utc`   | `TIMESTAMP` | Timestamp of post creation (in UTC)                         |
| `num_comments`  | `INTEGER`   | Number of comments on the post                              |
| `upvote_ratio`  | `FLOAT`     | Ratio of upvotes to total votes                             |

