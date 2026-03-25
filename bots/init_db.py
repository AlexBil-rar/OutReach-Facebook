import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database.db"


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ----------------- USER -----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS User (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT UNIQUE,
        user_name TEXT,
        profile_url TEXT,
        posts_all_count INTEGER DEFAULT 0,
        comms_all_count INTEGER DEFAULT 0,
        activity TEXT,
        contacted INTEGER DEFAULT 0,
        contacts TEXT,
        fb_user_id TEXT
    )
    """)

    # ----------------- GROUP -----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS "Group" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER UNIQUE,
        group_name TEXT,
        group_url TEXT
    )
    """)

    # ----------------- POST -----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Post (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        post_id TEXT UNIQUE,
        group_id INTEGER,
        user_id TEXT,
        user_ref_id INTEGER,
        user_name TEXT,
        user_url TEXT,
        text TEXT,
        post_summary TEXT,
        post_sell_or_buy TEXT, 
        post_city TEXT,
        post_land_size TEXT, 
        post_price TEXT, 
        comment_count INTEGER,
        created_at TEXT,
        post_url TEXT,
        post_user_contacts TEXT,

        FOREIGN KEY(user_ref_id) REFERENCES User(id)
    )
    """)

    # ----------------- COMMENT -----------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Comment (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fb_comment_id TEXT UNIQUE,
        post_ref_id INTEGER,
        user_ref_id INTEGER,
        text TEXT,
        comment_date TEXT,
        comment_user_contacts TEXT,

        FOREIGN KEY(post_ref_id) REFERENCES Post(id),
        FOREIGN KEY(user_ref_id) REFERENCES User(id)
    )
    """)

    conn.commit()
    conn.close()

    print("✅ Database initialized")


if __name__ == "__main__":
    main()
