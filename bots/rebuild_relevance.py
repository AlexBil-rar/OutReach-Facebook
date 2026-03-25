import sqlite3
from pathlib import Path
from posts_filter import should_keep_post

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("UPDATE Post SET is_relevant = 0;")

    cur.execute("SELECT post_id, text FROM Post;")
    rows = cur.fetchall()

    cnt = 0
    for r in rows:
        pid = r["post_id"]
        text = r["text"] or ""
        if should_keep_post(text):
            cur.execute("UPDATE Post SET is_relevant = 1 WHERE post_id = ?;", (pid,))
            cnt += 1

    conn.commit()
    print(f"✅ Relevant posts: {cnt}")

    # пересчёт users
    cur.execute("""
        UPDATE User
        SET posts_all_count = (
            SELECT COUNT(*)
            FROM Post p
            WHERE p.user_id = User.user_id
              AND p.is_relevant = 1
        )
    """)

    cur.execute("""
        UPDATE User
        SET comms_all_count = (
            SELECT COUNT(*)
            FROM Comment c
            JOIN Post p ON p.post_id = c.post_id
            WHERE c.user_id = User.user_id
              AND p.is_relevant = 1
        )
    """)

    conn.commit()
    conn.close()
    print("✅ User counts recalculated")

if __name__ == "__main__":
    main()
