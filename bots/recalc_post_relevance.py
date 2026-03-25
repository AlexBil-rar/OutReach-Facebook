import sqlite3
from posts_filter import should_keep_post

DB_PATH = "database.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("SELECT post_id, text FROM Post")
posts = cur.fetchall()

updated = 0

for p in posts:
    text = p["text"] or ""
    is_rel = 1 if should_keep_post(text) else 0

    cur.execute(
        "UPDATE Post SET is_relevant = ? WHERE post_id = ?",
        (is_rel, p["post_id"])
    )
    updated += 1

conn.commit()
conn.close()

print(f"✅ Recalculated relevance for {updated} posts")
