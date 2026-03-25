import sqlite3
from posts_filter import should_keep_post

DB_PATH = "database.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("SELECT post_id, summary, text FROM Post")

rows = cur.fetchall()
updated = 0

for r in rows:
    text = (r["summary"] or r["text"] or "").strip()
    is_relevant = 1 if should_keep_post(text) else 0

    cur.execute(
        "UPDATE Post SET is_relevant = ? WHERE post_id = ?",
        (is_relevant, r["post_id"])
    )
    updated += 1

conn.commit()
conn.close()

print(f"✅ Updated {updated} posts with is_relevant")
