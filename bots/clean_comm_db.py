import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database.db"


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("🔍 Counting duplicates BEFORE cleanup...")

    cur.execute("""
        SELECT COUNT(*) FROM Comment
    """)
    total_before = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT user_name, comment_text, comment_date
            FROM Comment
            GROUP BY user_name, comment_text, comment_date
            HAVING COUNT(*) > 1
        )
    """)
    dup_groups = cur.fetchone()[0]

    print(f"📦 Total comments: {total_before}")
    print(f"♻️ Duplicate groups: {dup_groups}")

    print("\n🧹 Removing duplicates...")

    cur.execute("""
        DELETE FROM Comment
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM Comment
            GROUP BY user_name, comment_text, comment_date
        )
    """)

    conn.commit()

    cur.execute("""
        SELECT COUNT(*) FROM Comment
    """)
    total_after = cur.fetchone()[0]

    removed = total_before - total_after

    print("\n✅ CLEANUP DONE")
    print(f"🗑️ Removed duplicates: {removed}")
    print(f"📦 Remaining comments: {total_after}")

    conn.close()


if __name__ == "__main__":
    main()
