# repair_links.py

import sqlite3
from pathlib import Path


# ================= CONFIG =================

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database.db"


# ================= DB =================

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    print("📌 DB:", DB_PATH)
    return conn


# ================= UTILS =================

def safe_int(v):
    try:
        return int(v)
    except:
        return None


def col_exists(conn, table, col) -> bool:

    rows = conn.execute(
        f'PRAGMA table_info("{table}")'
    ).fetchall()

    return any(r["name"] == col for r in rows)


# ================= MAIN =================

def main():

    print("🔧 Repairing links & counters...")

    conn = get_db()
    cur = conn.cursor()

    # ================= USERS MAP =================

    user_map = {}

    rows = cur.execute("""
        SELECT id, user_id
        FROM User
        WHERE user_id IS NOT NULL
    """).fetchall()

    for r in rows:
        iid = safe_int(r["id"])
        if iid is not None:
            user_map[r["user_id"]] = iid

    print("👤 Users mapped:", len(user_map))

    # ================= TRANSACTION START =================

    conn.execute("BEGIN")

    try:

        # ================= PROFILE URL NORMALIZATION =================

        rows = cur.execute("""
            SELECT id, user_id, profile_url
            FROM User
            WHERE profile_url IS NOT NULL
        """).fetchall()

        fixed_profiles = 0

        for r in rows:

            uid = r["user_id"]
            row_id = safe_int(r["id"])
            profile_url = (r["profile_url"] or "").strip()

            if not uid or not str(uid).isdigit():
                continue

            canonical = f"https://www.facebook.com/profile.php?id={uid}"

            if profile_url != canonical:
                cur.execute("""
                    UPDATE User
                    SET profile_url=?
                    WHERE id=?
                """, (canonical, row_id))

                fixed_profiles += 1

        print("🔗 Profile URLs normalized:", fixed_profiles)


        # ================= POSTS =================

        if col_exists(conn, "Post", "user_ref_id"):

            rows = cur.execute("""
                SELECT id, user_id
                FROM Post
                WHERE user_ref_id IS NULL
                  AND user_id IS NOT NULL
            """).fetchall()

            fixed = 0

            for r in rows:

                pid = safe_int(r["id"])
                uid = user_map.get(r["user_id"])

                if not pid or not uid:
                    continue

                cur.execute("""
                    UPDATE Post
                    SET user_ref_id=?
                    WHERE id=?
                """, (uid, pid))

                fixed += 1

            print("📝 Posts fixed:", fixed)


        # ================= COMMENTS =================

        if col_exists(conn, "Comment", "user_ref_id"):

            rows = cur.execute("""
                SELECT c.id     AS cid,
                       u.user_id AS uid
                FROM Comment c
                JOIN Post p ON p.id = c.post_ref_id
                JOIN User u ON u.id = p.user_ref_id
                WHERE c.user_ref_id IS NULL
            """).fetchall()

            fixed = 0

            for r in rows:

                cid = safe_int(r["cid"])
                uid = user_map.get(r["uid"])

                if not cid or not uid:
                    continue

                cur.execute("""
                    UPDATE Comment
                    SET user_ref_id=?
                    WHERE id=?
                """, (uid, cid))

                fixed += 1

            print("💬 Comments fixed:", fixed)


        # ================= COUNTERS =================

        if col_exists(conn, "Post", "comment_count"):

            cur.execute("""
                UPDATE Post
                SET comment_count = (
                    SELECT COUNT(1)
                    FROM Comment c
                    WHERE c.post_ref_id = Post.id
                )
            """)

            print("📊 Post.comment_count updated")


        if col_exists(conn, "User", "posts_all_count"):

            cur.execute("""
                UPDATE User
                SET posts_all_count = (
                    SELECT COUNT(1)
                    FROM Post p
                    WHERE p.user_ref_id = User.id
                )
            """)

            print("📊 User.posts_all_count updated")


        if col_exists(conn, "User", "comms_all_count"):

            cur.execute("""
                UPDATE User
                SET comms_all_count = (
                    SELECT COUNT(1)
                    FROM Comment c
                    WHERE c.user_ref_id = User.id
                )
            """)

            print("📊 User.comms_all_count updated")


        conn.commit()
        print("✅ Repair done successfully")

    except Exception as e:

        conn.rollback()
        print("❌ Repair failed:", e)
        raise e

    finally:

        conn.close()



if __name__ == "__main__":
    main()
