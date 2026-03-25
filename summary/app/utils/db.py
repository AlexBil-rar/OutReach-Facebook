import sqlite3
from pathlib import Path

# путь к БД
DB_PATH = Path(__file__).resolve().parents[3] / "bots" / "database.db"


def get_connection():
    return sqlite3.connect(DB_PATH)


# =========================
# POSTS
# =========================

def fetch_posts(limit: int | None = None):
    conn = get_connection()
    cur = conn.cursor()

    sql = """
        SELECT
            rowid AS post_id,
            text
        FROM Post
        WHERE (post_sell_or_buy IS NULL OR post_city IS NULL OR main_object IS NULL)
          AND text IS NOT NULL
          AND TRIM(text) != ''
    """

    if limit:
        sql += f" LIMIT {limit}"

    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return rows


def update_post_enrichment(post_id: int, location: str, intent: str, main_object: str):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE Post
        SET
            post_city = ?,
            post_sell_or_buy = ?,
            main_object = ?
        WHERE rowid = ?
        """,
        (location, intent, main_object, post_id)
    )

    conn.commit()
    conn.close()


# =========================
# COMMENTS
# =========================

def fetch_comments(limit: int | None = None):
    conn = get_connection()
    cur = conn.cursor()

    sql = """
        SELECT
            c.post_ref_id,
            c.user_ref_id,
            c.text            AS comment_text,
            p.text            AS post_text
        FROM Comment c
        JOIN Post p ON p.id = c.post_ref_id
        WHERE (c.comment_sell_or_buy IS NULL OR c.comment_city IS NULL OR c.comment_main_object IS NULL)
          AND c.text IS NOT NULL
          AND TRIM(c.text) != ''
    """

    if limit:
        sql += f" LIMIT {limit}"

    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return rows


def update_comment_enrichment(
    post_id: int,
    user_id: int,
    location: str,
    intent: str,
    main_object: str
):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE Comment
        SET
            comment_city = ?,
            comment_sell_or_buy = ?,
            comment_main_object = ?
        WHERE post_ref_id = ?
          AND user_ref_id = ?
        """,
        (location, intent, main_object, post_id, user_id)
    )

    conn.commit()
    conn.close()
