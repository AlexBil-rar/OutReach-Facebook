import re
import sqlite3
from pathlib import Path
from typing import Set, Iterable

# =========================
# PATHS
# =========================
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database.db"

# =========================
# REGEX
# =========================
PHONE_RE = re.compile(
    r'(?<!\d)(?:(?:\+?972[-\s.]?)|0)(?:5\d|[23489]\d?)[-\s.]?\d{3}[-\s.]?\d{4}(?!\d)'
)

EMAIL_RE = re.compile(
    r'(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}'
)

WEBSITE_RE = re.compile(
    r'(?i)\b(?:https?://|www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)+(?:/[^\s]*)?'
)
# =========================
# NORMALIZATION
# =========================
def normalize_il_phone(raw: str) -> str:
    digits = re.sub(r"\D+", "", raw)
    if digits.startswith("972"):
        digits = "0" + digits[3:]
    return digits


def is_valid_il_phone(phone: str) -> bool:
    return phone.startswith("0") and len(phone) in (9, 10)


def normalize_website(url: str) -> str:
    url = url.rstrip(".,;:!?)\"]}")
    if not url.startswith("http"):
        url = "https://" + url
    return url


# =========================
# CONTACT EXTRACTION
# =========================
def extract_contacts(text: str) -> Set[str]:
    contacts: Set[str] = set()
    src = text or ""

    for m in PHONE_RE.finditer(src):
        phone = normalize_il_phone(m.group())
        if is_valid_il_phone(phone):
            contacts.add(phone)

    for email in EMAIL_RE.findall(src):
        contacts.add(email.lower())

    for m in WEBSITE_RE.finditer(src):
        contacts.add(normalize_website(m.group().lower()))

    return contacts


def split_contacts(value: str) -> Set[str]:
    return {v.strip() for v in (value or "").split(",") if v.strip()}


def join_contacts(values: Iterable[str]) -> str:
    return ", ".join(sorted(set(values)))


# =========================
# GENERIC UPDATE FUNCTION
# =========================
def extract_and_update(
    conn: sqlite3.Connection,
    select_sql: str,
    update_sql: str
) -> int:
    cur = conn.cursor()
    cur.execute(select_sql)

    updated = 0
    for row_id, text in cur.fetchall():
        contacts = extract_contacts(text)
        if not contacts:
            continue

        cur.execute(update_sql, (join_contacts(contacts), row_id))
        updated += cur.rowcount

    conn.commit()
    return updated


# =========================
# STEP 1: POSTS
# =========================
def extract_post_contacts(conn: sqlite3.Connection) -> int:
    updated = extract_and_update(
        conn,
        """
        SELECT post_id, text
        FROM Post
        WHERE text IS NOT NULL AND TRIM(text) != ''
        """,
        """
        UPDATE Post
        SET post_user_contacts = ?
        WHERE post_id = ?
        """
    )

    print(f"[Post] обновлено строк: {updated}")
    return updated


# =========================
# STEP 2: COMMENTS
# =========================
def extract_comment_contacts(conn: sqlite3.Connection) -> int:
    updated = extract_and_update(
        conn,
        """
        SELECT id, text
        FROM Comment
        WHERE text IS NOT NULL AND TRIM(text) != ''
        """,
        """
        UPDATE Comment
        SET comment_user_contacts = ?
        WHERE id = ?
        """
    )

    print(f"[Comment] обновлено строк: {updated}")
    return updated


# =========================
# STEP 4: USER AGGREGATION
# =========================
def aggregate_users_contacts(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()

    cur.execute("""
        SELECT user_id
        FROM User
        WHERE user_id IS NOT NULL AND TRIM(user_id) != ''
    """)

    users = [r[0] for r in cur.fetchall()]
    updated = 0

    for user_id in users:
        contacts: Set[str] = set()

        cur.execute("""
            SELECT post_user_contacts
            FROM Post
            WHERE user_id = ?
        """, (user_id,))
        for (v,) in cur.fetchall():
            contacts |= split_contacts(v)

        cur.execute("""
            SELECT comment_user_contacts
            FROM Comment
            WHERE user_ref_id = ?
        """, (user_id,))
        for (v,) in cur.fetchall():
            contacts |= split_contacts(v)

        if not contacts:
            continue

        cur.execute("""
            UPDATE User
            SET contacts = ?
            WHERE user_id = ?
        """, (join_contacts(contacts), user_id))

        updated += cur.rowcount

    conn.commit()
    print(f"[User] обновлено пользователей: {updated}")
    return updated


# =========================
# MAIN
# =========================
def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        extract_post_contacts(conn)
        extract_comment_contacts(conn)
        aggregate_users_contacts(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
