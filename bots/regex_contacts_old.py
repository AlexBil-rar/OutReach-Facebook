import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Set

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
    r'(?:https?://)?(?:www\.)?(?:[a-z0-9-]+\.)+[a-z]{2,}'
)

# =========================
# NORMALIZATION
# =========================

def normalize_il_phone(raw: str) -> str:
    digits = re.sub(r'\D+', '', raw)
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

    for m in PHONE_RE.finditer(text or ""):
        phone = normalize_il_phone(m.group())
        if is_valid_il_phone(phone):
            contacts.add(phone)

    for email in EMAIL_RE.findall(text or ""):
        contacts.add(email.lower())

    for m in WEBSITE_RE.finditer(text or ""):
        contacts.add(normalize_website(m.group().lower()))

    return contacts

# =========================
# STEP 1: EXTRACT CONTACTS FROM POSTS
# =========================

def extract_post_contacts(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""
        SELECT post_id, text
        FROM Post
        WHERE text IS NOT NULL
          AND TRIM(text) != ''
    """)

    rows = cur.fetchall()
    updated = 0

    for post_id, text in rows:
        contacts = extract_contacts(text)
        if not contacts:
            continue

        cur.execute("""
            UPDATE Post
            SET post_user_contacts = ?
            WHERE post_id = ?
        """, (", ".join(sorted(contacts)), post_id))

        updated += cur.rowcount

    conn.commit()
    print(f"[Post] обновлено строк с контактами: {updated}")

# =========================
# STEP 2: ENSURE USERS FROM POSTS
# =========================

def ensure_users_from_posts(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO User (user_id, user_name, profile_url)
        SELECT
            p.user_id,
            MAX(p.user_name),
            MAX(p.user_url)
        FROM Post p
        WHERE p.user_id IS NOT NULL
          AND TRIM(p.user_id) != ''
        GROUP BY p.user_id
    """)

    conn.commit()
    print(f"[User] добавлено из Post: {cur.rowcount}")

# =========================
# STEP 3: AGGREGATE USER CONTACTS (FROM POSTS ONLY)
# =========================

def split_contacts(value: str) -> Set[str]:
    return {v.strip() for v in (value or "").split(",") if v.strip()}


def aggregate_users_contacts(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("SELECT user_id FROM User")
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

        if not contacts:
            continue

        cur.execute("""
            UPDATE User
            SET contacts = ?
            WHERE user_id = ?
        """, (", ".join(sorted(contacts)), user_id))

        updated += cur.rowcount

    conn.commit()
    print(f"[User] обновлено пользователей с контактами: {updated}")

# =========================
# MAIN
# =========================

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        extract_post_contacts(conn)
        ensure_users_from_posts(conn)
        aggregate_users_contacts(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
