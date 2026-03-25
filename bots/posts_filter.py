import sqlite3
import re
from pathlib import Path
import re
import sqlite3
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database.db"


INCLUDE_KEYWORDS = [
    " קרקע","מגרש","נחלה","חקלאית","חקלאי","חקלאיים","חקלאות",
    "טאבו","בגוש","גוש ","דונם","דירה להשכיר","וותמל",
    "שטח ","שטח לבנייה","תב״ע",'תב"ע',"תמל","תמ״ל",
    "שטחים","לבנייה","למכירה ב","צהובה","מקרקעין","תוכנית בניין עיר",
    "תמ״א", "תמ\"א", "מתחם", "לתעשיה", "לעסקה מהירה"
]

EXCLUDE_KEYWORDS = [
    "דירה למכירה","למכירה דיר","מוכר דירה","קונה דירה",
    "מטבח","בריכה","מקלחת","חדרי שירותים","מזגן","ג׳קוזי",
    "חדרים מרווחת","בבניין בוטיק","מעליות","לובי","תקרות",
    "חלונות","מרפסת","חדר אירועים","מחסן",
    "מחיר שכירות","בכניסה לדירה","גובה תקרה","להשכרה", "השכרה",
    "שכירות", "בית קטן", "וילה", "בית פרטי", "אחסון", "ממד", "תקרה בגובה",
    "לוגיסטיקה", "חדר רחצה", "רהיטים", "חניות פרטיות", "ממ״ד"
]

INCLUDE_PATTERNS = [
    re.compile(r"ב\s*\d{2,6}"),
    re.compile(r"חלקה\s*[:\-]?\s*\d{2,6}")
]


def _contains_any_keyword(text, keywords):
    return any(k in text for k in keywords) if text else False


def _contains_any_pattern(text, patterns):
    return any(p.search(text) for p in patterns) if text else False


def should_keep_post(text: str) -> bool:
    if not text or not text.strip():
        return False

    if _contains_any_keyword(text, EXCLUDE_KEYWORDS):
        return False

    return (
        _contains_any_keyword(text, INCLUDE_KEYWORDS)
        or _contains_any_pattern(text, INCLUDE_PATTERNS)
    )


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT id, text FROM Post")
    posts = cur.fetchall()

    keep_post_ids = set()
    delete_post_ids = set()

    for p in posts:
        if should_keep_post(p["text"]):
            keep_post_ids.add(p["id"])
        else:
            delete_post_ids.add(p["id"])

    print(f"KEEP posts: {len(keep_post_ids)}")
    print(f"DELETE posts: {len(delete_post_ids)}")

    if not delete_post_ids:
        print("Nothing to delete.")
        return

    # 1️⃣ Удаляем комментарии к нерелевантным постам
    cur.executemany(
        "DELETE FROM Comment WHERE post_ref_id = ?",
        [(pid,) for pid in delete_post_ids]
    )

    # 2️⃣ Удаляем сами посты
    cur.executemany(
        "DELETE FROM Post WHERE id = ?",
        [(pid,) for pid in delete_post_ids]
    )

    conn.commit()
    conn.close()

    print("✅ Database cleaned successfully.")


if __name__ == "__main__":
    main()
