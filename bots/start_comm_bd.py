import os
import time
import json
import sqlite3
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from urllib.parse import urlparse, urlunparse

import requests

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database.db"

BRIGHTDATA_TOKEN = os.getenv("BRIGHTDATA_TOKEN", "cf1d227a-be9c-48b2-8c03-8d106bb7fb66")
DATASET_ID = os.getenv("BRIGHTDATA_COMMENTS_DATASET_ID", "gd_lkay758p1eanlolqw8")

SCRAPE_URL = "https://api.brightdata.com/datasets/v3/scrape"
PROGRESS_URL = "https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
SNAPSHOT_URL = "https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"

HEADERS = {"Authorization": f"Bearer {BRIGHTDATA_TOKEN}", "Content-Type": "application/json"}

BATCH_SIZE = int(os.getenv("COMMENTS_BATCH_SIZE", "300"))
REQUEST_TIMEOUT = int(os.getenv("BD_TIMEOUT", "2800"))
PROGRESS_POLL_SEC = int(os.getenv("BD_PROGRESS_POLL", "30"))
MAX_WAIT_SEC = int(os.getenv("BD_MAX_WAIT", "2800"))


# -------------------------
# DB utils
# -------------------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def table_info(conn: sqlite3.Connection, table: str) -> Dict[str, Dict[str, Any]]:
    """
    returns: {col_name: {"type": "...", "pk": 0/1}}
    """
    cur = conn.cursor()
    rows = cur.execute(f'PRAGMA table_info("{table}")').fetchall()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        out[r["name"]] = {"type": (r["type"] or "").upper(), "pk": int(r["pk"])}
    return out


# -------------------------
# helpers
# -------------------------
def normalize_url(u: Optional[str]) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    u = u.replace("www.", "")
    try:
        p = urlparse(u)
        p = p._replace(query="", fragment="")
        u = urlunparse(p)
    except Exception:
        pass
    return u.rstrip("/").lower()


def normalize_date(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # оставляю поведение максимально мягким
        return None


def get_first(obj: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        v = obj.get(k)
        if v not in (None, "", [], {}):
            return v
    return None


def chunked(lst, n: int):
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


# ОСТАВЛЯЮ (ничего не удаляю), но ниже мы больше не ограничиваемся numeric-only
def looks_like_numeric_user_id(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    # Отсекаем pfbid... и прочие токены
    if s.lower().startswith("pfbid"):
        return None
    if s.isdigit():
        return s
    return None


# -------------------------
# ✅ ЛОГИКА ИЗ ЛОКАЛЬНОГО СКРИПТА (перенесена 1:1 по смыслу)
# -------------------------
def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def is_fb_profile_url(u: Optional[str]) -> bool:
    u = (u or "").lower()
    return "facebook.com" in u and "fbcdn.net" not in u

def maybe_generate_profile_url(user_name: Optional[str],
                               user_id: Optional[str],
                               profile_url: Optional[str]) -> Optional[str]:
    """
    Приводим comment profile URL к canonical виду.
    """

    # Anonymous не трогаем
    if user_name and user_name.lower().startswith("anonymous participant"):
        return profile_url

    # 1️⃣ Если user_id numeric — он главный
    if user_id and str(user_id).isdigit():
        return f"https://www.facebook.com/profile.php?id={user_id}"

    # 2️⃣ Если profile_url есть и это /people/.../NUMERIC/
    if profile_url:
        try:
            parts = profile_url.rstrip("/").split("/")
            last_part = parts[-1]
            if last_part.isdigit():
                return f"https://www.facebook.com/profile.php?id={last_part}"
        except Exception:
            pass

    return profile_url

def resolve_comment_user_key(c: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    user_name = get_first(c, ["user_name", "commentator_name", "name"])

    # В BD-данных commentator_profile_url часто = аватар (fbcdn), НЕ профиль.
    raw_profile_url = get_first(
        c,
        ["commentator_profile", "commentator_profile_url", "profile_url", "user_url"]
    )
    profile_url = normalize_url(raw_profile_url) if is_fb_profile_url(raw_profile_url) else None

    user_id_any = get_first(c, ["user_id", "profile_id", "fb_user_id", "commentator_profile_id", "commentator_id"])
    if user_id_any:
        user_key = str(user_id_any).strip()
        if user_key:
            return user_key, user_name, profile_url

    if profile_url:
        return sha1(profile_url), user_name, profile_url

    if user_name:
        return sha1(user_name.strip().lower()), user_name, None

    cid = get_first(c, ["comment_id", "fb_comment_id", "id"])
    if cid:
        return sha1(str(cid)), None, None

    return None, user_name, profile_url


# -------------------------
# Data sources
# -------------------------
def get_posts_for_comments(conn, only_post_ids: Optional[List[str]] = None):
    cur = conn.cursor()

    # 🔥 1. Если передали конкретные post_id → работаем ТОЛЬКО с ними
    if only_post_ids:
        placeholders = ",".join("?" for _ in only_post_ids)

        rows = cur.execute(f"""
            SELECT id, post_id, post_url
            FROM Post
            WHERE post_id IN ({placeholders})
              AND post_url IS NOT NULL
              AND TRIM(post_url) != ''
        """, only_post_ids).fetchall()

    # 🔥 2. Если не передали → берём только посты за последние 72 часа
    else:
        rows = cur.execute("""
            SELECT id, post_id, post_url
            FROM Post
            WHERE post_url IS NOT NULL
              AND TRIM(post_url) != ''
              AND created_at >= datetime('now', '-3 days')
        """).fetchall()

    return [
        (int(r["id"]), str(r["post_id"]), str(r["post_url"]))
        for r in rows
        if r["id"] is not None
    ]



# -------------------------
# BrightData
# -------------------------
def start_scrape_batch(post_urls: List[str]) -> Dict[str, Any]:
    payload = {"input": [{"url": url, "get_all_replies": True, "comments_sort": ""} for url in post_urls]}

    resp = requests.post(
        SCRAPE_URL,
        headers=HEADERS,
        params={"dataset_id": DATASET_ID, "notify": "false", "include_errors": "true"},
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )

    if resp.status_code in (401, 403):
        raise RuntimeError(f"Invalid credentials ({resp.status_code}). Check BRIGHTDATA_TOKEN / dataset permissions.")

    if resp.status_code not in (200, 202):
        raise RuntimeError(f"BrightData error {resp.status_code}: {resp.text}")

    if resp.status_code == 202:
        sid = (resp.json() or {}).get("snapshot_id")
        return {"mode": "async", "snapshot_id": sid}

    records: List[Dict[str, Any]] = []
    for line in resp.text.splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        if "snapshot_id" in obj:
            return {"mode": "async", "snapshot_id": obj["snapshot_id"]}
        records.append(obj)

    return {"mode": "sync", "records": records}


def wait_snapshot(snapshot_id: str):
    start = time.time()
    while True:
        r = requests.get(PROGRESS_URL.format(snapshot_id=snapshot_id), headers=HEADERS, timeout=60)
        if r.status_code in (401, 403):
            raise RuntimeError("Invalid credentials while polling progress. Check BRIGHTDATA_TOKEN.")
        r.raise_for_status()

        status = (r.json() or {}).get("status")
        print(f"⏳ Snapshot {snapshot_id}: {status}")

        if status == "ready":
            return
        if status in ("failed", "canceled"):
            raise RuntimeError(f"Snapshot {snapshot_id} failed ({status})")

        if time.time() - start > MAX_WAIT_SEC:
            raise TimeoutError(f"Snapshot {snapshot_id} timeout after {MAX_WAIT_SEC}s")

        time.sleep(PROGRESS_POLL_SEC)


def download_snapshot_all(snapshot_id: str, max_parts: int = 200) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen_hashes = set()

    for part in range(1, max_parts + 1):
        print(f"📥 Downloading snapshot part {part} ...")

        r = requests.get(
            SNAPSHOT_URL.format(snapshot_id=snapshot_id),
            headers=HEADERS,
            params={"format": "ndjson", "part": part},
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code in (401, 403):
            raise RuntimeError("Invalid credentials while downloading snapshot. Check BRIGHTDATA_TOKEN.")
        r.raise_for_status()

        part_rows = []
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            obj = json.loads(line)

            # INPUT echo guard
            if (
                "url" in obj
                and "get_all_replies" in obj
                and "comment_text" not in obj
                and "text" not in obj
                and "comment_id" not in obj
            ):
                print("⚠️ INPUT echo detected → stop downloading")
                return rows

            h = json.dumps(obj, sort_keys=True)
            if h in seen_hashes:
                print("⚠️ Repeated data detected → stop downloading")
                return rows

            seen_hashes.add(h)
            part_rows.append(obj)

        if not part_rows:
            print("📭 Empty part → stop downloading")
            break

        print(f"📦 Part {part}: {len(part_rows)} records")
        rows.extend(part_rows)

    return rows


# -------------------------
# MAIN
# -------------------------
def main(only_post_ids: Optional[List[str]] = None):
    print("🔑 BD token:", (BRIGHTDATA_TOKEN[:6] + "..." if BRIGHTDATA_TOKEN else "EMPTY"))
    print("🧾 BD dataset:", (DATASET_ID if DATASET_ID else "EMPTY"))
    print("📌 DB PATH:", DB_PATH)

    if not BRIGHTDATA_TOKEN or not DATASET_ID:
        print("❌ Missing BRIGHTDATA_TOKEN or BRIGHTDATA_COMMENTS_DATASET_ID")
        return

    conn = get_db()

    # schema detection
    user_cols = table_info(conn, "User")
    post_cols = table_info(conn, "Post")
    comment_cols = table_info(conn, "Comment")

    if "id" not in post_cols:
        print('❌ Post table has no "id" column. Comments need Post.id to reference it.')
        conn.close()
        return

    # determine how Comment.user_ref_id should be stored
    comment_user_ref_type = (comment_cols.get("user_ref_id") or {}).get("type", "")
    comment_user_ref_is_int = "INT" in comment_user_ref_type  # INTEGER FK on User.id
    user_has_int_id = "id" in user_cols and ("INT" in (user_cols["id"]["type"] or "") or user_cols["id"]["pk"] == 1)

    print(f"🧩 Comment.user_ref_id type: {comment_user_ref_type or '(unknown)'} | int_fk={comment_user_ref_is_int}")

    posts = get_posts_for_comments(conn, only_post_ids)
    print(f"📦 Posts: {len(posts)}")

    if not posts:
        print("⚠️ No posts with Post.id+url → skip comments")
        conn.close()
        return

    # url -> Post.id(INTEGER)
    url_to_post_rowid = {normalize_url(url): post_rowid for (post_rowid, _pid, url) in posts}

    inserted = 0
    skipped_post = 0
    skipped_id = 0
    skipped_text = 0
    skipped_user = 0

    cur = conn.cursor()

    for batch in chunked(posts, BATCH_SIZE):
        batch_urls = [u for _rid, _pid, u in batch]
        print(f"\n🚀 Batch: {len(batch_urls)}")

        result = start_scrape_batch(batch_urls)
        if result["mode"] == "sync":
            comments = result["records"]
        else:
            sid = result["snapshot_id"]
            print(f"🆔 Snapshot {sid}")
            wait_snapshot(sid)
            comments = download_snapshot_all(sid)

        print(f"📦 Raw: {len(comments)}")

        try:
            conn.execute("BEGIN")

            for c in comments:
                # пропускаем ошибки BD-типа
                if c.get("error"):
                    continue

                input_url = normalize_url(
                    ((c.get("input") or {}).get("url"))
                    or c.get("post_url")
                    or c.get("url")
                    or ""
                )
                post_ref_id = url_to_post_rowid.get(input_url)
                if not post_ref_id:
                    skipped_post += 1
                    continue

                fb_comment_id = get_first(c, ["comment_id", "fb_comment_id", "id"])
                if not fb_comment_id:
                    skipped_id += 1
                    continue

                text = (get_first(c, ["comment_text", "text"]) or "").strip()
                if not text:
                    skipped_text += 1
                    continue

                # ✅ ПЕРЕНЕСЕНА ЛОГИКА ИЗ ЛОКАЛЬНОГО СКРИПТА
                user_key, user_name, profile_url = resolve_comment_user_key(c)

                # пробуем сгенерировать profile URL если нужно
                profile_url = maybe_generate_profile_url(
                    user_name=user_name,
                    user_id=user_key,
                    profile_url=profile_url
                )

                user_rowid: Optional[int] = None
                if comment_user_ref_is_int:
                    # FK на User.id → обязаны получить rowid, иначе НЕ ВСТАВЛЯЕМ комментарий
                    if not user_key:
                        skipped_user += 1
                        continue

                    cur.execute(
                        """
                        INSERT INTO User (user_id, user_name, profile_url)
                        VALUES (?, ?, ?)
                        ON CONFLICT(user_id) DO UPDATE SET
                            user_name = COALESCE(excluded.user_name, User.user_name),
                            profile_url = COALESCE(excluded.profile_url, User.profile_url)
                        """,
                        (user_key, user_name, profile_url),
                    )

                    if user_has_int_id:
                        r = cur.execute("SELECT id FROM User WHERE user_id=?", (user_key,)).fetchone()
                        user_rowid = int(r["id"]) if r and r["id"] is not None else None

                    if user_rowid is None:
                        skipped_user += 1
                        continue

                    user_ref_value = user_rowid
                else:
                    # если вдруг user_ref_id TEXT
                    user_ref_value = user_key

                cur.execute(
                    """
                    INSERT INTO Comment (fb_comment_id, post_ref_id, user_ref_id, text, comment_date)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(fb_comment_id) DO NOTHING
                    """,
                    (
                        str(fb_comment_id),
                        int(post_ref_id),
                        user_ref_value,
                        text,
                        normalize_date(get_first(c, ["date_created", "comment_date", "created_at", "date", "timestamp"])),
                    ),
                )

                if cur.rowcount == 1:
                    inserted += 1

            conn.commit()

        except Exception:
            conn.rollback()
            raise

    conn.close()

    print("\n🎉 COMMENTS DONE")
    print("Inserted:", inserted)
    print("Skip post:", skipped_post)
    print("Skip id:", skipped_id)
    print("Skip text:", skipped_text)
    print("Skip user (no resolved FK):", skipped_user)

    # after comments – repair & recount
    if inserted > 0:
        try:
            print("\n🔧 START REPAIR LINKS")
            import repair_links
            repair_links.main()
            print("✅ REPAIR DONE")
        except Exception as e:
            print("❌ REPAIR FAILED:", e)


if __name__ == "__main__":
    main()
