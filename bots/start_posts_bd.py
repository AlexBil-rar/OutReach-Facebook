import os
import time
import json
import sqlite3
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, date, timedelta
from collections import defaultdict
from urllib.parse import urlparse, urlunparse

import requests


# =========================
# PATHS / CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database.db"

BRIGHTDATA_TOKEN = os.getenv("BRIGHTDATA_TOKEN", "cf1d227a-be9c-48b2-8c03-8d106bb7fb66")
DATASET_ID = os.getenv("BRIGHTDATA_POSTS_DATASET_ID", "gd_lz11l67o2cb3r0lkj3")

SCRAPE_URL = "https://api.brightdata.com/datasets/v3/scrape"
PROGRESS_URL = "https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
SNAPSHOT_URL = "https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"

HEADERS = {"Authorization": f"Bearer {BRIGHTDATA_TOKEN}", "Content-Type": "application/json"}

def get_posts_days_back() -> int:
    wd = date.today().weekday()

    if wd in (4, 5):
        return 0

    if wd == 6:
        return 3

    return 1



POSTS_LIMIT_PER_GROUP = int(os.getenv("POSTS_LIMIT_PER_GROUP", "5000"))
POSTS_DAYS_BACK = get_posts_days_back()
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "30"))

REQUEST_TIMEOUT = int(os.getenv("BD_TIMEOUT", "2200"))
PROGRESS_POLL_SEC = int(os.getenv("BD_PROGRESS_POLL", "120"))
MAX_WAIT_SEC = int(os.getenv("BD_MAX_WAIT", "2800"))

# Если хочешь принудительно: "ymd" или "mdy" (по умолчанию auto)
DATE_MODE = os.getenv("BD_DATE_MODE", "auto").lower().strip()  # auto|ymd|mdy



# =========================
# DB helpers
# =========================
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def table_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f'PRAGMA table_info("{table}")')
    return {r[1] for r in cur.fetchall()}


def normalize_date(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return date_str


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def get_first(obj: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        v = obj.get(k)
        if v not in (None, "", [], {}):
            return v
    return None


def make_post_id_fallback(group_id, user_id, text, created_at, post_url) -> str:
    raw = f"{group_id}|{user_id}|{created_at}|{post_url}|{text}"
    return _sha1(raw)


def build_upsert_sql(table: str, pk: str, cols: List[str]) -> str:
    placeholders = ",".join(["?"] * len(cols))
    updates = [f'{c}=COALESCE(excluded.{c},{table}.{c})' for c in cols if c != pk]
    return f'''
        INSERT INTO "{table}" ({",".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT({pk}) DO UPDATE SET {", ".join(updates)}
    '''


def upsert_many(cur, table: str, pk: str, rows: List[Dict[str, Any]], allowed_cols: set[str]) -> int:
    if not rows:
        return 0

    colset = {pk}
    for r in rows:
        colset |= {k for k in r.keys() if k in allowed_cols}

    cols = [pk] + sorted(c for c in colset if c != pk)
    sql = build_upsert_sql(table, pk, cols)

    values = []
    for r in rows:
        if r.get(pk):
            values.append([r.get(c) for c in cols])

    if not values:
        return 0

    cur.executemany(sql, values)
    return len(values)


def get_groups() -> List[Tuple[int, str]]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT group_id, group_url FROM "Group" WHERE TRIM(group_url) != ""')
    rows = cur.fetchall()
    conn.close()
    return [(int(r["group_id"]), (r["group_url"] or "").strip()) for r in rows]


# =========================
# URL normalization (КРИТИЧНО)
# =========================
def normalize_profile_url(user_url: Optional[str], profile_id: Optional[str]) -> Optional[str]:
    """
    Приводим profile URL к canonical виду:
    https://www.facebook.com/profile.php?id=NUMERIC
    """

    if not user_url:
        return user_url

    user_url = user_url.strip()

    # 1️⃣ если profile_id numeric — используем его как главный источник истины
    if profile_id and str(profile_id).isdigit():
        return f"https://www.facebook.com/profile.php?id={profile_id}"

    # 2️⃣ если URL вида /people/.../123456/
    try:
        parts = user_url.rstrip("/").split("/")
        last_part = parts[-1]
        if last_part.isdigit():
            return f"https://www.facebook.com/profile.php?id={last_part}"
    except Exception:
        pass

    return user_url


def normalize_url(u: Optional[str]) -> str:
    """
    Приводим URL к максимально сравнимому виду:
    - lower
    - убираем www.
    - убираем query/fragment
    - убираем trailing /
    """
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


# =========================
# BrightData helpers
# =========================
def to_bd_date(d: date, mode: str) -> str:
    if mode == "mdy":
        return d.strftime("%m-%d-%Y")
    # ymd
    return d.strftime("%Y-%m-%d")


def start_scrape_batch(urls: List[str], date_mode: str) -> Dict[str, Any]:
    today = date.today()
    start_d = to_bd_date(today - timedelta(days=POSTS_DAYS_BACK), date_mode)
    end_d = to_bd_date(today, date_mode)

    print(f"🗓️ BrightData window ({date_mode}): {start_d} → {end_d}")
    payload = {"input": [{"url": url, "start_date": start_d, "end_date": end_d} for url in urls]}

    resp = requests.post(
        SCRAPE_URL,
        headers=HEADERS,
        params={"dataset_id": DATASET_ID, "notify": "false", "include_errors": "true"},
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )

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


def download_snapshot_all(snapshot_id: str, max_parts=20) -> List[Dict[str, Any]]:
    rows = []
    seen_hashes = set()

    for part in range(1, max_parts + 1):
        r = requests.get(
            SNAPSHOT_URL.format(snapshot_id=snapshot_id),
            headers=HEADERS,
            params={"format": "ndjson", "part": part},
            timeout=1200,
        )

        part_rows = []
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            obj = json.loads(line)

            # ❌ если это input-эхо — сразу стоп
            if "start_date" in obj and "end_date" in obj and "url" in obj and "text" not in obj:
                print("⚠️ BrightData returned INPUT echo instead of posts. Stop downloading.")
                return []

            h = json.dumps(obj, sort_keys=True)
            if h in seen_hashes:
                print("⚠️ Repeating data detected. Stop downloading.")
                return rows

            seen_hashes.add(h)
            part_rows.append(obj)

        if not part_rows:
            break

        rows.extend(part_rows)

    return rows



def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def fetch_posts_for_batch(batch_urls: List[str]) -> List[Dict[str, Any]]:
    """
    AUTO режим: если в первом формате 0 valid — пробуем второй.
    """
    modes = []
    if DATE_MODE in ("ymd", "mdy"):
        modes = [DATE_MODE]
    else:
        modes = ["mdy", "ymd"]  # авто: сначала mdy, потом ymd

    last = []
    for mode in modes:
        result = start_scrape_batch(batch_urls, mode)

        if result["mode"] == "sync":
            posts = result["records"]
        else:
            sid = result["snapshot_id"]
            print(f"🆔 Snapshot {sid}")
            wait_snapshot(sid)
            posts = download_snapshot_all(sid)

        valid = [p for p in posts if not p.get("error")]
        print(f"📦 Batch fetch ({mode}): raw={len(posts)} valid={len(valid)}")

        last = valid
        if valid:
            return valid

    return last  # вернёт пусто, если пусто


# =========================
# MAIN
# =========================
def main():

    # 🔥 0. Не скрапим посты в пятницу и субботу
    if POSTS_DAYS_BACK == 0:
        print("⛔ Skipping posts scraping (Friday/Saturday)")
        return

    groups = get_groups()
    if not groups:
        print("⚠️ No groups in DB")
        return

    group_url_to_id = {normalize_url(url): gid for gid, url in groups}

    print(f"📌 Total groups: {len(groups)} | batch size: {BATCH_SIZE}")
    for i, (gid, url) in enumerate(groups[:20], start=1):
        print(f"   DB group[{i}]: {gid}  {url}")

    all_posts: List[Dict[str, Any]] = []

    for idx, batch in enumerate(chunked(groups, BATCH_SIZE), start=1):
        batch_urls = [url for _, url in batch]
        print(f"\n🚀 Batch {idx}: {len(batch_urls)} groups")
        for gid, url in batch:
            print(f"   • Group {gid} => {url}")

        valid = fetch_posts_for_batch(batch_urls)
        all_posts.extend(valid)

    print(f"\n📦 Total valid posts fetched: {len(all_posts)}")
    if not all_posts:
        print("❌ No posts collected at all. Stop.")
        return

    conn = get_db()
    cur = conn.cursor()

    post_cols = table_cols(conn, "Post")
    user_cols = table_cols(conn, "User")

    user_rows: Dict[str, Dict[str, Any]] = {}
    post_rows: List[Dict[str, Any]] = []
    group_counter = defaultdict(int)

    skipped_no_gid = 0
    skipped_limit = 0
    unmatched_examples = []

    for p in all_posts:
        raw_input_url = (
            (p.get("input") or {}).get("url")
            or p.get("group_url")
            or p.get("url")
            or ""
        )

        input_url = normalize_url(raw_input_url)
        gid = group_url_to_id.get(input_url)

        if not gid:
            skipped_no_gid += 1
            if len(unmatched_examples) < 10:
                unmatched_examples.append((raw_input_url, input_url))
            continue

        if group_counter[gid] >= POSTS_LIMIT_PER_GROUP:
            skipped_limit += 1
            continue

        user_id = get_first(p, ["profile_id", "user_id", "author_id", "fb_user_id"])
        user_name = get_first(p, ["user_username_raw", "user_name", "author_name", "name"])
        user_url = get_first(p, ["user_url", "profile_url", "author_url"])
        user_url = normalize_profile_url(user_url, user_id)

        # 1️⃣ Сначала обычный текст
        text = (get_first(p, ["content", "text"]) or "").strip()

        # 2️⃣ Если текста нет — пробуем accessibility_caption из attachments
        if not text:
            attachments = p.get("attachments") or []

            for att in attachments:
                caption = (att.get("accessibility_caption") or "").strip()
                if caption:
                    # убираем английскую обёртку
                    if "text that says" in caption:
                        caption = caption.split("text that says")[-1]
                    text = caption.strip(" '\"")
                    break
                
        created_at = normalize_date(
            get_first(p, ["date_posted", "created_at", "date"])
        )

        post_url = normalize_url(get_first(p, ["post_url", "url"]))

        comment_count = get_first(p, ["num_comments", "comment_count", "comments_count"])
        try:
            comment_count = int(comment_count) if comment_count is not None else 0
        except Exception:
            comment_count = 0

        post_summary = p.get("post_summary")

        post_id = get_first(p, ["post_id", "id"]) or make_post_id_fallback(
            gid, user_id, text, created_at, post_url
        )

        if user_id:
            uid = str(user_id)
            if uid not in user_rows:
                user_rows[uid] = {
                    "user_id": uid,
                    "user_name": user_name,
                    "profile_url": user_url,
                }

        post_rows.append({
            "post_id": str(post_id),
            "group_id": gid,
            "user_id": str(user_id) if user_id else None,
            "user_name": user_name,
            "user_url": user_url,
            "text": text,
            "created_at": created_at,
            "comment_count": comment_count,
            "post_summary": post_summary,
            "post_url": post_url,
        })

        group_counter[gid] += 1

    print("\n🧾 PRE-SAVE STATS")
    print(f"Prepared posts: {len(post_rows)}")
    print(f"Prepared users: {len(user_rows)}")
    print(f"Skipped (no group match): {skipped_no_gid}")
    print(f"Skipped (per-group limit): {skipped_limit}")

    if unmatched_examples:
        print("\n⚠️ Examples of URL mismatch:")
        for raw_u, norm_u in unmatched_examples:
            print(f"   RAW : {raw_u}")
            print(f"   NORM: {norm_u}\n")

    if not post_rows:
        print("❌ Prepared 0 posts → nothing to write.")
        conn.close()
        return

    try:
        conn.execute("BEGIN")
        users_written = upsert_many(cur, "User", "user_id", list(user_rows.values()), user_cols)
        posts_written = upsert_many(cur, "Post", "post_id", post_rows, post_cols)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"\n✅ DONE: posts={posts_written}, users={users_written}")
    for gid, cnt in sorted(group_counter.items()):
        print(f"📌 Group {gid}: {cnt} posts")

    # =========================
    # CHAIN
    # =========================

    if posts_written > 0:

        new_post_ids = [r["post_id"] for r in post_rows]

        try:
            print("\n🧹 START DB CLEANUP")
            from posts_filter import main as cleanup_main
            cleanup_main()
            print("✅ DB CLEANUP DONE")
        except Exception as e:
            print("❌ DB CLEANUP FAILED:", e)
            return

        try:
            conn = get_db()
            cur = conn.cursor()

            placeholders = ",".join("?" for _ in new_post_ids)

            rows = cur.execute(f"""
                SELECT post_id
                FROM Post
                WHERE post_id IN ({placeholders})
            """, new_post_ids).fetchall()

            filtered_post_ids = [r["post_id"] for r in rows]
            conn.close()

            print(f"📌 New & filtered posts for comments: {len(filtered_post_ids)}")

        except Exception as e:
            print("❌ Failed to collect filtered post_ids:", e)
            return

        if filtered_post_ids:
            try:
                print("\n🚀 START COMMENTS SCRAPER (new + filtered only)")
                import start_comm_bd
                start_comm_bd.main(filtered_post_ids)
                print("✅ COMMENTS DONE")
            except Exception as e:
                print("❌ Failed to start comments:", e)
        else:
            print("⚠️ No new relevant posts for comments.")

        try:
            print("\n🔍 START REGEX CONTACTS")
            import regex_contacts
            regex_contacts.main()
            print("✅ REGEX CONTACTS DONE")
        except Exception as e:
            print("❌ REGEX CONTACTS FAILED:", e)

        try:
            print("\n🤖 START AI ENRICHMENT (posts + comments)")
            import sys
            from pathlib import Path

            PROJECT_ROOT = Path(__file__).resolve().parents[1]
            sys.path.insert(0, str(PROJECT_ROOT))

            from summary.app.main import main as summary_main
            summary_main()
            print("✅ AI ENRICHMENT DONE")
        except Exception as e:
            print("❌ AI ENRICHMENT FAILED:", e)


if __name__ == "__main__":
    main()
