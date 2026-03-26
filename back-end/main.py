from fastapi.responses import StreamingResponse
import csv
import io
import sqlite3
import subprocess
import sys
import secrets
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Dict, Set, Optional
from fastapi import Body
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

import jwt
import time


BASE_DIR = Path(__file__).resolve().parent
BOTS_DIR = BASE_DIR.parent / "bots"
sys.path.append(str(BOTS_DIR))
START_POSTS_SCRIPT = BASE_DIR.parent / "bots" / "start_posts_bd.py"
START_COMMENTS_SCRIPT = BASE_DIR.parent / "bots" / "start_comm_bd.py"

DB_PATH = BASE_DIR.parent / "bots" / "database.db"
FRONTEND_DIR = BASE_DIR.parent / "front-end"

scheduler = BackgroundScheduler(timezone="Asia/Jerusalem")


def run_start_posts():
    try:
        subprocess.Popen(["python3", str(START_POSTS_SCRIPT)], cwd=str(START_POSTS_SCRIPT.parent))
    except Exception as e:
        print("❌ Error running start_posts_bd.py:", e)


def run_start_comments():
    try:
        subprocess.Popen(["python3", str(START_COMMENTS_SCRIPT)], cwd=str(START_COMMENTS_SCRIPT.parent))
    except Exception as e:
        print("❌ Error running start_comm_bd.py:", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 09:00 posts, 09:15 comments (Asia/Jerusalem)
    scheduler.add_job(run_start_posts, CronTrigger(hour=4, minute=0), id="posts", replace_existing=True, misfire_grace_time=3600)
    scheduler.add_job(run_start_comments, CronTrigger(hour=5, minute=15), id="comments", replace_existing=True, misfire_grace_time=3600)

    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

security = HTTPBearer()

USERS = {
    "invest@erra.co.il": {
        "password": "TL.Ct?w3]n^.DBzPu%v^",
        "role": "admin"
    },
    "shimon@erra.co.il": {
        "password": "TLV,cyZ_HA?w]n^._BIBI=",
        "role": "admin"
    },
    "margarita@erra.co.il": {
        "password": "UhgPG+ARcQwiA#g-E3EC#*#Z:!f%FK>?rk>kWWBa~DQfjBP@R:nBhrw46zYq",
        "role": "admin"
    },
    "andrey@erra.co.il ": {
        "password": "4==GdFcG5kYLUwzfid.T",
        "role": "admin"
    },
    "danny@erra.co.il": {
        "password": "qYz]Pdb74>bUKH0!w95R",
        "role": "admin"
    },
    "nicol@erra.co.il": {
        "password": "9^i4wW],i-TU0!6Nf?WL",
        "role": "user"
    }

}
TOKENS: Dict[str, dict] = {}


def auth_required(creds: HTTPAuthorizationCredentials = Depends(security)):
    token = creds.credentials
    if token not in TOKENS:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return TOKENS[token]


def auth_cookie_required(request: Request):
    token = request.cookies.get("auth_token")
    if not token or token not in TOKENS:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return TOKENS[token]


@app.get("/login")
def login_page():
    return FileResponse(FRONTEND_DIR / "login.html")


@app.get("/")
def dashboard_page():
    return FileResponse(FRONTEND_DIR / "main.html")


@app.post("/api/login")
def login(data: dict):
    username = data.get("email")
    password = data.get("password")

    if not username or not password:
        raise HTTPException(status_code=400, detail="Missing credentials")

    user_data = USERS.get(username)

    if not user_data or user_data["password"] != password:
        raise HTTPException(status_code=401, detail="Invalid login or password")

    token = secrets.token_hex(32)

    TOKENS[token] = {
        "username": username,
        "role": user_data["role"]
    }

    response = JSONResponse({
        "token": token,
        "user": {
            "username": username,
            "role": user_data["role"]
        }
    })

    response.set_cookie(
        key="auth_token",
        value=token,
        httponly=True,
        samesite="lax"
    )

    return response

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def map_user(row):
    d = dict(row)
    return {
        "UserID": d.get("id"),                     
        "FBUserID": d.get("user_id"),           
        "UserName": d.get("user_name"),
        "ProfileURL": d.get("profile_url"),
        "PostsAllCount": d.get("real_posts_count"),
        "CommsAllCount": d.get("real_comments_count"),
        "Activity": d.get("activity"),
        "Contacted": d.get("contacted"),
        "Contacts": d.get("contacts"),
    }


def map_post(row):
    d = dict(row)
    return {
        "PostID": d.get("post_id"),
        "UserID": d.get("user_id"),
        "UserName": d.get("user_name"),
        "ProfileURL": d.get("user_url"),
        "GroupID": d.get("group_id"),
        "TextPost": d.get("text") or d.get("post_summary"),
        "PostDate": d.get("created_at") or d.get("date"),
        "SellBuy": d.get("post_sell_or_buy") or d.get("sell_buy") or "unknown",
        "Main_object": d.get("main_object"),
        "City": d.get("post_city") or d.get("city"),
        "LandSize": d.get("post_land_size") or d.get("land_size"),
        "Price": d.get("post_price") or d.get("price"),
        "Contacts": d.get("post_user_contacts"), 
        "CommentsCount": d.get("comment_count", 0),
        "Contacted": d.get("contacted"),
        "Posturl": d.get("post_url"),
        "notes": d.get("notes"),
        "trash": d.get("trash")
    }


def map_comment(row):
    d = dict(row)
    return {
        "CommentID": d["comment_id"],
        "PostID": d["post_id"],
        "UserID": d.get("comment_user_id"),
        "UserName": d.get("comment_user_name") or "Anonymous",
        "ProfileURL": d.get("comment_user_url") or "",
        "PostOwnerID": d.get("post_owner_id"),
        "PostOwnerName": d.get("post_owner_name"),
        "PostOwnerURL": d.get("post_owner_url") or "",
        "TextComment": d.get("comment_text") or "",
        "TextPost": d.get("post_text") or "",
        "PostURL": d.get("post_url") or "",
        "CommentDate": d.get("comment_date"),
        "SellBuy": d.get("comment_sell_or_buy") or "unknown",
        "Main_object": d.get("comment_main_object"),
        "City": d.get("comment_city"),
        "LandSize": d.get("post_land_size"),
        "Price": d.get("post_price"),
        "Contacts": d.get("comment_user_contacts"),
        "Contacted": 0,
        "GroupID": d.get("group_id"),
        "notes": d.get("notes"),
    }






def map_group(row):
    d = dict(row)
    return {"GroupID": d.get("group_id"), "GroupName": d.get("group_name"), "GroupURL": d.get("group_url")}



@app.get("/posts")
def get_posts(user=Depends(auth_required)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM Post
        ORDER BY created_at DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return [map_post(r) for r in rows]

@app.get("/trash/posts")
def get_trash_posts(user=Depends(auth_required)):

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM Post
        WHERE trash = 1
        ORDER BY created_at DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return [map_post(r) for r in rows]



@app.patch("/api/posts/{post_id}/trash")
def move_post_to_trash(post_id: str, user=Depends(auth_required)):

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        UPDATE Post
        SET trash = 1
        WHERE post_id = ?
    """, (post_id,))

    conn.commit()
    conn.close()

    return {"success": True}

@app.patch("/api/posts/{post_id}/restore")
def restore_post(post_id: str, user=Depends(auth_required)):

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        UPDATE Post
        SET trash = 0
        WHERE post_id = ?
    """, (post_id,))

    conn.commit()
    conn.close()

    return {"success": True}


@app.get("/comments")
def get_comments(
    user=Depends(auth_required),
    user_id: Optional[str] = None
):
    conn = get_db()
    cur = conn.cursor()

    query = """
        SELECT
            c.id            AS comment_id,
            c.text          AS comment_text,
            c.comment_date  AS comment_date,
            c.notes        AS notes,

            cu.user_id      AS comment_user_id,
            cu.user_name    AS comment_user_name,
            cu.profile_url  AS comment_user_url,

            p.post_id       AS post_id,
            p.text          AS post_text,
            p.post_url      AS post_url,

            p.user_id       AS post_owner_id,
            p.user_name     AS post_owner_name,
            p.user_url      AS post_owner_url,

            c.comment_sell_or_buy,
            c.comment_main_object,
            c.comment_city,
            p.post_land_size,
            p.post_price,
            c.comment_user_contacts,
            p.group_id
        FROM Comment c
        JOIN Post p ON p.id = c.post_ref_id
        LEFT JOIN User cu ON cu.id = c.user_ref_id
    """

    params = []
    if user_id:
        query += " WHERE cu.user_id = ? "
        params.append(user_id)

    query += " ORDER BY c.comment_date DESC"

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    return [map_comment(r) for r in rows]



@app.get("/users")
def get_users(user=Depends(auth_required)):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        WITH posts_cnt AS (
            SELECT
                user_id,
                COUNT(*) AS posts_cnt
            FROM Post
            WHERE user_id IS NOT NULL AND TRIM(user_id) != ''
            GROUP BY user_id
        ),
        comments_cnt AS (
            SELECT
                u.user_id AS user_id,
                COUNT(c.id) AS comments_cnt
            FROM Comment c
            JOIN User u ON u.id = c.user_ref_id
            WHERE u.user_id IS NOT NULL AND TRIM(u.user_id) != ''
            GROUP BY u.user_id
        )

        SELECT
            u.user_id,
            u.user_name,
            u.profile_url,

            COALESCE(pc.posts_cnt, 0)    AS posts_cnt,
            COALESCE(cc.comments_cnt, 0) AS comments_cnt,

            COALESCE(u.activity, 0)  AS activity,
            COALESCE(u.contacted, 0) AS contacted,
            COALESCE(u.contacts, '') AS contacts

        FROM User u
        LEFT JOIN posts_cnt pc ON pc.user_id = u.user_id
        LEFT JOIN comments_cnt cc ON cc.user_id = u.user_id

        WHERE COALESCE(pc.posts_cnt,0) > 0
        OR COALESCE(cc.comments_cnt,0) > 0

        ORDER BY comments_cnt DESC;

    """)

    rows = cur.fetchall()
    conn.close()

    return [{
        "UserID": r["user_id"],
        "UserName": r["user_name"],
        "ProfileURL": r["profile_url"],
        "PostsAllCount": r["posts_cnt"],
        "CommsAllCount": r["comments_cnt"],
        "Activity": r["activity"],
        "Contacted": r["contacted"],
        "Contacts": r["contacts"],
    } for r in rows]

@app.patch("/users/{user_id}/status")
def update_user_status(user_id: str, data: dict = Body(...)):
    new_status = data.get("status")

    if new_status not in [0,1,2,3,4]:
        raise HTTPException(status_code=400, detail="Invalid status")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "SELECT contacted FROM User WHERE user_id=?",
        (user_id,)
    )
    row = cur.fetchone()

    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="User not found")

    old_status = row[0]

    if old_status == new_status:
        conn.close()
        return {"success": True, "message": "Status unchanged"}

    cur.execute(
        "UPDATE User SET contacted=? WHERE user_id=?",
        (new_status, user_id)
    )

    cur.execute(
        """
        INSERT INTO UserStatusHistory (user_id, old_status, new_status)
        VALUES (?, ?, ?)
        """,
        (user_id, old_status, new_status)
    )

    conn.commit()
    conn.close()

    return {"success": True}


@app.get("/groups")
def get_groups(user=Depends(auth_required)):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM "Group"')
    rows = cur.fetchall()
    conn.close()
    return [map_group(r) for r in rows]


@app.get("/all")
def get_all(user=Depends(auth_required)):
    return {
        "User": get_users(user),
        "Post": get_posts(user),
        "Comment": get_comments(user),
        "Group": get_groups(user),
    }


FIELDS_CONFIG = {
    "all": [
        "Type",
        "TextPost",
        "TextComment",
        "UserName",
        "ProfileURL",
        "Intent",
        "City",
        "Price",
        "Contacts",
        "Status",
        "notes"
    ],
    "posts": [
        "TextPost",
        "Posturl",
        "CommentsCount",
        "UserName",
        "ProfileURL",
        "UserID",
        "PostDate",
        "SellBuy",
        "City",
        "Object",
        "LandSize",
        "Price",
        "Contacts",
        "Contacted",
        "notes"
    ],
    "comments": [
        "TextComment",
        "TextPost",
        "PostURL",
        "UserName",
        "ProfileURL",
        "UserID",
        "CommentDate",
        "SellBuy",
        "Object",
        "City",
        "LandSize",
        "Price",
        "Contacts",
        "notes"
    ],
    "users": [
        "UserName",
        "ProfileURL",
        "UserID",
        "PostsAllCount",
        "CommsAllCount",
        "Activity",
        "Contacts",
        "Contacted",
        "notes"
    ]
}



@app.post("/export")
def export_data(payload: dict = Body(...), user=Depends(auth_required)):

    view   = payload.get("view")
    rows   = payload.get("rows", [])
    fields = payload.get("fields")
    filters = payload.get("filters", {})

    if not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="Rows must be list")

    # =====================
    # 🔹 DEDUP
    # =====================

    if view == "posts":
        seen = set()
        unique = []
        for p in rows:
            key = (
                (p.get("UserID") or "").strip(),
                (p.get("TextPost") or "").strip()
            )
            if key in seen:
                continue
            seen.add(key)
            unique.append(p)
        rows = unique

    elif view == "comments":
        seen = set()
        unique = []
        for c in rows:
            key = (
                (c.get("UserID") or "").strip(),
                (c.get("TextComment") or "").strip(),
                (c.get("TextPost") or "").strip()
            )
            if key in seen:
                continue
            seen.add(key)
            unique.append(c)
        rows = unique
    elif view == "all":
        seen = set()
        unique = []

        for row in rows:
            key = (
                (row.get("Type") or "").strip(),
                (row.get("UserID") or "").strip(),
                (row.get("TextPost") or "").strip(),
                (row.get("TextComment") or "").strip()
            )

            if key in seen:
                continue

            seen.add(key)
            unique.append(row)

        rows = unique

    # =====================
    # 🔹 FIELDS
    # =====================

    allowed = FIELDS_CONFIG.get(view, [])

    if fields:
        fields = [f for f in fields if f in allowed]
    else:
        fields = allowed

    # =====================
    # 🔹 CSV
    # =====================

    output = io.StringIO()
    output.write("\ufeff")  # UTF-8 BOM

    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()

    for row in rows:
        writer.writerow(row)

    output.seek(0)

    # =====================
    # 🔹 FILENAME
    # =====================

    today = datetime.now().strftime("%d.%m.%y")
    filename = f"Dataset_{today}.csv"

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )



@app.patch("/api/notes/{kind}/{item_id}")
def update_notes(kind: str, item_id: str, data: dict = Body(...), user=Depends(auth_required)):
    notes = data.get("notes", "")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if kind == "post":
        cur.execute("UPDATE Post SET notes=? WHERE post_id=?", (notes, item_id))

    elif kind == "comment":
        cur.execute("UPDATE Comment SET notes=? WHERE id=?", (notes, item_id))

    else:
        raise HTTPException(status_code=400, detail="Invalid kind")

    conn.commit()
    conn.close()
    return {"success": True}

from fastapi.responses import JSONResponse

METABASE_SITE_URL = "http://56.228.69.1:3000"
METABASE_SECRET_KEY = "b77610888a4861d643bacd2483f2370102f5a85144d8c136d8ac489e46b22cf0"


@app.get("/api/metabase/embed/dashboard/{dashboard_id}")
def embed_dashboard(dashboard_id: int):

    payload = {
        "resource": {"dashboard": dashboard_id},
        "params": {},
        "exp": int(time.time()) + 600  # 10 минут
    }

    token = jwt.encode(
        payload,
        METABASE_SECRET_KEY,
        algorithm="HS256"
    )

    iframe_url = (
        f"{METABASE_SITE_URL}/embed/dashboard/{token}"
        "#bordered=true&titled=true"
    )

    return {"url": iframe_url}
