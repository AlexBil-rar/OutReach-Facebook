# summary/app/main.py
import os
import time

from summary.app.services.post_enricher import enrich_post
from summary.app.services.comment_enricher import enrich_comment

from summary.app.utils.db import (
    fetch_posts,
    update_post_enrichment,
    fetch_comments,
    update_comment_enrichment,
)

RATE_LIMIT_SEC = float(os.getenv("RATE_LIMIT_SEC", "1.2"))

# Можно управлять из env:
# POSTS_LIMIT=200 COMMENTS_LIMIT=200 python3 -m summary.app.main
POSTS_LIMIT = int(os.getenv("POSTS_LIMIT", "0")) or None
COMMENTS_LIMIT = int(os.getenv("COMMENTS_LIMIT", "0")) or None

# MODE: posts | comments | both
MODE = os.getenv("MODE", "both").lower()


def run_posts():
    posts = fetch_posts(limit=POSTS_LIMIT)
    print(f"Found {len(posts)} posts to process")

    for idx, (post_id, post_text) in enumerate(posts, 1):
        try:
            preview = (post_text or "").replace("\n", " ")[:140]
            print(f"[POST {idx}] INPUT {post_id}: {preview}")

            if not post_text or not post_text.strip():
                print(f"[POST {idx}] SKIP {post_id}: empty text")
                update_post_enrichment(
                    post_id=post_id,
                    location="Unknown",
                    intent="Unknown",
                    main_object="Unknown",
                )
                continue

            result = enrich_post(post_id, post_text)

            if not result:
                print(f"[POST {idx}] AI RETURNED NONE {post_id}")
                continue

            update_post_enrichment(
                post_id=post_id,
                location=result["post_location"],
                intent=result["post_intent"],
                main_object=result["main_object"],
            )

            print(f"[POST {idx}] OK {post_id} → intent={result['post_intent']}, object={result['main_object']}")
            print(result)
            time.sleep(RATE_LIMIT_SEC)

        except Exception as e:
            print(f"[POST {idx}] ERROR {post_id}: {e}")


def run_comments():
    comments = fetch_comments(limit=COMMENTS_LIMIT)
    print(f"Found {len(comments)} comments to process")

    for idx, (post_id, user_id, comment_text, post_text) in enumerate(comments, 1):
        key = f"{post_id}:{user_id}"
        try:
            preview = (comment_text or "").replace("\n", " ")[:140]
            print(f"[COM {idx}] INPUT {key}: {preview}")

            result = enrich_comment(
                post_id=post_id,
                user_id=user_id,
                comment_text=comment_text,
                post_text=post_text,
            )

            update_comment_enrichment(
                post_id=post_id,
                user_id=user_id,
                location=result["comment_location"],
                intent=result["comment_intent"],
                main_object=result["comment_main_object"],
            )

            print(f"[COM {idx}] OK {key} → intent={result['comment_intent']}, object={result['comment_main_object']}")
            time.sleep(RATE_LIMIT_SEC)

        except Exception as e:
            print(f"[COM {idx}] ERROR {key}: {e}")


def main():
    if MODE == "posts":
        run_posts()
    elif MODE == "comments":
        run_comments()
    else:
        run_posts()
        run_comments()


if __name__ == "__main__":
    main()
