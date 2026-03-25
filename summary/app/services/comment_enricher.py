from pathlib import Path

from summary.app.openai_client import run_structured_prompt
from summary.app.schemas.comment_schema import COMMENT_SCHEMA
from summary.app.config import MODEL
from summary.app.services.prompt_builder import build_system_prompt


# 📌 Абсолютный путь к prompts
BASE_DIR = Path(__file__).resolve().parent.parent
PROMPT_PATH = BASE_DIR / "prompts" / "comment_task.txt"

SYSTEM_PROMPT = build_system_prompt(PROMPT_PATH)


def enrich_comment(
    post_id: str,
    user_id: str,
    comment_text: str,
    post_text: str
) -> dict:
    try:
        result = run_structured_prompt(
            system_prompt=SYSTEM_PROMPT,
            user_payload={
                "comment_id": f"{post_id}:{user_id}",
                "comment_text": comment_text or "",
                "post_text": post_text or ""
            },
            schema=COMMENT_SCHEMA,
            model=MODEL
        )
    except Exception as e:
        print(f"[AI] ERROR comment {post_id}:{user_id}: {e}")
        return {
            "comment_location": "Unknown",
            "comment_intent": "Unknown",
            "comment_main_object": "Unknown",
        }

    if not isinstance(result, dict):
        print(f"[AI] EMPTY RESULT comment {post_id}:{user_id}")
        return {
            "comment_location": "Unknown",
            "comment_intent": "Unknown",
            "comment_main_object": "Unknown",
        }

    return {
        "comment_location": result.get("comment_location", "Unknown"),
        "comment_intent": result.get("comment_intent", "Unknown"),
        "comment_main_object": result.get("comment_main_object", "Unknown"),
    }
