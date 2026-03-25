from pathlib import Path

from summary.app.openai_client import run_structured_prompt
from summary.app.schemas.post_schema import POST_SCHEMA
from summary.app.config import MODEL
from summary.app.services.prompt_builder import build_system_prompt


# 📌 Абсолютный путь к prompts
BASE_DIR = Path(__file__).resolve().parent.parent
PROMPT_PATH = BASE_DIR / "prompts" / "post_task.txt"

SYSTEM_PROMPT = build_system_prompt(PROMPT_PATH)


def enrich_post(post_id: str, post_text: str) -> dict:
    try:
        result = run_structured_prompt(
            system_prompt=SYSTEM_PROMPT,
            user_payload={
                "post_id": post_id,
                "post_text": post_text or ""
            },
            schema=POST_SCHEMA,
            model=MODEL
        )
    except Exception as e:
        print(f"[AI] ERROR post {post_id}: {e}")
        return {
            "post_id": post_id,
            "post_location": "Unknown",
            "post_intent": "Unknown",
            "main_object": "Unknown",
        }

    if not isinstance(result, dict):
        print(f"[AI] EMPTY RESULT post {post_id}")
        return {
            "post_id": post_id,
            "post_location": "Unknown",
            "post_intent": "Unknown",
            "main_object": "Unknown",
        }

    return {
        "post_id": post_id,
        "post_location": result.get("post_location", "Unknown"),
        "post_intent": result.get("post_intent", "Unknown"),
        "main_object": result.get("main_object", "Unknown"),
    }
