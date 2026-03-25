import json
import os
from datetime import datetime
from openai import OpenAI
from summary.app.config import OPENAI_API_KEY

client = OpenAI(api_key=OPENAI_API_KEY)

DEBUG_OPENAI = os.getenv("DEBUG_OPENAI") == "1"
DEBUG_LOG_DIR = os.getenv("DEBUG_LOG_DIR", "logs")
DEBUG_LOG_PATH = os.path.join(DEBUG_LOG_DIR, "openai_debug.jsonl")


def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _log_debug(record: dict) -> None:
    if not DEBUG_OPENAI:
        return
    _safe_mkdir(DEBUG_LOG_DIR)
    record["_ts"] = datetime.utcnow().isoformat() + "Z"
    with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def run_structured_prompt(
    *,
    system_prompt: str,
    user_payload: dict,
    schema: dict,
    model: str
) -> dict:
    user_content = json.dumps(user_payload, ensure_ascii=False)

    _log_debug({
        "event": "request",
        "model": model,
        "system_prompt_preview": system_prompt[:800],
        "user_payload": user_payload,
    })

    response = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content}
        ],
        text={"format": schema}
    )

    # Сырой дамп ответа (важно!)
    try:
        raw = response.model_dump()
    except Exception:
        raw = {"repr": repr(response)}

    _log_debug({
        "event": "response_raw",
        "raw": raw
    })

    # Достаём structured output максимально надёжно
    message = response.output[0].content[0]

    if hasattr(message, "parsed") and message.parsed is not None:
        parsed = message.parsed
    elif hasattr(message, "text"):
        parsed = json.loads(message.text)
    else:
        raise RuntimeError("Unexpected OpenAI response format")

    _log_debug({
        "event": "response_parsed",
        "parsed": parsed
    })

    return parsed
