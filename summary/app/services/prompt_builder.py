from pathlib import Path
import csv


BASE_DIR = Path(__file__).resolve().parent.parent
RULES_FILE = BASE_DIR / "rules" / "almost final - posts_location_intent.csv"


def build_system_prompt(task_prompt_path: Path) -> str:
    task_prompt_path = task_prompt_path.resolve()

    if not task_prompt_path.exists():
        raise FileNotFoundError(f"Prompt file not found: {task_prompt_path}")

    base_prompt = task_prompt_path.read_text(encoding="utf-8")

    if not RULES_FILE.exists():
        return base_prompt

    sell_keywords = set()
    buy_keywords = set()

    with RULES_FILE.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            text = (row.get("text") or "").strip()
            intent = (row.get("intent") or "").strip()

            if not text or intent not in {"Sell", "Buy"}:
                continue

            if len(text) <= 40:
                if intent == "Sell":
                    sell_keywords.add(text)
                elif intent == "Buy":
                    buy_keywords.add(text)

    rules_block = "\n\n====================\nD) Rules from dataset\n====================\n"

    if sell_keywords:
        rules_block += "\nSELL indicators (examples):\n"
        for kw in sorted(sell_keywords):
            rules_block += f"- {kw}\n"

    if buy_keywords:
        rules_block += "\nBUY indicators (examples):\n"
        for kw in sorted(buy_keywords):
            rules_block += f"- {kw}\n"

    return base_prompt + rules_block
