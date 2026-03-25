import os
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("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")
OPENAI_PROJECT_ID = os.getenv("proj_pEFp3JcscgzfmDmlTi62ayUl")

MODEL = "gpt-4.1-mini"
