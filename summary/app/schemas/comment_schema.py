COMMENT_SCHEMA = {
    "type": "json_schema",
    "name": "comment_enrichment",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "comment_id": {"type": "string"},
            "comment_location": {"type": "string"},
            "comment_intent": {
                "type": "string",
                "enum": ["Sell", "Buy", "Unknown"]
            }
        },
        "required": ["comment_id", "comment_location", "comment_intent"],
        "additionalProperties": False
    }
}
