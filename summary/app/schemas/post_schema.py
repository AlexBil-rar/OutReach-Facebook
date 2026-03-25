POST_SCHEMA = {
    "type": "json_schema",
    "name": "post_enrichment",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "post_id": {"type": "string"},
            "post_location": {"type": "string"},
            "post_intent": {
                "type": "string",
                "enum": ["Sell", "Buy", "Unknown"]
            }
        },
        "required": ["post_id", "post_location", "post_intent"],
        "additionalProperties": False
    }
}
