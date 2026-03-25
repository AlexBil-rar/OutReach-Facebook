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
            },
            "comment_main_object": {
                "type": "string",
                "enum": [
                    "Land",
                    "Residential property",
                    "Offices",
                    "Warehouses/logistics",
                    "Commercial",
                    "Unknown"
                ]
            }
        },
        "required": ["comment_id", "comment_location", "comment_intent", "comment_main_object"],
        "additionalProperties": False
    }
}
