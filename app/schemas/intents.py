PSA_INTENTS = {
    "data_discover": [
        "schema_discovery",
        "data_preview",
    ],
   
}

PSA_INTENTS_TOOLS = {
    "schema_discovery": {
        "sequence_required": True,
        "tools": [
            {"order": 1, "name": "test_connection", "desc": "Test connection to the data source first"},
            {"order": 2, "name": "discover_schema", "desc": "Run schema discovery after connection is verified"}
        ]
    },
    "data_preview": {
        "sequence_required": False,
        "tools": ["get_schemas"]
    }
}