from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ExecutionContext(BaseModel):
    max_rows: Optional[int] = None
    timeout_seconds: Optional[int] = None


class DataSource(BaseModel):
    data_source_id: int
    name: str
    type: str
    schemas: Optional[List[Dict[str, Any]]] = None
    governance_policies: Optional[Dict[str, Any]] = None


class ExecutionRequest(BaseModel):
    """Request body for the execution endpoint."""

    request_id: Optional[str] = None
    execution_id: Optional[str] = None
    timestamp: Optional[str] = None
    user_context: Dict[str, Any] = Field(default_factory=dict)
    user_prompt: str = ""
    data_sources: List[DataSource] = Field(default_factory=list)
    execution_context: Optional[ExecutionContext] = None
    available_governance_policies: Optional[Dict[str, Any]] = None

    def to_request_dict(self) -> dict:
        """Convert to dict format expected by rivergen.run()."""
        return {
            "request_id": self.request_id,
            "execution_id": self.execution_id,
            "timestamp": self.timestamp,
            "user_context": self.user_context,
            "user_prompt": self.user_prompt,
            "data_sources": [ds.model_dump() for ds in self.data_sources],
            "execution_context": self.execution_context.model_dump() if self.execution_context else None,
            "available_governance_policies": self.available_governance_policies,
        }


class ToolCallStep(BaseModel):
    """One tool invocation within a step."""
    tool_name: str
    args: Dict[str, Any] = Field(default_factory=dict)
    output: str = ""


class StepRecord(BaseModel):
    """One planning step: previous model message (response_text) and tool calls made."""
    step_index: int
    tool_calls: List[ToolCallStep] = Field(default_factory=list)
    response_text: str = ""  # model's previous response.text that triggered these tool calls


class ExecutionResponse(BaseModel):
    """Response body for POST /execute."""
    final_message: str = ""
    steps: List[StepRecord] = Field(default_factory=list)


# Example payload (kept for reference / tests)
EXAMPLE_EXECUTION_REQUEST = {
    "request_id": "req-12345",
    "execution_id": "exec-67890",
    "timestamp": "2025-01-15T10:00:00Z",
    "user_context": {
        "user_id": 1,
        "workspace_id": 5,
        "organization_id": 10,
        "roles": ["analyst", "sales"],
        "permissions": ["read:customers", "read:orders"],
        "attributes": {
            "assigned_region": "US-WEST",
            "department": "Sales"
        }
    },
    "user_prompt": "i want to discover the schema of data",
    "data_sources": [
        {
            "data_source_id": 1,
            "name": "PostgreSQL Production",
            "type": "postgresql",
            "schemas": [
                {
                    "schema_name": "public",
                    "tables": [
                        {
                            "table_name": "customers",
                            "table_type": "table",
                            "row_count": 45000,
                            "indexes": ["idx_region", "idx_segment"],
                            "columns": [
                                {
                                    "column_name": "id",
                                    "column_type": "integer",
                                    "is_nullable": False,
                                    "is_primary_key": True,
                                    "is_foreign_key": False,
                                    "column_comment": "Customer ID"
                                },
                                {
                                    "column_name": "name",
                                    "column_type": "varchar(255)",
                                    "is_nullable": False,
                                    "is_primary_key": False,
                                    "is_foreign_key": False
                                },
                                {
                                    "column_name": "revenue",
                                    "column_type": "decimal(10,2)",
                                    "is_nullable": True,
                                    "is_primary_key": False,
                                    "is_foreign_key": False
                                },
                                {
                                    "column_name": "region",
                                    "column_type": "varchar(50)",
                                    "is_nullable": False
                                },
                                {
                                    "column_name": "email",
                                    "column_type": "varchar(255)",
                                    "is_nullable": False,
                                    "pii": True
                                }
                            ]
                        }
                    ]
                }
            ],
            "governance_policies": {
                "row_level_security": {
                    "enabled": False,
                    "rules": [
                        {
                            "condition": "region IN (SELECT region FROM user_access WHERE user_id = {user_id})",
                            "description": "Users can only see customers in their assigned regions"
                        }
                    ]
                },
                "column_masking": {
                    "enabled": True,
                    "rules": [
                        {
                            "column": "email",
                            "condition": "region != {user.attributes.assigned_region}",
                            "masking_function": "email_mask",
                            "description": "Mask emails for users outside assigned region"
                        }
                    ]
                }
            }
        }
    ],
    "execution_context": {
        "max_rows": 1000,
        "timeout_seconds": 30
    }
}
