from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class ColumnSchema(BaseModel):
    column_name: str
    column_type: str
    is_primary_key: Optional[bool] = False
    is_nullable: Optional[bool] = True

class TableSchema(BaseModel):
    table_name: str
    table_type: str  # e.g., 'table', 'view', 'stream', 'vector_index', 'parquet'
    columns: List[ColumnSchema]
    # Add optional fields for Big Data / File sources
    file_path: Optional[str] = None
    file_format: Optional[str] = None

class SchemaDetails(BaseModel):
    schema_name: str
    tables: List[TableSchema]

class GovernanceRLS(BaseModel):
    enabled: bool = False
    rules: List[Dict[str, str]] = [] # Added to support rule definitions

class GovernanceMasking(BaseModel):
    enabled: bool = False
    rules: List[str] = []

class GovernancePolicies(BaseModel):
    row_level_security: Optional[GovernanceRLS] = None
    column_masking: Optional[GovernanceMasking] = None

class DataSource(BaseModel):
    data_source_id: int
    name: str
    type: str  # e.g., 'postgresql', 'kafka', 's3', 'pinecone'
    # FIX 1: Make schemas optional with a default empty list
    schemas: List[SchemaDetails] = [] 
    # FIX 2: Add file_metadata field to support S3/Data Lake payloads
    file_metadata: Optional[Dict[str, Any]] = None 
    governance_policies: Optional[GovernancePolicies] = None
    # FIX 3: Add optional topics field for Kafka support
    topics: Optional[List[Dict[str, Any]]] = None

class ExecutionContext(BaseModel):
    max_rows: int = 1000
    timeout_seconds: int = 30

class UserContext(BaseModel):
    user_id: int
    workspace_id: int
    organization_id: int
    roles: List[str] = []
    permissions: List[str] = []
    attributes: Dict[str, Any] = {}

class ExecutionRequest(BaseModel):
    request_id: str
    execution_id: Optional[str] = None
    timestamp: Optional[str] = None
    user_context: UserContext
    user_prompt: str
    data_sources: List[DataSource]
    execution_context: Optional[ExecutionContext] = None
    ai_model: Optional[str] = None
    temperature: Optional[float] = 0.1
    include_visualization: Optional[bool] = True