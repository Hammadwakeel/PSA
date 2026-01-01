from enum import Enum
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, ConfigDict, field_validator

# ==============================================================================
# 1. ENUMS (Type Safety)
# ==============================================================================
class DataSourceType(str, Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"
    MONGODB = "mongodb"
    REDIS = "redis"
    ELASTICSEARCH = "elasticsearch"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    S3 = "s3"
    KAFKA = "kafka"
    PINECONE = "pinecone"
    WEAVIATE = "weaviate"
    
class TableType(str, Enum):
    TABLE = "table"
    VIEW = "view"
    STREAM = "stream" 
    VECTOR_INDEX = "vector_index"
    PARQUET = "parquet"
    CSV = "csv"
    COLLECTION = "collection"

# ==============================================================================
# 2. SCHEMA DEFINITIONS
# ==============================================================================
class ColumnSchema(BaseModel):
    # CHANGED: 'ignore' allows extra fields (like 'comment') without crashing
    model_config = ConfigDict(extra='ignore') 

    column_name: str = Field(..., min_length=1, description="Name of the column")
    column_type: str = Field(..., description="Native data type (e.g. VARCHAR, INTEGER)")
    
    # ✅ FIXED: Added missing fields from your payload
    is_primary_key: bool = Field(False, description="Is this the PK?")
    is_foreign_key: bool = Field(False, description="Is this a FK?")
    is_nullable: bool = Field(True, description="Can this be null?")
    pii: bool = Field(False, description="Contains Personally Identifiable Information?")

class TableSchema(BaseModel):
    table_name: str = Field(..., min_length=1)
    table_type: TableType = Field(..., description="Physical storage type")
    columns: List[ColumnSchema] = Field(default_factory=list)
    
    file_path: Optional[str] = Field(None, description="Full S3/GCS path")
    file_format: Optional[str] = Field(None, description="Format if file-based (parquet/csv)")

class SchemaDetails(BaseModel):
    schema_name: str = Field("default", description="Database schema name")
    tables: List[TableSchema] = Field(default_factory=list)

# ==============================================================================
# 3. GOVERNANCE (Policy Models)
# ==============================================================================
class RLSRule(BaseModel):
    """
    Structured definition for a Row Level Security rule.
    """
    condition: str = Field(..., description="SQL predicate (e.g. region = 'US')")
    description: Optional[str] = Field(None, description="Human readable explanation")

class GovernanceRLS(BaseModel):
    enabled: bool = False
    # ✅ FIXED: Now supports simple strings OR structured rule objects
    rules: List[Union[RLSRule, str]] = Field(default_factory=list, description="List of RLS rules")

class GovernanceMasking(BaseModel):
    enabled: bool = False
    rules: List[str] = Field(default_factory=list, description="List of fields to mask")

class GovernancePolicies(BaseModel):
    row_level_security: Optional[GovernanceRLS] = None
    column_masking: Optional[GovernanceMasking] = None

# ==============================================================================
# 4. DATA SOURCES
# ==============================================================================
class DataSource(BaseModel):
    data_source_id: int = Field(..., gt=0, description="Internal ID of the source")
    name: str = Field(..., min_length=3, description="Human readable name")
    type: DataSourceType = Field(..., description="Supported engine type")
    
    schemas: List[SchemaDetails] = Field(default_factory=list)
    file_metadata: Optional[Dict[str, Any]] = Field(None, description="S3/File specific properties")
    topics: Optional[List[Dict[str, Any]]] = Field(None, description="Kafka/Stream metadata")
    
    governance_policies: Optional[GovernancePolicies] = None

# ==============================================================================
# 5. CONTEXT & REQUEST
# ==============================================================================
class ExecutionContext(BaseModel):
    max_rows: int = Field(1000, ge=1, le=100000)
    timeout_seconds: int = Field(30, ge=5, le=300)

class UserContext(BaseModel):
    user_id: int = Field(..., gt=0)
    workspace_id: int = Field(..., gt=0)
    organization_id: int = Field(..., gt=0)
    roles: List[str] = Field(default_factory=list)
    permissions: List[str] = Field(default_factory=list)
    attributes: Dict[str, Any] = Field(default_factory=dict)

class ExecutionRequest(BaseModel):
    """
    Primary payload for the RiverGen Execution Engine.
    """
    model_config = ConfigDict(str_strip_whitespace=True)

    request_id: str = Field(..., min_length=5, description="Unique Trace ID")
    execution_id: Optional[str] = None
    timestamp: Optional[str] = None
    
    user_context: UserContext
    
    user_prompt: str = Field(
        ..., 
        min_length=3, 
        max_length=5000, 
        description="Natural language query from the user"
    )
    
    data_sources: List[DataSource] = Field(..., min_length=1, description="Available data sources")
    
    execution_context: ExecutionContext = Field(default_factory=ExecutionContext)
    
    ai_model: Optional[str] = Field("meta-llama/llama-3.3-70b-versatile", description="Target LLM model")
    temperature: float = Field(0.1, ge=0.0, le=1.0, description="LLM Creativity")
    include_visualization: bool = Field(True, description="Request chart suggestions")
    
    @field_validator('data_sources')
    def validate_sources(cls, v):
        if not v:
            raise ValueError("At least one data source is required")
        return v

# ==============================================================================
# 6. RESPONSE SCHEMA
# ==============================================================================
class AIMetadata(BaseModel):
    generation_time_ms: int
    confidence_score: float
    explanation: Optional[str] = None
    reasoning_steps: List[str] = []
    # Added model field to match agent output
    model: Optional[str] = None 

class ExecutionResponse(BaseModel):
    """
    Standardized response format for the Execution API.
    """
    request_id: str
    status: str = Field(..., description="success, error, or partial")
    
    execution_id: Optional[str] = None
    plan_id: Optional[str] = None
    timestamp: Optional[str] = None
    
    intent_type: Optional[str] = None
    intent_summary: Optional[str] = None
    
    execution_plan: Optional[Dict[str, Any]] = None 
    
    visualization: Optional[List[Dict[str, Any]]] = None
    ai_metadata: Optional[AIMetadata] = None
    suggestions: List[str] = []
    
    error: Optional[str] = None