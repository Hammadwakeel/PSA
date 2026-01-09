import logging
import json
from typing import Dict, List
from fastapi import APIRouter, HTTPException, Response, status
from fastapi.responses import JSONResponse
from fastapi.concurrency import run_in_threadpool
from datetime import datetime
from app.schemas.payload import ExecutionRequest, ExecutionResponse, ErrorDetail, DataSourceType
from app.services.rivergen import run_rivergen_flow

# 1. Setup Structured Logging
logger = logging.getLogger("api_execution")

router = APIRouter(tags=["Execution"])

# Helper function to get supported data source types organized by category
def get_supported_data_source_types() -> Dict[str, List[str]]:
    """
    Returns all supported data source types organized by category.
    """
    return {
        "sql_databases": [
            DataSourceType.POSTGRESQL.value,
            DataSourceType.MYSQL.value,
            DataSourceType.MARIADB.value,
            DataSourceType.SQLSERVER.value,
            DataSourceType.ORACLE.value,
        ],
        "cloud_warehouses": [
            DataSourceType.SNOWFLAKE.value,
            DataSourceType.BIGQUERY.value,
            DataSourceType.REDSHIFT.value,
            DataSourceType.SYNAPSE.value,
            DataSourceType.DATABRICKS.value,
        ],
        "cloud_storage": [
            DataSourceType.S3.value,
            DataSourceType.AZURE_BLOB_STORAGE.value,
            DataSourceType.GCS.value,
        ],
        "file_formats": [
            DataSourceType.CSV.value,
            DataSourceType.EXCEL.value,
            DataSourceType.JSON.value,
            DataSourceType.PARQUET.value,
            DataSourceType.ORC.value,
            DataSourceType.DELTA_LAKE.value,
            DataSourceType.ICEBERG.value,
            DataSourceType.HUDI.value,
        ],
        "nosql_databases": [
            DataSourceType.MONGODB.value,
            DataSourceType.DYNAMODB.value,
            DataSourceType.CASSANDRA.value,
            DataSourceType.ELASTICSEARCH.value,
            DataSourceType.REDIS.value,
        ],
        "saas_apis": [
            DataSourceType.SALESFORCE.value,
            DataSourceType.HUBSPOT.value,
            DataSourceType.STRIPE.value,
            DataSourceType.JIRA.value,
            DataSourceType.SERVICENOW.value,
            DataSourceType.REST_API.value,
            DataSourceType.GRAPHQL_API.value,
        ],
        "streaming": [
            DataSourceType.KAFKA.value,
        ],
        "vector_databases": [
            DataSourceType.PINECONE.value,
            DataSourceType.WEAVIATE.value,
        ],
    }

@router.post(
    "/execute", 
    response_model=ExecutionResponse,
    summary="Execute AI Flow",
    description="Processes natural language prompts via the RiverGen Engine. Validates that all data source types are from the supported list."
)
async def execute_prompt(request: ExecutionRequest):
    """
    Primary endpoint to process natural language prompts against data sources.
    Uses threadpooling to prevent blocking the async event loop.
    
    Validates:
    - All data source types must be from the supported DataSourceType enum
    - At least one data source is required
    """
    request_id = request.request_id or "unknown"
    logger.info(f"🚀 [API] Received execution request: {request_id}")

    try:
        # Validation is handled by Pydantic model validators
        # Additional explicit validation for data source types
        supported_types = [ds_type.value for ds_type in DataSourceType]
        for source in request.data_sources:
            if source.type not in supported_types:
                # Return structured error response
                error_detail = ErrorDetail(
                    code="UNSUPPORTED_DATA_SOURCE_TYPE",
                    message=f"Unsupported data source type: '{source.type}'",
                    details={
                        "data_source_type": source.type,
                        "supported_types": sorted(supported_types)
                    },
                    suggested_fixes=[
                        f"Use one of the supported types: {', '.join(sorted(supported_types)[:5])}...",
                        "Check /api/v1/supported-data-sources endpoint for full list"
                    ]
                )
                error_response = ExecutionResponse(
                    request_id=request.request_id,
                    execution_id=request.execution_id,
                    status="error",
                    timestamp=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    error=error_detail
                )
                return JSONResponse(
                    content=error_response.model_dump(exclude_none=True),
                    status_code=status.HTTP_200_OK
                )
        
        # Convert Pydantic model to dict
        payload = request.model_dump()
        
        # ------------------------------------------------------------------
        # ⚡ CRITICAL FIX: Run Blocking Code in Threadpool
        # ------------------------------------------------------------------
        # Since 'run_rivergen_flow' is synchronous, we offload it to a worker thread.
        result_dict = await run_in_threadpool(run_rivergen_flow, payload)
        
        # Clean up result_dict: remove null fields for successful responses before processing
        if result_dict.get("status") == "success":
            # Remove these fields if they're None/null in successful responses
            if result_dict.get("error") is None:
                result_dict.pop("error", None)
            if result_dict.get("usage") is None:
                result_dict.pop("usage", None)
            if result_dict.get("warnings") is None:
                result_dict.pop("warnings", None)
            if result_dict.get("last_feedback") is None:
                result_dict.pop("last_feedback", None)
        
        # Check logical errors from the service layer
        if result_dict.get("status") == "error" or "error" in result_dict:
            logger.warning(f"⚠️ [API] Logic Error for {request_id}")
            
            # Convert error to structured ErrorDetail format
            error_data = result_dict.get("error")
            last_feedback = result_dict.get("last_feedback", "")
            
            # Handle different error formats from service layer
            if isinstance(error_data, dict) and "code" in error_data:
                # Already in structured format - ensure all fields are present
                error_detail = ErrorDetail(
                    code=error_data.get("code", "EXECUTION_ERROR"),
                    message=error_data.get("message", "An error occurred"),
                    details=error_data.get("details"),
                    suggested_fixes=error_data.get("suggested_fixes", [])
                )
            else:
                # Convert string/legacy format to structured format
                error_message = error_data if isinstance(error_data, str) else "Unknown processing error"
                
                # Determine error code from message or context
                error_code = "EXECUTION_ERROR"
                if "write" in error_message.lower() and ("permission" in error_message.lower() or "denied" in error_message.lower() or "not allowed" in error_message.lower()):
                    error_code = "WRITE_PERMISSION_DENIED"
                elif "permission" in error_message.lower() or "denied" in error_message.lower():
                    error_code = "PERMISSION_DENIED"
                elif "validation" in error_message.lower():
                    error_code = "VALIDATION_ERROR"
                elif "plan generation failed" in error_message.lower():
                    error_code = "PLAN_GENERATION_FAILED"
                
                # Build details from available context
                details = {}
                
                # Try to extract data source information from request if available
                if request.data_sources and len(request.data_sources) > 0:
                    # Use first data source as default, but service layer can override
                    primary_source = request.data_sources[0]
                    if error_code in ["WRITE_PERMISSION_DENIED", "PERMISSION_DENIED"]:
                        details["data_source_id"] = primary_source.data_source_id
                        details["data_source_type"] = primary_source.type.value if hasattr(primary_source.type, 'value') else str(primary_source.type)
                        details["data_source_name"] = primary_source.name
                        # Check if read_only is mentioned or infer from error
                        if "read only" in error_message.lower() or "read-only" in error_message.lower():
                            details["read_only"] = True
                        # Extract user permissions if mentioned in error
                        if "permission" in error_message.lower():
                            details["user_permissions"] = request.user_context.permissions if hasattr(request.user_context, 'permissions') else []
                
                if result_dict.get("usage"):
                    details["usage"] = result_dict.get("usage")
                if last_feedback:
                    details["judge_feedback"] = last_feedback
                
                # Build suggested fixes based on error type
                suggested_fixes = []
                if error_code == "WRITE_PERMISSION_DENIED":
                    suggested_fixes.extend([
                        "Use a different data source that supports writes",
                        "Contact administrator to enable write permissions"
                    ])
                elif error_code == "PERMISSION_DENIED":
                    suggested_fixes.extend([
                        "Check user permissions for the requested data source",
                        "Contact administrator to enable required permissions"
                    ])
                elif error_code == "VALIDATION_ERROR" or last_feedback:
                    suggested_fixes.extend([
                        "Review the query and ensure all referenced tables/columns exist",
                        "Check data source schema matches the query requirements"
                    ])
                elif error_code == "PLAN_GENERATION_FAILED":
                    suggested_fixes.extend([
                        "Review the query prompt for clarity",
                        "Ensure data sources are properly configured",
                        "Check schema definitions match actual data source structure"
                    ])
                else:
                    suggested_fixes.append("Review the error details and contact support if the issue persists")
                
                error_detail = ErrorDetail(
                    code=error_code,
                    message=error_message,
                    details=details if details else None,
                    suggested_fixes=suggested_fixes
                )
            
            # Build error response
            error_response = ExecutionResponse(
                request_id=request.request_id,
                execution_id=request.execution_id or result_dict.get("execution_id"),
                status="error",
                timestamp=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                error=error_detail
            )
            
            # Return error response with 400 status code
            # Use JSONResponse to properly serialize the Pydantic model
            return JSONResponse(
                content=error_response.model_dump(exclude_none=True),
                status_code=status.HTTP_200_OK
            )
        
        # Convert dict result to Pydantic ExecutionResponse model
        # ExecutionResponse uses extra='allow' to handle any additional fields from service layer
        response = ExecutionResponse.model_validate(result_dict)
        
        # Clean up response: remove null error/usage/warnings/last_feedback fields for successful responses
        response_dict = response.model_dump(exclude_none=True)
        
        # For successful responses, explicitly remove these fields even if they exist (they should only appear in error responses)
        if response_dict.get("status") == "success":
            # Remove these fields from successful responses - they should only appear in error responses
            response_dict.pop("error", None)
            response_dict.pop("usage", None)  # Usage is in ai_metadata for success, so remove top-level usage
            response_dict.pop("warnings", None)
            response_dict.pop("last_feedback", None)
            
        logger.info(f"✅ [API] Success for {request_id}")
        # Return JSONResponse with cleaned dict to ensure null fields are not included
        return JSONResponse(
            content=response_dict,
            status_code=status.HTTP_200_OK
        )

    except HTTPException as http_exc:
        # Check if it's already a structured error response
        # If not, the global handler in main.py will convert it
        raise

    except Exception as e:
        # 🔒 SECURITY FIX: Log the real error internally, hide raw traceback from user
        logger.error(f"❌ [API] System Crash for {request_id}: {str(e)}", exc_info=True)
        
        # Return structured error response
        error_detail = ErrorDetail(
            code="INTERNAL_SERVER_ERROR",
            message="Internal Server Error. Please contact support.",
            details={
                "request_id": request_id,
                "error_type": type(e).__name__
            },
            suggested_fixes=[
                f"Contact support with Request ID: {request_id}",
                "Check API logs for detailed error information"
            ]
        )
        error_response = ExecutionResponse(
            request_id=request.request_id,
            execution_id=request.execution_id,
            status="error",
            timestamp=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            error=error_detail
        )
        return JSONResponse(
            content=error_response.model_dump(exclude_none=True),
            status_code=status.HTTP_200_OK
        )

@router.get(
    "/supported-data-sources",
    response_model=Dict[str, List[str]],
    summary="Get Supported Data Source Types",
    description="Returns all supported data source types organized by category."
)
async def get_supported_data_sources():
    """
    Returns a dictionary of all supported data source types organized by category.
    Useful for API consumers to know what types are available.
    """
    return get_supported_data_source_types()