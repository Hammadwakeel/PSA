from typing import List, Dict, Optional
from pydantic import BaseModel, Field, ConfigDict

class DecideIntent(BaseModel):
    detect_psa_intent: bool = Field(
        ...,
        description="True if the user query maps to any PSA intent. False otherwise."
    )
    psa_intent: Optional[str] = Field(
        default=None,
        description="Primary PSA intent name. Return null/None if not detected."
    )
    psa_sub_intent: Optional[str] = Field(
        default=None,
        description="Sub-intent name. Return null/None if not detected."
    )

class GovernancePolicy(BaseModel):
    # Explanation is now Optional, defaults to None
    explanation: Optional[str] = Field(
        default=None,
        description="Explanation of the policy. Must be null/None if the policy is not available."
    )
    available: bool = Field(
        ...,
        description="True only if the policy is explicitly enabled in the data source configuration."
    )

class GovernanceApply(BaseModel):
    # Using aliases to match your required JSON output format
    rbac: GovernancePolicy = Field(..., alias="RBAC")
    row_level_security: GovernancePolicy = Field(..., alias="Row-Level-Security")
    data_masking: GovernancePolicy = Field(..., alias="Data Masking")
    query_limits: GovernancePolicy = Field(..., alias="Query Limits")

    model_config = ConfigDict(populate_by_name=True)