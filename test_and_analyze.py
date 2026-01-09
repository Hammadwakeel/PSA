#!/usr/bin/env python3
"""
Test and analyze API responses to check query correctness and governance policy application.
"""

import json
import requests
import re
from typing import Dict, Any, List

BASE_URL = "http://localhost:8000"
ENDPOINT = f"{BASE_URL}/api/v1/execute"

# Test Case 1: PostgreSQL with RLS and Column Masking
TEST_CASE_1 = {
    "request_id": "req-test-001",
    "execution_id": "exec-test-001",
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
    "user_prompt": "Show me top 10 customers by revenue this quarter",
    "model_name": "openai/gpt-oss-120b",
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
                            "columns": [
                                {"column_name": "id", "column_type": "integer", "is_nullable": False, "is_primary_key": True},
                                {"column_name": "name", "column_type": "varchar(255)", "is_nullable": False},
                                {"column_name": "revenue", "column_type": "decimal(10,2)", "is_nullable": True},
                                {"column_name": "region", "column_type": "varchar(50)", "is_nullable": True},
                                {"column_name": "email", "column_type": "varchar(255)", "is_nullable": False, "pii": True}
                            ]
                        },
                        {
                            "table_name": "orders",
                            "table_type": "table",
                            "columns": [
                                {"column_name": "order_id", "column_type": "integer", "is_nullable": False, "is_primary_key": True},
                                {"column_name": "customer_id", "column_type": "integer", "is_nullable": False, "is_foreign_key": True},
                                {"column_name": "order_date", "column_type": "date", "is_nullable": False},
                                {"column_name": "total_amount", "column_type": "decimal(10,2)", "is_nullable": False}
                            ]
                        }
                    ]
                }
            ],
            "governance_policies": {
                "row_level_security": {
                    "enabled": True,
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


def analyze_query(query: str, user_prompt: str, governance_rules: Dict, user_context: Dict) -> Dict[str, Any]:
    """Analyze the generated query for correctness and governance compliance."""
    issues = []
    warnings = []
    positives = []
    
    query_upper = query.upper()
    query_lower = query.lower()
    user_region = user_context.get("attributes", {}).get("assigned_region", "")
    
    print(f"\n🔍 ANALYZING QUERY:")
    print("=" * 80)
    print(query)
    print("=" * 80)
    
    # Check 1: RLS Application - CRITICAL
    if "user_access" in query_lower:
        issues.append({
            "type": "RLS_NOT_PROPERLY_APPLIED",
            "severity": "CRITICAL",
            "message": "Query still references non-existent 'user_access' table. RLS condition should be replaced with literal value from user context.",
            "expected": f"region = '{user_region}' or region IN ('{user_region}')",
            "found": "Query contains subquery: region IN (SELECT region FROM user_access WHERE user_id = 1)",
            "fix": f"Replace the subquery with: c.region = '{user_region}'"
        })
    elif f"region = '{user_region}'" in query or f"region IN ('{user_region}')" in query or f"region='{user_region}'" in query.replace(" ", ""):
        positives.append(f"✅ RLS correctly applied with literal region filter: region = '{user_region}'")
    elif "region" in query_lower and "WHERE" in query_upper:
        # Check if region filter exists but might not be using literal
        region_pattern = r"region\s*(?:IN|in|=)\s*\([^)]+\)"
        if re.search(region_pattern, query):
            issues.append({
                "type": "RLS_USES_SUBQUERY",
                "severity": "HIGH",
                "message": "RLS filter uses subquery instead of literal value from user context",
                "expected": f"c.region = '{user_region}'",
                "found": "Subquery in WHERE clause"
            })
    
    # Check 2: Column Masking - Conditional masking based on region
    if "email" in query_lower:
        # Check if masking considers the condition (region != assigned_region)
        has_email_mask = "CASE" in query_upper and "email" in query_lower
        if has_email_mask:
            # Check if it properly masks only when region != user_region
            if user_region and f"region != '{user_region}'" in query or f"region <> '{user_region}'" in query:
                positives.append(f"✅ Email masking correctly applies condition: masks when region != '{user_region}'")
            elif "LENGTH" in query_upper and "CONCAT" in query_upper:
                warnings.append({
                    "type": "EMAIL_MASKING_MISSING_CONDITION",
                    "message": f"Email is masked but doesn't check condition: should only mask when region != '{user_region}'. Current masking applies to all rows.",
                    "expected": f"CASE WHEN region != '{user_region}' THEN email ELSE <masked> END"
                })
            
            # Check masking format
            if "SUBSTR" in query_upper or "SUBSTRING" in query_upper:
                if "@" in query:
                    positives.append("✅ Email masking uses proper format with @ symbol handling")
                else:
                    warnings.append({
                        "type": "EMAIL_MASKING_FORMAT",
                        "message": "Email masking should preserve email domain format (xxx@domain.com), but current implementation may not handle @ properly"
                    })
        else:
            issues.append({
                "type": "EMAIL_NOT_MASKED",
                "severity": "HIGH",
                "message": "Email column is selected but no masking is applied despite governance policy"
            })
    
    # Check 3: Top 10 Requirement
    if "top 10" in user_prompt.lower() or "top ten" in user_prompt.lower():
        if "LIMIT 10" in query_upper:
            positives.append("✅ Correctly limits to top 10 as requested")
        elif "LIMIT" in query_upper:
            limit_match = re.search(r"LIMIT\s+(\d+)", query_upper)
            if limit_match:
                limit_val = int(limit_match.group(1))
                if limit_val != 10:
                    warnings.append({
                        "type": "INCORRECT_LIMIT",
                        "message": f"User asked for top 10 but query uses LIMIT {limit_val}"
                    })
        else:
            issues.append({
                "type": "MISSING_LIMIT",
                "severity": "MEDIUM",
                "message": "User requested 'top 10' but query doesn't have LIMIT clause"
            })
    
    # Check 4: Current Quarter Filter
    if "quarter" in user_prompt.lower() or "this quarter" in user_prompt.lower():
        if "EXTRACT(QUARTER" in query_upper:
            positives.append("✅ Quarter extraction detected")
        else:
            warnings.append({
                "type": "MISSING_QUARTER_EXTRACTION",
                "message": "User mentioned 'quarter' but query doesn't extract quarter"
            })
        
        # Check for current quarter filter
        if "EXTRACT(QUARTER FROM CURRENT_DATE)" in query_upper or "QUARTER(CURRENT_DATE)" in query_upper:
            positives.append("✅ Current quarter filter applied")
        elif "quarter = " in query_lower or "quarter=" in query_lower.replace(" ", ""):
            # Might be filtering by quarter
            positives.append("✅ Quarter filtering detected")
        else:
            warnings.append({
                "type": "MISSING_CURRENT_QUARTER_FILTER",
                "message": "User asked for 'this quarter' but query doesn't filter for current quarter"
            })
    
    # Check 5: Revenue Calculation
    if "revenue" in user_prompt.lower():
        if "revenue" in query_lower:
            positives.append("✅ Revenue column included in query")
        else:
            issues.append({
                "type": "MISSING_REVENUE",
                "severity": "HIGH",
                "message": "User asked for revenue but query doesn't include revenue column"
            })
        
        # Check ORDER BY revenue DESC for "top"
        if "ORDER BY revenue DESC" in query_upper or "ORDER BY revenue DESCENDING" in query_upper:
            positives.append("✅ Results ordered by revenue descending (correct for 'top' query)")
        elif "ORDER BY revenue" in query_upper:
            warnings.append({
                "type": "MISSING_DESC_ORDER",
                "message": "Results ordered by revenue but missing DESC - 'top' results should be descending"
            })
    
    # Check 6: Syntax Validation
    syntax_checks = [
        ("SELECT", "SELECT statement"),
        ("FROM", "FROM clause"),
    ]
    
    for keyword, name in syntax_checks:
        if keyword not in query_upper:
            issues.append({
                "type": "INVALID_SYNTAX",
                "severity": "CRITICAL",
                "message": f"Query missing {name}"
            })
    
    # Check 7: GROUP BY and aggregation
    if "SUM" in query_upper or "COUNT" in query_upper or "AVG" in query_upper:
        if "GROUP BY" not in query_upper:
            issues.append({
                "type": "MISSING_GROUP_BY",
                "severity": "HIGH",
                "message": "Query uses aggregation functions but doesn't have GROUP BY clause"
            })
    
    return {
        "issues": issues,
        "warnings": warnings,
        "positives": positives,
        "total_issues": len(issues),
        "total_warnings": len(warnings)
    }


def test_and_analyze():
    """Test the API and analyze responses."""
    print("=" * 80)
    print("TESTING AND ANALYZING API RESPONSES")
    print("=" * 80)
    
    # Test Case 1
    print("\n" + "=" * 80)
    print("TEST CASE 1: Top 10 customers by revenue this quarter")
    print("=" * 80)
    print(f"Prompt: {TEST_CASE_1['user_prompt']}")
    print(f"User Region: {TEST_CASE_1['user_context']['attributes']['assigned_region']}")
    print("\nSending request...")
    
    try:
        response = requests.post(
            ENDPOINT,
            json=TEST_CASE_1,
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Request successful\n")
            
            # Extract query
            execution_plan = result.get("execution_plan", {})
            operations = execution_plan.get("operations", [])
            
            if operations:
                query = operations[0].get("query", "")
                
                # Analyze query
                governance = TEST_CASE_1["data_sources"][0]["governance_policies"]
                analysis = analyze_query(
                    query, 
                    TEST_CASE_1["user_prompt"], 
                    governance,
                    TEST_CASE_1["user_context"]
                )
                
                print("\n" + "=" * 80)
                print("ANALYSIS RESULTS:")
                print("=" * 80)
                
                if analysis["positives"]:
                    print("\n✅ POSITIVES:")
                    for positive in analysis["positives"]:
                        print(f"  {positive}")
                
                if analysis["warnings"]:
                    print(f"\n⚠️  WARNINGS ({analysis['total_warnings']}):")
                    for warning in analysis["warnings"]:
                        if isinstance(warning, dict):
                            print(f"\n  [{warning.get('type', 'WARNING')}]")
                            print(f"  {warning.get('message', '')}")
                            if "expected" in warning:
                                print(f"  Expected: {warning['expected']}")
                        else:
                            print(f"  {warning}")
                
                if analysis["issues"]:
                    print(f"\n❌ ISSUES FOUND ({analysis['total_issues']}):")
                    for issue in analysis["issues"]:
                        print(f"\n  [{issue['severity']}] {issue['type']}")
                        print(f"  Message: {issue['message']}")
                        if "expected" in issue:
                            print(f"  Expected: {issue['expected']}")
                        if "found" in issue:
                            print(f"  Found: {issue['found']}")
                        if "fix" in issue:
                            print(f"  Fix: {issue['fix']}")
                else:
                    print("\n✅ No critical issues found!")
                
                # Governance Applied Check
                print("\n" + "-" * 80)
                print("GOVERNANCE POLICY CHECK:")
                print("-" * 80)
                governance_applied = operations[0].get("governance_applied", {})
                rls_rules = governance_applied.get("rls_rules", [])
                masking_rules = governance_applied.get("masking_rules", [])
                
                print(f"RLS Rules in metadata: {len(rls_rules)}")
                for rule in rls_rules:
                    print(f"  - {rule[:100]}...")
                
                print(f"\nMasking Rules in metadata: {len(masking_rules)}")
                for rule in masking_rules:
                    print(f"  - {rule}")
                
                # Intent Summary
                print("\n" + "-" * 80)
                print("INTENT SUMMARY:")
                print("-" * 80)
                print(result.get("intent_summary", "N/A"))
                
                # AI Metadata
                if result.get("ai_metadata"):
                    metadata = result["ai_metadata"]
                    print("\n" + "-" * 80)
                    print("AI METADATA:")
                    print("-" * 80)
                    print(f"Confidence Score: {metadata.get('confidence_score', 'N/A')}")
                    print(f"Generation Time: {metadata.get('generation_time_ms', 'N/A')} ms")
                
                return {
                    "test_case": "TEST_CASE_1",
                    "success": True,
                    "query": query,
                    "analysis": analysis,
                    "response": result
                }
            else:
                print("❌ No operations found in execution plan")
                print(f"Response structure: {json.dumps(result, indent=2)[:500]}")
                return {"test_case": "TEST_CASE_1", "success": False, "error": "No operations"}
        else:
            print(f"❌ Request failed with status {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return {"test_case": "TEST_CASE_1", "success": False, "error": f"HTTP {response.status_code}"}
            
    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"test_case": "TEST_CASE_1", "success": False, "error": str(e)}


if __name__ == "__main__":
    result = test_and_analyze()
    
    # Save results
    with open("analysis_result.json", "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n📊 Analysis results saved to analysis_result.json")
