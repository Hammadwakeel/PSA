#!/usr/bin/env python3
"""
Test script to run 4 test samples against the RiverGen API endpoint
and generate a comprehensive report.
"""

import json
import requests
import time
from datetime import datetime
from typing import Dict, Any, List

# API Configuration
BASE_URL = "http://localhost:8000"
ENDPOINT = f"{BASE_URL}/api/v1/execute"

# Base request payload
BASE_PAYLOAD = {
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
    "user_prompt": "Show me top 10 customers by revenue this quarter",
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
                                    "is_nullable": True
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
                    "rules": ["email"]
                }
            }
        }
    ],
    "selected_schema_names": ["public"],
    "execution_context": {
        "max_rows": 1000,
        "timeout_seconds": 30
    },
    "model_name": "meta-llama/llama-4-scout-17b-16e-instruct",
    "include_visualization": True
}

# Test cases with variations
TEST_CASES = [
    {
        "test_id": "TEST-001",
        "name": "Top Customers by Revenue",
        "description": "Original request - Top 10 customers by revenue this quarter",
        "payload": {
            **BASE_PAYLOAD,
            "request_id": "req-test-001",
            "user_prompt": "Show me top 10 customers by revenue this quarter"
        }
    },
    {
        "test_id": "TEST-002",
        "name": "Customer Count by Region",
        "description": "Aggregation query - Count customers grouped by region",
        "payload": {
            **BASE_PAYLOAD,
            "request_id": "req-test-002",
            "user_prompt": "How many customers are in each region? Show me a breakdown by region"
        }
    },
    {
        "test_id": "TEST-003",
        "name": "High Revenue Customers Filter",
        "description": "Filter query - Customers with revenue above 10000",
        "payload": {
            **BASE_PAYLOAD,
            "request_id": "req-test-003",
            "user_prompt": "Find all customers with revenue greater than 10000 and show their names and regions"
        }
    },
    {
        "test_id": "TEST-004",
        "name": "Customer Revenue Statistics",
        "description": "Statistical query - Average, min, max revenue analysis",
        "payload": {
            **BASE_PAYLOAD,
            "request_id": "req-test-004",
            "user_prompt": "What is the average, minimum, and maximum revenue across all customers?",
            "model_name": "meta-llama/llama-4-maverick-17b-128e-instruct"
        }
    }
]


def check_health() -> bool:
    """Check if the API is running and healthy."""
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return False


def run_test(test_case: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a single test case and return results."""
    test_id = test_case["test_id"]
    payload = test_case["payload"]
    
    print(f"\n{'='*60}")
    print(f"Running {test_id}: {test_case['name']}")
    print(f"{'='*60}")
    print(f"Prompt: {test_case['payload']['user_prompt']}")
    print(f"Model: {test_case['payload']['model_name']}")
    
    start_time = time.time()
    result = {
        "test_id": test_id,
        "test_name": test_case["name"],
        "description": test_case["description"],
        "request_payload": payload,
        "timestamp": datetime.now().isoformat(),
        "status": "unknown",
        "response": None,
        "error": None,
        "response_time_seconds": 0,
        "http_status_code": None
    }
    
    try:
        response = requests.post(
            ENDPOINT,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        
        result["response_time_seconds"] = round(time.time() - start_time, 2)
        result["http_status_code"] = response.status_code
        
        if response.status_code == 200:
            result["status"] = "success"
            result["response"] = response.json()
            print(f"✅ Success - Response time: {result['response_time_seconds']}s")
        else:
            result["status"] = "error"
            try:
                result["error"] = response.json()
            except:
                result["error"] = response.text
            print(f"❌ Error (HTTP {response.status_code})")
            if result["error"]:
                print(f"   Error details: {json.dumps(result['error'], indent=2)[:200]}")
                
    except requests.exceptions.Timeout:
        result["status"] = "timeout"
        result["error"] = "Request timed out after 60 seconds"
        result["response_time_seconds"] = 60
        print(f"⏱️  Timeout after 60 seconds")
        
    except Exception as e:
        result["status"] = "exception"
        result["error"] = str(e)
        result["response_time_seconds"] = round(time.time() - start_time, 2)
        print(f"❌ Exception: {str(e)}")
    
    return result


def generate_report(results: List[Dict[str, Any]]) -> str:
    """Generate a comprehensive test report."""
    report_lines = []
    report_lines.append("="*80)
    report_lines.append("RIVERGEN API TEST EXECUTION REPORT")
    report_lines.append("="*80)
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")
    
    # Summary Statistics
    total_tests = len(results)
    successful = sum(1 for r in results if r["status"] == "success")
    failed = total_tests - successful
    avg_response_time = sum(r["response_time_seconds"] for r in results) / total_tests if results else 0
    
    report_lines.append("EXECUTION SUMMARY")
    report_lines.append("-"*80)
    report_lines.append(f"Total Tests: {total_tests}")
    report_lines.append(f"Successful: {successful} ({successful/total_tests*100:.1f}%)")
    report_lines.append(f"Failed: {failed} ({failed/total_tests*100:.1f}%)")
    report_lines.append(f"Average Response Time: {avg_response_time:.2f} seconds")
    report_lines.append("")
    
    # Detailed Results
    report_lines.append("DETAILED TEST RESULTS")
    report_lines.append("="*80)
    
    for i, result in enumerate(results, 1):
        report_lines.append("")
        report_lines.append(f"Test {i}: {result['test_id']} - {result['test_name']}")
        report_lines.append("-"*80)
        report_lines.append(f"Description: {result['description']}")
        report_lines.append(f"Timestamp: {result['timestamp']}")
        report_lines.append(f"Status: {result['status'].upper()}")
        report_lines.append(f"HTTP Status Code: {result['http_status_code']}")
        report_lines.append(f"Response Time: {result['response_time_seconds']} seconds")
        report_lines.append(f"User Prompt: {result['request_payload']['user_prompt']}")
        report_lines.append(f"Model Used: {result['request_payload']['model_name']}")
        
        if result["status"] == "success" and result["response"]:
            response = result["response"]
            report_lines.append("")
            report_lines.append("Response Details:")
            report_lines.append(f"  - Request ID: {response.get('request_id', 'N/A')}")
            report_lines.append(f"  - Execution ID: {response.get('execution_id', 'N/A')}")
            report_lines.append(f"  - Status: {response.get('status', 'N/A')}")
            report_lines.append(f"  - Intent Type: {response.get('intent_type', 'N/A')}")
            report_lines.append(f"  - Intent Summary: {response.get('intent_summary', 'N/A')}")
            
            if response.get("ai_metadata"):
                metadata = response["ai_metadata"]
                report_lines.append("  - AI Metadata:")
                report_lines.append(f"    * Generation Time: {metadata.get('generation_time_ms', 'N/A')} ms")
                report_lines.append(f"    * Confidence Score: {metadata.get('confidence_score', 'N/A')}")
                if metadata.get("explanation"):
                    report_lines.append(f"    * Explanation: {metadata.get('explanation')[:200]}...")
            
            if response.get("execution_plan"):
                report_lines.append("  - Execution Plan: Present")
            
            if response.get("visualization"):
                report_lines.append(f"  - Visualizations: {len(response['visualization'])} suggested")
            
            if response.get("suggestions"):
                report_lines.append(f"  - Suggestions: {len(response['suggestions'])} provided")
        
        elif result["error"]:
            report_lines.append("")
            report_lines.append("Error Details:")
            if isinstance(result["error"], dict):
                report_lines.append(json.dumps(result["error"], indent=2))
            else:
                report_lines.append(f"  {result['error']}")
        
        report_lines.append("")
    
    # Response Time Analysis
    report_lines.append("")
    report_lines.append("RESPONSE TIME ANALYSIS")
    report_lines.append("-"*80)
    for result in results:
        report_lines.append(f"{result['test_id']}: {result['response_time_seconds']:.2f}s - {result['status']}")
    report_lines.append("")
    
    # Recommendations
    report_lines.append("RECOMMENDATIONS")
    report_lines.append("-"*80)
    if failed > 0:
        report_lines.append("⚠️  Some tests failed. Review error details above.")
    if avg_response_time > 10:
        report_lines.append("⚠️  Average response time is high. Consider optimization.")
    if successful == total_tests:
        report_lines.append("✅ All tests passed successfully!")
    report_lines.append("")
    
    report_lines.append("="*80)
    report_lines.append("END OF REPORT")
    report_lines.append("="*80)
    
    return "\n".join(report_lines)


def main():
    """Main execution function."""
    print("🚀 Starting RiverGen API Test Execution")
    print(f"Target: {ENDPOINT}")
    
    # Health check
    print("\n📡 Checking API health...")
    if not check_health():
        print("❌ API is not responding. Please ensure the backend is running.")
        return
    print("✅ API is healthy and responding")
    
    # Run tests
    results = []
    for test_case in TEST_CASES:
        result = run_test(test_case)
        results.append(result)
        time.sleep(1)  # Small delay between requests
    
    # Generate report
    print("\n" + "="*80)
    print("Generating Report...")
    print("="*80)
    
    report = generate_report(results)
    
    # Save report to file
    report_filename = f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(report_filename, "w") as f:
        f.write(report)
    
    # Print report to console
    print("\n" + report)
    print(f"\n📄 Full report saved to: {report_filename}")
    
    # Save detailed JSON results
    json_filename = f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(json_filename, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"📊 Detailed JSON results saved to: {json_filename}")


if __name__ == "__main__":
    main()

