#!/bin/bash

# Test script for PSA API endpoint
# Usage: ./test_endpoint.sh

echo "Testing PSA API Endpoint: POST /api/v1/execute"
echo "=============================================="
echo ""

curl -X POST "http://localhost:8000/api/v1/execute" \
  -H "Content-Type: application/json" \
  -d @test_request.json \
  | python3 -m json.tool

echo ""
echo "=============================================="
echo "Test completed!"


