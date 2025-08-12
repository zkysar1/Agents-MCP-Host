#!/bin/bash

# Test script for unified conversation endpoint with automatic MCP tool detection
# Usage: ./test-mcp-endpoints.sh

BASE_URL="http://localhost:8080"
CONV_URL="$BASE_URL/host/v1/conversations"

echo "=== Testing Unified Conversation Endpoint with Auto Tool Detection ==="
echo

# Function to test an endpoint
test_endpoint() {
    local description="$1"
    local url="$2"
    local data="$3"
    
    echo "Test: $description"
    echo "Request: $data"
    
    response=$(curl -s -X POST "$url" \
        -H "Content-Type: application/json" \
        -d "$data")
    
    echo "Response: $response" | python3 -m json.tool
    echo "---"
    echo
}

# Test health check first
echo "Health Check:"
curl -s "$BASE_URL/health" | python3 -m json.tool
echo "---"
echo

# Test Calculator Tool (auto-detected)
test_endpoint "Calculator Tool - Addition (auto-detected)" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Can you calculate 10 plus 20?"}]}'

test_endpoint "Calculator Tool - Multiplication (auto-detected)" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is 5 multiplied by 7?"}]}'

# Test Weather Tool (auto-detected)
test_endpoint "Weather Tool (auto-detected)" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is the weather like today?"}]}'

test_endpoint "Weather Tool - Temperature (auto-detected)" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is the current temperature?"}]}'

# Test Database Tool (auto-detected)
test_endpoint "Database Tool - Query Users (auto-detected)" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Query the database for users"}]}'

test_endpoint "Database Tool - Alternative (auto-detected)" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Show me all users in the database"}]}'

# Test File System Tool (auto-detected)
test_endpoint "File System Tool - List Files (auto-detected)" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"List files in the current directory"}]}'

test_endpoint "File System Tool - Save Operation (auto-detected)" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Save this to a file"}]}'

# Test Non-Tool Messages (should use standard LLM if OPENAI_API_KEY is set)
test_endpoint "Non-Tool Message - Joke (LLM or fallback)" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Tell me a joke"}]}'

test_endpoint "Non-Tool Message - General Question (LLM or fallback)" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is the meaning of life?"}]}'

# Test messages that don't trigger tools
test_endpoint "Simple Greeting (no tools)" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Hello"}]}'

test_endpoint "Math question without tool keywords" \
    "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is ten and twenty?"}]}'

echo "=== Test Complete ==="
echo ""
echo "Note: The unified endpoint automatically detects when to use MCP tools."
echo "Messages with keywords like 'calculate', 'weather', 'database', or 'file' will trigger tools."
echo "Other messages will use the LLM if OPENAI_API_KEY is set, or return a fallback response."