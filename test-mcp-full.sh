#!/bin/bash

# Test script for full MCP implementation with all endpoints
# Usage: ./test-mcp-full.sh

BASE_URL="http://localhost:8080"
CONV_URL="$BASE_URL/host/v1/conversations"

echo "=== Testing Full MCP Implementation ===="
echo
echo "Testing all MCP servers, clients, and endpoints"
echo "================================================"
echo

# Function to test an endpoint
test_endpoint() {
    local description="$1"
    local method="$2"
    local url="$3"
    local data="$4"
    
    echo "Test: $description"
    echo "Method: $method"
    echo "URL: $url"
    
    if [ "$method" = "GET" ]; then
        response=$(curl -s -X GET "$url")
    else
        echo "Request: $data"
        response=$(curl -s -X POST "$url" \
            -H "Content-Type: application/json" \
            -d "$data")
    fi
    
    echo "Response: $response" | python3 -m json.tool 2>/dev/null || echo "$response"
    echo "---"
    echo
}

# Test health and status endpoints
echo "=== System Health and Status ==="
test_endpoint "Health Check" "GET" "$BASE_URL/health" ""
test_endpoint "Host Status" "GET" "$BASE_URL/host/v1/status" ""

# Test MCP-specific status endpoints
echo "=== MCP Infrastructure Status ==="
test_endpoint "MCP System Status" "GET" "$BASE_URL/host/v1/mcp/status" ""
test_endpoint "List All Clients" "GET" "$BASE_URL/host/v1/clients" ""
test_endpoint "List All Tools" "GET" "$BASE_URL/host/v1/tools" ""

# Test tools from DualServerClient (Calculator + Weather)
echo "=== Testing DualServerClient Tools ==="
test_endpoint "Calculator via DualClient - Addition" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Calculate 25 plus 17"}]}'

test_endpoint "Weather via DualClient" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is the weather like at latitude 37.7749 longitude -122.4194?"}]}'

# Test tools from SingleServerClient (Database only)
echo "=== Testing SingleServerClient Tools ==="
test_endpoint "Database Query via SingleClient" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Query the database for orders table"}]}'

test_endpoint "Database Insert via SingleClient" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Insert a new user into the database"}]}'

# Test tools from FileSystemClient
echo "=== Testing FileSystemClient Tools ==="
test_endpoint "List Files via FileSystemClient" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"List files in /tmp/mcp-sandbox"}]}'

test_endpoint "Read File via FileSystemClient" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Read the file at /tmp/mcp-sandbox/sample.txt"}]}'

# Test tool routing to correct clients
echo "=== Testing Tool Routing ==="
test_endpoint "Calculator Tool (should route to DualClient)" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Multiply 8 by 9"}]}'

test_endpoint "Database Tool (should route to both clients)" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Update records in the products table"}]}'

test_endpoint "File Tool (should route to FileSystemClient)" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Write hello world to a file"}]}'

# Test sampling workflow (large result sets)
echo "=== Testing Sampling Workflow ==="
test_endpoint "Large Database Query (triggers sampling)" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Query all orders from the database with limit 50"}]}'

# Test non-tool messages
echo "=== Testing Non-Tool Messages ==="
test_endpoint "Simple Greeting" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Hello, how are you?"}]}'

test_endpoint "General Question" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is artificial intelligence?"}]}'

echo "=== Test Statistics ==="
echo
# Get final status
status=$(curl -s "$BASE_URL/host/v1/mcp/status")
servers=$(echo "$status" | python3 -c "import sys, json; data = json.load(sys.stdin); print(data['servers']['count'])" 2>/dev/null || echo "Unknown")
clients=$(echo "$status" | python3 -c "import sys, json; data = json.load(sys.stdin); print(data['clients']['count'])" 2>/dev/null || echo "Unknown")
tools=$(echo "$status" | python3 -c "import sys, json; data = json.load(sys.stdin); print(data['tools']['count'])" 2>/dev/null || echo "Unknown")

echo "MCP Infrastructure Summary:"
echo "- Servers deployed: $servers"
echo "- Clients connected: $clients"
echo "- Tools available: $tools"
echo
echo "=== Test Complete ==="
echo
echo "Note: The system has:"
echo "1. Four MCP servers (Calculator, Weather, Database, FileSystem)"
echo "2. Three client configurations:"
echo "   - DualServerClient: connects to Calculator + Weather"
echo "   - SingleServerClient: connects to Database only"
echo "   - FileSystemClient: connects to FileSystem with roots/resources"
echo "3. Automatic tool routing based on availability"
echo "4. Support for all MCP workflows (tool invocation, sampling, resources/roots)"