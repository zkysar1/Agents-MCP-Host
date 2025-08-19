#!/bin/bash

# Comprehensive MCP tool testing
# Tests all MCP servers, clients, and tool invocations

# Source common utilities
source ./test-common.sh

print_header "MCP Tool Testing Suite"

# Check if server is already running, start if not
if ! check_server; then
    print_info "Server not running, starting it..."
    if ! start_server; then
        print_error "Failed to start server"
        exit 1
    fi
    STARTED_SERVER=true
fi

print_section "System Health and Status"

test_endpoint "Health Check" "GET" "$BASE_URL/health" ""
test_endpoint "Host Status" "GET" "$BASE_URL/host/v1/status" ""

print_section "MCP Infrastructure Status"

test_endpoint "MCP System Status" "GET" "$BASE_URL/host/v1/mcp/status" ""
test_endpoint "List All Clients" "GET" "$BASE_URL/host/v1/clients" ""
test_endpoint "List All Tools" "GET" "$BASE_URL/host/v1/tools" ""

echo "MCP Status Summary:"
get_mcp_status
echo

print_section "Calculator Tool Tests"

test_endpoint "Addition (25 + 17)" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Calculate 25 plus 17"}]}'

test_endpoint "Multiplication (8 × 9)" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Multiply 8 by 9"}]}'

test_endpoint "Complex calculation" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is (100 + 50) divided by 3?"}]}'

print_section "Weather Tool Tests"

test_endpoint "Weather by coordinates" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is the weather like at latitude 37.7749 longitude -122.4194?"}]}'

test_endpoint "General weather query" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is the weather today?"}]}'

test_endpoint "Temperature query" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is the current temperature?"}]}'

print_section "Database Tool Tests"

test_endpoint "Query users table" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Query the database for users"}]}'

test_endpoint "Query orders table" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Show me all orders from the database"}]}'

test_endpoint "Database insert" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Insert a new user into the database"}]}'

test_endpoint "Large query (triggers sampling)" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Query all orders from the database with limit 50"}]}'

print_section "FileSystem Tool Tests"

test_endpoint "List files in /tmp" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"List files in /tmp/mcp-sandbox"}]}'

test_endpoint "Read file" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Read the file at /tmp/mcp-sandbox/sample.txt"}]}'

test_endpoint "Write to file" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Write hello world to a file"}]}'

test_endpoint "Save operation" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Save this data to a file"}]}'

print_section "Tool Routing Tests"

print_info "Testing that tools route to correct clients..."

test_endpoint "Calculator routing (should use DualClient)" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Calculate 10 plus 20"}]}'

test_endpoint "Database routing (available to multiple clients)" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Update records in the products table"}]}'

test_endpoint "File routing (should use FileSystemClient)" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"List files in the current directory"}]}'

print_section "Non-Tool Messages"

print_info "Testing messages that should NOT trigger tools..."

test_endpoint "Simple greeting" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Hello, how are you?"}]}'

test_endpoint "General question" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is artificial intelligence?"}]}'

test_endpoint "Joke request" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Tell me a joke"}]}'

print_section "Edge Cases"

test_endpoint "Ambiguous math (no tool keyword)" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"What is ten and twenty?"}]}'

test_endpoint "Empty message" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":""}]}'

test_endpoint "Multiple tool keywords" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Calculate the weather in the database file"}]}'

print_section "Test Summary"

# Get final statistics
echo
print_header "Final Statistics"

status=$(curl -s "$BASE_URL/host/v1/mcp/status")
servers=$(echo "$status" | python3 -c "import sys, json; print(json.load(sys.stdin)['servers']['count'])" 2>/dev/null || echo "0")
clients=$(echo "$status" | python3 -c "import sys, json; print(json.load(sys.stdin)['clients']['count'])" 2>/dev/null || echo "0")
tools=$(echo "$status" | python3 -c "import sys, json; print(json.load(sys.stdin)['tools']['count'])" 2>/dev/null || echo "0")

print_success "MCP Infrastructure Summary:"
echo "  - Servers deployed: $servers"
echo "  - Clients connected: $clients"
echo "  - Tools available: $tools"
echo

print_info "System capabilities:"
echo "  • Four MCP servers: Calculator, Weather, Database, FileSystem"
echo "  • Three client configurations:"
echo "    - DualServerClient: Calculator + Weather"
echo "    - SingleServerClient: Database only"
echo "    - FileSystemClient: FileSystem with roots/resources"
echo "  • Automatic tool routing based on availability"
echo "  • Support for all MCP workflows"

# Clean up if we started the server
if [ "$STARTED_SERVER" = true ]; then
    echo
    print_info "Stopping server (started by this test)..."
    stop_server
fi

print_success "MCP test suite completed!"