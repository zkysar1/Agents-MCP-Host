#!/bin/bash

# Shared utility functions for all test scripts
# Source this file in other test scripts: source ./test-common.sh

# Configuration
export BASE_URL="http://localhost:8080"
export CONV_URL="$BASE_URL/host/v1/conversations"
export ORACLE_TESTING_DATABASE_PASSWORD="ARmy0320-- milk"
export SERVER_PID=""

# Colors for output
export GREEN='\033[0;32m'
export RED='\033[0;31m'
export YELLOW='\033[1;33m'
export BLUE='\033[0;34m'
export NC='\033[0m' # No Color

# Function to test an endpoint
test_endpoint() {
    local description="$1"
    local method="$2"
    local url="$3"
    local data="$4"
    
    echo -e "${BLUE}Test:${NC} $description"
    echo "Method: $method"
    echo "URL: $url"
    
    if [ "$method" = "GET" ]; then
        response=$(curl -s -X GET "$url")
    else
        if [ -n "$data" ]; then
            echo "Request: $data"
        fi
        response=$(curl -s -X POST "$url" \
            -H "Content-Type: application/json" \
            -d "$data")
    fi
    
    # Try to pretty print JSON, fallback to raw if not valid JSON
    echo "Response: "
    echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
    echo "---"
    echo
}

# Function to test SSE endpoint
test_sse_endpoint() {
    local description="$1"
    local data="$2"
    
    echo -e "${BLUE}SSE Test:${NC} $description"
    echo "Request: $data"
    echo "Streaming response:"
    
    curl -N -X POST "$CONV_URL" \
        -H "Content-Type: application/json" \
        -H "Accept: text/event-stream" \
        -d "$data"
    
    echo
    echo "---"
    echo
}

# Function to check if server is running
check_server() {
    if curl -s "$BASE_URL/health" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Server is running${NC}"
        return 0
    else
        echo -e "${RED}✗ Server is not running${NC}"
        return 1
    fi
}

# Function to start the server
start_server() {
    local wait_time="${1:-50}"  # Default to 50 seconds as recommended
    
    echo "Starting server..."
    java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar > server.log 2>&1 &
    SERVER_PID=$!
    
    echo "Server PID: $SERVER_PID"
    echo "Waiting ${wait_time} seconds for server to initialize..."
    sleep "$wait_time"
    
    if check_server; then
        echo -e "${GREEN}✓ Server started successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Server failed to start${NC}"
        cat server.log | tail -20
        return 1
    fi
}

# Function to stop the server
stop_server() {
    if [ -n "$SERVER_PID" ]; then
        echo "Stopping server (PID: $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
        echo -e "${GREEN}✓ Server stopped${NC}"
    else
        # Try to find and kill java process
        pkill -f "java -jar.*Agents-MCP-Host" 2>/dev/null
        echo "Server process terminated"
    fi
}

# Function to get MCP status
get_mcp_status() {
    local status=$(curl -s "$BASE_URL/host/v1/mcp/status")
    
    if [ -n "$status" ]; then
        echo "$status" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    print(f'Ready: {data.get(\"ready\")}')
    print(f'Servers: {data.get(\"servers\", {}).get(\"count\")}')
    print(f'Clients: {data.get(\"clients\", {}).get(\"count\")}')
    print(f'Tools: {data.get(\"tools\", {}).get(\"count\")}')
except:
    print('Failed to parse MCP status')
"
    else
        echo "No MCP status available"
    fi
}

# Function to list available tools
list_tools() {
    local filter="$1"
    local response=$(curl -s "$BASE_URL/host/v1/tools")
    
    if [ -n "$response" ]; then
        echo "$response" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    tools = data.get('tools', [])
    filter_str = '$filter'.lower() if '$filter' else ''
    
    if filter_str:
        tools = [t for t in tools if filter_str in t.lower()]
        print(f'Found {len(tools)} tools matching \"$filter\":')
    else:
        print(f'Found {len(tools)} total tools:')
    
    for tool in tools[:10]:
        print(f'  - {tool}')
    
    if len(tools) > 10:
        print(f'  ... and {len(tools) - 10} more')
except:
    print('Failed to list tools')
"
    else
        echo "No tools available"
    fi
}

# Function to extract response content
extract_response_content() {
    local response="$1"
    echo "$response" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    if 'choices' in data:
        content = data['choices'][0]['message']['content']
        print(content)
    else:
        print('No content in response')
except Exception as e:
    print(f'Failed to extract content: {e}')
" 2>/dev/null
}

# Function to print test header
print_header() {
    local title="$1"
    local width=60
    local padding=$(( (width - ${#title}) / 2 ))
    
    echo
    echo "$(printf '=%.0s' {1..60})"
    printf "%*s%s%*s\n" $padding "" "$title" $padding ""
    echo "$(printf '=%.0s' {1..60})"
    echo
}

# Function to print test section
print_section() {
    local title="$1"
    echo
    echo -e "${YELLOW}=== $title ===${NC}"
    echo
}

# Function to print success message
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Function to print error message
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Function to print info message
print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

# Cleanup function for trap
cleanup() {
    echo
    echo "Cleaning up..."
    stop_server
    exit
}

# Set trap for cleanup on script exit
trap cleanup EXIT INT TERM