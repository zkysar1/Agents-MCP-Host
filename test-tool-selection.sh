#!/bin/bash

# Test script for unified tool selection
# Tests the new ToolSelectionVerticle integration

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Testing Unified Tool Selection ===${NC}"

# Function to test a query
test_query() {
    local query="$1"
    local expected_strategy="$2"
    
    echo -e "\n${BLUE}Testing:${NC} $query"
    echo -e "${BLUE}Expected:${NC} $expected_strategy"
    
    response=$(curl -s -X POST http://localhost:8080/host/v1/conversations \
        -H "Content-Type: application/json" \
        -d "{\"messages\":[{\"role\":\"user\",\"content\":\"$query\"}]}" | head -n 100)
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Request successful${NC}"
        echo "Response preview: ${response:0:200}..."
    else
        echo -e "${RED}✗ Request failed${NC}"
    fi
}

# Check if server is running
if ! curl -s http://localhost:8080/health > /dev/null; then
    echo -e "${RED}Server is not running. Please start it first.${NC}"
    exit 1
fi

echo -e "${GREEN}Server is running${NC}"

# Test various query types

echo -e "\n${BLUE}=== Test 1: Simple Table List ===${NC}"
test_query "list all tables" "SINGLE_TOOL (oracle__list_tables)"

echo -e "\n${BLUE}=== Test 2: Complex Business Query ===${NC}"
test_query "how many pending orders from California" "ORACLE_AGENT"

echo -e "\n${BLUE}=== Test 3: Direct SQL ===${NC}"
test_query "SELECT * FROM orders WHERE status = 'PENDING'" "SINGLE_TOOL (oracle__execute_query)"

echo -e "\n${BLUE}=== Test 4: Ambiguous Query ===${NC}"
test_query "tell me about the data" "ORACLE_AGENT or STANDARD_LLM"

echo -e "\n${BLUE}=== Test 5: Non-Database Query ===${NC}"
test_query "What is the capital of France?" "STANDARD_LLM"

echo -e "\n${BLUE}=== Test 6: Table Description ===${NC}"
test_query "describe the orders table" "SINGLE_TOOL (oracle__describe_table)"

echo -e "\n${BLUE}=== Test 7: Complex Join Query ===${NC}"
test_query "show me customers who have pending orders with their order details" "ORACLE_AGENT"

echo -e "\n${BLUE}=== Test Complete ===${NC}"
echo -e "${GREEN}✓ All test queries sent successfully${NC}"
echo -e "\nNote: Check the server logs to verify that ToolSelectionVerticle is being used"
echo -e "Look for messages like:"
echo -e "  - 'Tool Selection Verticle initialized successfully'"
echo -e "  - 'tool.selection.analyze' event bus messages"
echo -e "  - Decision logs showing strategy selection"