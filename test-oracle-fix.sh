#!/bin/bash

echo "=== Testing Oracle Query Fix ==="
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Set Oracle password
export ORACLE_TESTING_DATABASE_PASSWORD="ARmy0320-- milk"

# Kill any existing Java processes on port 8080
echo -e "${BLUE}Cleaning up any existing processes...${NC}"
lsof -ti:8080 | xargs -r kill -9 2>/dev/null
lsof -ti:8086 | xargs -r kill -9 2>/dev/null
sleep 2

# Start the server in background
echo -e "${BLUE}Starting server...${NC}"
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar > server_test.log 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo -e "${BLUE}Waiting for server to start...${NC}"
for i in {1..30}; do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo -e "${GREEN}Server is ready!${NC}"
        break
    fi
    echo -n "."
    sleep 1
done
echo

# Wait a bit more for all services to initialize
sleep 5

# Test the problematic query
echo -e "${BLUE}Testing query: 'How many pending orders in California?'${NC}"
echo

RESPONSE=$(curl -s -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"role": "user", "content": "How many pending orders in California?"}
    ]
  }' 2>/dev/null)

# Check if response contains error
if echo "$RESPONSE" | grep -q "ORA-00933"; then
    echo -e "${RED}FAILED: Still getting ORA-00933 error${NC}"
    echo "Response: $RESPONSE"
else
    if echo "$RESPONSE" | grep -q "error"; then
        echo -e "${RED}Different error occurred:${NC}"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    else
        echo -e "${GREEN}SUCCESS: Query executed without ORA-00933!${NC}"
        echo "Response:"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    fi
fi

echo
echo -e "${BLUE}Checking server logs for our fixes...${NC}"
echo

# Check if our logging is working
if grep -q "Removed trailing semicolon" server_test.log; then
    echo -e "${GREEN}✓ Semicolon removal is working${NC}"
fi

if grep -q "Using schema_matches for SQL generation" server_test.log; then
    echo -e "${GREEN}✓ Schema context passing is working${NC}"
fi

if grep -q "Extracted table for sample data" server_test.log; then
    echo -e "${GREEN}✓ Sample data extraction is working${NC}"
fi

if grep -q "generateSql inputs:" server_test.log; then
    echo -e "${GREEN}✓ SQL generation logging is working${NC}"
fi

echo
echo -e "${BLUE}Last 20 lines of server log:${NC}"
tail -20 server_test.log | grep -E "\[OracleTools\]|\[Orchestration\]|ORA-"

# Kill the server
echo
echo -e "${BLUE}Stopping server...${NC}"
kill $SERVER_PID 2>/dev/null

echo
echo -e "${BLUE}Test complete!${NC}"