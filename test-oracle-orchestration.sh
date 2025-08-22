#!/bin/bash

# Test Oracle orchestration pipeline

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting Oracle Orchestration Test...${NC}"

# Start server in background
echo -e "${BLUE}Starting server...${NC}"
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar > server.log 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo -e "${BLUE}Waiting for server to start...${NC}"
sleep 10

# Check if server is running
if ! curl -s http://localhost:8080/health > /dev/null; then
    echo -e "${RED}Server failed to start${NC}"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

echo -e "${GREEN}Server started successfully${NC}"

# Test the orchestration pipeline
echo -e "${BLUE}Testing Oracle query orchestration...${NC}"
RESPONSE=$(curl -s -X POST http://localhost:8080/host/v1/conversations \
    -H "Content-Type: application/json" \
    -d '{
        "messages": [
            {"role": "user", "content": "How many pending orders in California?"}
        ]
    }')

echo -e "${BLUE}Response:${NC}"
echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"

# Check server logs for errors
echo -e "${BLUE}Checking server logs for errors...${NC}"
if grep -q "SEVERE\|ERROR\|Exception" server.log; then
    echo -e "${RED}Errors found in server log:${NC}"
    grep "SEVERE\|ERROR\|Exception" server.log | head -20
else
    echo -e "${GREEN}No errors in server log${NC}"
fi

# Clean up
echo -e "${BLUE}Stopping server...${NC}"
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo -e "${GREEN}Test complete!${NC}"