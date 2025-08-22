#!/bin/bash

echo "=== Testing Startup Coordination Fix ==="
echo "This test verifies that all components wait properly for dependencies"
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Set Oracle password
export ORACLE_TESTING_DATABASE_PASSWORD="ARmy0320-- milk"

# Kill any existing Java processes on port 8080
echo -e "${BLUE}Cleaning up any existing processes...${NC}"
lsof -ti:8080 | xargs -r kill -9 2>/dev/null
lsof -ti:8086 | xargs -r kill -9 2>/dev/null
sleep 2

# Start the server in background
echo -e "${BLUE}Starting server with startup coordination...${NC}"
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar > startup_test.log 2>&1 &
SERVER_PID=$!

echo -e "${YELLOW}Monitoring startup sequence...${NC}"
echo

# Function to check for specific log entries
check_startup_event() {
    local event_name="$1"
    local log_pattern="$2"
    
    if grep -q "$log_pattern" startup_test.log 2>/dev/null; then
        echo -e "${GREEN}✓ $event_name${NC}"
        return 0
    else
        echo -e "${RED}✗ $event_name (waiting...)${NC}"
        return 1
    fi
}

# Monitor startup sequence for up to 60 seconds
STARTUP_COMPLETE=false
for i in {1..60}; do
    echo -e "${BLUE}=== Startup Check $i/60 ===${NC}"
    
    # Check each startup phase
    check_startup_event "Logger deployed" "Logger Verticle initialized"
    check_startup_event "HostAPI deployed" "HostAPIVerticle initialized - waiting for system ready"
    check_startup_event "MCP infrastructure deployed" "Infrastructure deployed, waiting for components"
    check_startup_event "Oracle Tools Server ready" "Oracle Tools MCP Server started"
    check_startup_event "Oracle Tools Client waiting" "Server oracle-tools is ready, initializing client"
    check_startup_event "Oracle Tools Client ready" "OracleToolsClient.*client.ready"
    check_startup_event "MCP system ready" "MCP System Ready"
    check_startup_event "Orchestration ready" "Oracle Orchestration Strategy Ready"
    check_startup_event "System fully ready" "Published system.fully.ready"
    check_startup_event "HTTP server started" "Host API server successfully started"
    
    # Check if HTTP server is up
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓ HTTP server accepting requests${NC}"
        STARTUP_COMPLETE=true
        break
    else
        echo -e "${YELLOW}HTTP server not yet available${NC}"
    fi
    
    echo
    sleep 1
done

if [ "$STARTUP_COMPLETE" = true ]; then
    echo -e "${GREEN}=== STARTUP SEQUENCE COMPLETED SUCCESSFULLY ===${NC}"
    echo
    
    # Test that we can't make requests before system is ready
    echo -e "${BLUE}Testing early request rejection...${NC}"
    
    # Check logs for any race condition indicators
    if grep -q "MCP system not ready" startup_test.log; then
        echo -e "${GREEN}✓ System correctly rejected early requests${NC}"
    else
        echo -e "${YELLOW}Note: No early requests were made during startup${NC}"
    fi
    
    # Now test the actual Oracle query
    echo
    echo -e "${BLUE}Testing Oracle query: 'How many pending orders in California?'${NC}"
    
    RESPONSE=$(curl -s -X POST http://localhost:8080/host/v1/conversations \
      -H "Content-Type: application/json" \
      -d '{
        "messages": [
          {"role": "user", "content": "How many pending orders in California?"}
        ]
      }' 2>/dev/null)
    
    # Check response
    if echo "$RESPONSE" | grep -q "ORA-00933"; then
        echo -e "${RED}FAILED: Still getting ORA-00933 error${NC}"
        echo "Response: $RESPONSE"
    else
        if echo "$RESPONSE" | grep -q "error"; then
            echo -e "${YELLOW}Different error occurred:${NC}"
            echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
        else
            echo -e "${GREEN}SUCCESS: Query processed without errors!${NC}"
            echo "Response:"
            echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
        fi
    fi
    
    # Check for our SQL fixes
    echo
    echo -e "${BLUE}Checking SQL generation fixes...${NC}"
    if grep -q "Removed trailing semicolon" startup_test.log; then
        echo -e "${GREEN}✓ Semicolon removal working${NC}"
    fi
    if grep -q "Using schema_matches for SQL generation" startup_test.log; then
        echo -e "${GREEN}✓ Schema context passing working${NC}"
    fi
    if grep -q "Extracted table for sample data" startup_test.log; then
        echo -e "${GREEN}✓ Sample data extraction working${NC}"
    fi
    
else
    echo -e "${RED}=== STARTUP FAILED TO COMPLETE ===${NC}"
    echo -e "${RED}The system did not start properly within 60 seconds${NC}"
    echo
    echo -e "${BLUE}Last 50 lines of startup log:${NC}"
    tail -50 startup_test.log
fi

# Show component timing
echo
echo -e "${BLUE}=== Component Startup Timing ===${NC}"
grep -E "ready|Ready|READY|started|Started" startup_test.log | grep -v "readiness" | head -20

# Kill the server
echo
echo -e "${BLUE}Stopping server...${NC}"
kill $SERVER_PID 2>/dev/null

echo
echo -e "${BLUE}Test complete!${NC}"