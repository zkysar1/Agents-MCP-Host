#!/bin/bash

# Test script for the simplified 6-milestone system
# This script tests different milestone targets to ensure the system works correctly

echo "========================================"
echo "Testing Simplified 6-Milestone System"
echo "========================================"
echo ""

# Build the project first
echo "Building the project..."
./gradlew clean fatJar
if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

# Start the server in background
echo "Starting server..."
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar > server.log 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for server to start (20 seconds)..."
sleep 20

# Check if server is running
echo "Checking server health..."
curl -s http://localhost:8080/host/v1/health
echo ""
echo ""

# Function to test a query
test_query() {
    local BACKSTORY="$1"
    local GUIDANCE="$2"
    local QUERY="$3"
    local TEST_NAME="$4"
    
    echo "========================================"
    echo "Test: $TEST_NAME"
    echo "========================================"
    echo "Backstory: $BACKSTORY"
    echo "Guidance: $GUIDANCE"
    echo "Query: $QUERY"
    echo ""
    echo "Response:"
    
    curl -s -N -X POST http://localhost:8080/host/v1/conversations \
        -H "Accept: text/event-stream" \
        -H "Content-Type: application/json" \
        -d "{
            \"messages\": [
                {\"role\": \"user\", \"content\": \"$QUERY\"}
            ],
            \"options\": {
                \"backstory\": \"$BACKSTORY\",
                \"guidance\": \"$GUIDANCE\"
            }
        }" | grep -E "event:|data:" | head -20
    
    echo ""
    echo ""
    sleep 2
}

# Test 1: Intent only (should stop at milestone 1)
test_query \
    "You are an intent analyzer" \
    "Only extract and share the user's intent" \
    "How many orders were placed last month?" \
    "Intent Extraction Only"

# Test 2: Schema exploration (should stop at milestone 2)
test_query \
    "You are a schema explorer" \
    "Show me what tables are available" \
    "What tables contain order information?" \
    "Schema Exploration"

# Test 3: SQL generation (should stop at milestone 4)
test_query \
    "You are a SQL generator" \
    "Generate SQL only, don't execute" \
    "Show me the SQL to count orders from last month" \
    "SQL Generation Only"

# Test 4: Full execution (should go to milestone 6)
test_query \
    "You are a business analyst assistant" \
    "Provide clear data insights with natural language response" \
    "How many orders were placed last month?" \
    "Full Natural Language Response"

# Test 5: Raw data (should stop at milestone 5)
test_query \
    "You are a data retrieval system" \
    "Return raw data only, no natural language" \
    "Get all orders from last month" \
    "Raw Data Retrieval"

# Cleanup
echo "========================================"
echo "Cleaning up..."
echo "========================================"
kill $SERVER_PID 2>/dev/null
echo "Server stopped"
echo ""
echo "Test completed!"
echo "Check server.log for detailed logs"