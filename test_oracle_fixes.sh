#!/bin/bash

echo "=== Testing Oracle Pipeline Fixes ==="

# Kill any existing processes
pkill -f "java.*Agents-MCP-Host" 2>/dev/null
sleep 2

# Start server
echo "Starting server..."
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar > server.log 2>&1 &
SERVER_PID=$!

# Wait for server to be ready
echo "Waiting for server to start..."
for i in {1..30}; do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "Server is ready!"
        break
    fi
    sleep 1
done

# Test the Oracle query
echo "Testing Oracle query..."
curl -s -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"How many pending orders are in California?"}]}' \
  > response.json 2>&1

echo "Response:"
cat response.json | python3 -m json.tool 2>/dev/null || cat response.json

# Check the server log for our fixes
echo ""
echo "=== Checking for fix indicators ==="
grep -E "sample_data is String|Ignoring error in sample_data|Empty table list|tryComplete|Skipping error value" server.log | head -10

# Check for errors
echo ""
echo "=== Checking for errors ==="
grep -E "ClassCastException|IllegalStateException|cannot be cast" server.log | head -5

# Kill server
kill $SERVER_PID 2>/dev/null

echo "Test complete"
