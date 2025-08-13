#!/bin/bash
#
# Test SSE streaming endpoint
# Usage: ./test-sse.sh

echo "Testing SSE streaming for conversation API..."
echo "================================================"

# Test SSE streaming with a tool call
echo ""
echo "Test 1: SSE stream with calculator tool"
echo "----------------------------------------"
curl -N -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "messages": [
      {
        "role": "user",
        "content": "Calculate 42 plus 58"
      }
    ]
  }'

echo ""
echo ""
echo "Test 2: SSE stream with regular conversation"
echo "--------------------------------------------"
curl -N -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "messages": [
      {
        "role": "user",
        "content": "Hello, how are you?"
      }
    ]
  }'

echo ""
echo ""
echo "Test 3: Non-streaming (regular JSON response)"
echo "----------------------------------------------"
curl -s -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {
        "role": "user",
        "content": "Calculate 10 plus 20"
      }
    ]
  }' | python3 -m json.tool

echo ""
echo "Tests complete!"