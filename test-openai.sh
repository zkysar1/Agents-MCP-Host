#!/bin/bash
#
# Agents-MCP-Host OpenAI Integration Test Script
# ========================================
# 
# This script tests the Agents-MCP-Host's integration with OpenAI API.
# It starts the server, sends test requests, and validates responses.
#
# Usage:
#   OPENAI_API_KEY=your-key-here ./test-openai.sh
#
# Or if you have the key in Windows environment (WSL):
#   OPENAI_API_KEY=$(wslvar OPENAI_API_KEY) ./test-openai.sh
#
# Requirements:
# - Java 21+
# - Valid OpenAI API key
# - curl and python3 installed
#

if [ -z "$OPENAI_API_KEY" ]; then
    echo "Error: OPENAI_API_KEY environment variable is not set"
    echo "Usage: OPENAI_API_KEY=your-key-here ./test-openai.sh"
    exit 1
fi

echo "Starting Agents-MCP-Host with OpenAI integration..."
echo "API Key: ${OPENAI_API_KEY:0:7}...${OPENAI_API_KEY: -4}"
echo ""

# Start the server in background
OPENAI_API_KEY=$OPENAI_API_KEY ./gradlew run > server.log 2>&1 &
SERVER_PID=$!

echo "Server starting (PID: $SERVER_PID)..."
echo "Waiting for server to be ready..."
sleep 8

# Check if server is running
if ! ps -p $SERVER_PID > /dev/null; then
    echo "Error: Server failed to start. Check server.log for details"
    cat server.log
    exit 1
fi

echo ""
echo "Testing conversation API with OpenAI..."
echo "==========================================="

# Test conversation
echo "Sending test message: 'Hello! What is 2+2?'"
echo ""

RESPONSE=$(curl -s -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {
        "role": "system",
        "content": "You are a helpful assistant. Keep responses brief."
      },
      {
        "role": "user",
        "content": "Hello! What is 2+2?"
      }
    ]
  }')

echo "Response from API:"
echo "$RESPONSE" | python3 -m json.tool

echo ""
echo "==========================================="
echo ""

# Extract the assistant's message content
ASSISTANT_MSG=$(echo "$RESPONSE" | python3 -c "
import json, sys
data = json.load(sys.stdin)
if 'choices' in data and data['choices']:
    print(data['choices'][0]['message']['content'])
else:
    print('No response content')
")

echo "Assistant's response: $ASSISTANT_MSG"
echo ""

# Test error handling
echo "Testing error handling (empty messages)..."
ERROR_RESPONSE=$(curl -s -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{"messages":[]}')

echo "Error response: $ERROR_RESPONSE"
echo ""

# Clean up
echo "Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "Test complete!"
echo ""
echo "Note: Check server.log for detailed logs"