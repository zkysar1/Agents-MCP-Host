#!/bin/bash

# Test enhanced streaming with detailed debugging information
# This script tests:
# 1. LLM request/response streaming
# 2. SQL query/result streaming
# 3. Tool routing details
# 4. Interrupt functionality

# Source common test utilities
source test-common.sh

# Test enhanced streaming
test_enhanced_streaming() {
    echo_blue "Testing enhanced streaming capabilities..."
    
    # Test 1: Simple calculation with detailed streaming
    echo_blue "Test 1: Calculation with enhanced streaming"
    response=$(curl -s -N -X POST http://localhost:8080/host/v1/conversations \
        -H "Accept: text/event-stream" \
        -H "Content-Type: application/json" \
        -d '{
            "messages": [
                {"role": "user", "content": "Calculate 42 + 58"}
            ]
        }' 2>&1)
    
    # Check for enhanced progress events
    if echo "$response" | grep -q "phase.*tool_selection"; then
        echo_green "✓ Tool selection phase detected"
    else
        echo_red "✗ Tool selection phase not found"
    fi
    
    if echo "$response" | grep -q "phase.*tool_routing"; then
        echo_green "✓ Tool routing phase detected"
    else
        echo_red "✗ Tool routing phase not found"
    fi
    
    if echo "$response" | grep -q "phase.*tool_completed"; then
        echo_green "✓ Tool completion phase detected"
    else
        echo_red "✗ Tool completion phase not found"
    fi
    
    # Test 2: Oracle query with SQL streaming
    echo_blue "\nTest 2: Oracle query with SQL streaming"
    response=$(curl -s -N -X POST http://localhost:8080/host/v1/conversations \
        -H "Accept: text-event-stream" \
        -H "Content-Type: application/json" \
        -d '{
            "messages": [
                {"role": "user", "content": "Show me all orders from the database"}
            ]
        }' 2>&1)
    
    # Check for SQL events
    if echo "$response" | grep -q "phase.*sql_query"; then
        echo_green "✓ SQL query phase detected"
    else
        echo_red "✗ SQL query phase not found"
    fi
    
    if echo "$response" | grep -q "phase.*sql_result"; then
        echo_green "✓ SQL result phase detected"
    else
        echo_red "✗ SQL result phase not found"
    fi
    
    # Test 3: LLM streaming
    echo_blue "\nTest 3: LLM request/response streaming"
    response=$(curl -s -N -X POST http://localhost:8080/host/v1/conversations \
        -H "Accept: text-event-stream" \
        -H "Content-Type: application/json" \
        -d '{
            "messages": [
                {"role": "user", "content": "What is the meaning of life?"}
            ]
        }' 2>&1)
    
    # Check for LLM events
    if echo "$response" | grep -q "phase.*llm_request"; then
        echo_green "✓ LLM request phase detected"
    else
        echo_red "✗ LLM request phase not found"
    fi
    
    if echo "$response" | grep -q "phase.*llm_response"; then
        echo_green "✓ LLM response phase detected"
    else
        echo_red "✗ LLM response phase not found"
    fi
}

# Test interrupt functionality
test_interrupt() {
    echo_blue "\nTesting interrupt functionality..."
    
    # Start a long-running query
    echo_blue "Starting long-running query..."
    
    # Capture the stream ID from the response
    stream_response=$(curl -s -N -X POST http://localhost:8080/host/v1/conversations \
        -H "Accept: text/event-stream" \
        -H "Content-Type: application/json" \
        -d '{
            "messages": [
                {"role": "user", "content": "Calculate the sum of all numbers from 1 to 1000000"}
            ]
        }' 2>&1 &)
    
    # Extract stream ID (assuming it's in the response)
    # For now, use a placeholder
    stream_id="test-stream-123"
    
    # Wait a moment then interrupt
    sleep 2
    
    echo_blue "Sending interrupt request..."
    interrupt_response=$(curl -s -X POST http://localhost:8080/host/v1/conversations/$stream_id/interrupt \
        -H "Content-Type: application/json" \
        -d '{"reason": "user_requested"}')
    
    if echo "$interrupt_response" | grep -q "success.*true"; then
        echo_green "✓ Interrupt request successful"
    else
        echo_red "✗ Interrupt request failed"
    fi
    
    # Check status
    status_response=$(curl -s http://localhost:8080/host/v1/conversations/$stream_id/status)
    if echo "$status_response" | grep -q "interrupted.*true"; then
        echo_green "✓ Conversation marked as interrupted"
    else
        echo_red "✗ Conversation not properly interrupted"
    fi
}

# Main test execution
echo_blue "Enhanced Streaming Test Suite"
echo_blue "============================="

# Ensure server is running
ensure_server_running

# Run tests
test_enhanced_streaming
test_interrupt

echo_blue "\nEnhanced streaming tests completed!"