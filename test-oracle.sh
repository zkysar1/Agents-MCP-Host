#!/bin/bash

# Comprehensive Oracle database testing
# Tests Oracle connection, metadata, queries, and MCP integration

# Source common utilities
source ./test-common.sh

print_header "Oracle Database Testing Suite"

# Oracle configuration is in test-common.sh
print_info "Oracle Password: ${ORACLE_TESTING_DATABASE_PASSWORD:0:5}..."

# Check if server is already running, start if not
if ! check_server; then
    print_info "Server not running, starting it..."
    if ! start_server 50; then  # Use 50 seconds as recommended
        print_error "Failed to start server"
        exit 1
    fi
    STARTED_SERVER=true
fi

print_section "Oracle Connection Test"

# Test basic connection
print_info "Testing Oracle database connection..."
response=$(curl -s -X POST "$CONV_URL" \
    -H "Content-Type: application/json" \
    -d '{"messages":[{"role":"user","content":"Test Oracle database connection"}]}')

if echo "$response" | grep -q "Oracle" || echo "$response" | grep -q "database"; then
    print_success "Oracle connection appears to be working"
else
    print_error "Oracle connection may not be configured"
fi

print_section "Oracle Tool Availability"

print_info "Checking for Oracle-specific tools..."
list_tools "oracle"

print_section "Oracle Metadata Tools"

test_endpoint "List all Oracle tables" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"List all tables in the Oracle database"}]}'

test_endpoint "Describe CUSTOMERS table" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Describe the CUSTOMERS table"}]}'

test_endpoint "Describe ORDERS table" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Describe the ORDERS table structure"}]}'

test_endpoint "Show relationships for ORDERS" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Show relationships for ORDERS table"}]}'

test_endpoint "Detect enumeration tables" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Detect enumeration tables in the database"}]}'

test_endpoint "Get table statistics" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Get statistics for PRODUCTS table"}]}'

print_section "Oracle Query Execution"

test_endpoint "Count tables" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Execute query: SELECT COUNT(*) FROM user_tables"}]}'

test_endpoint "Count customers" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"How many customers are in the database?"}]}'

test_endpoint "List order statuses" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Show all order status values"}]}'

test_endpoint "Find pending orders" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Show pending orders"}]}'

test_endpoint "California orders" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Show orders from California"}]}'

print_section "Business Queries"

test_endpoint "Pending orders from California" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Show pending orders from California"}]}'

test_endpoint "Low stock products" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Which products are low in stock?"}]}'

test_endpoint "High value customers" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Show customers with credit limit over 50000"}]}'

test_endpoint "Recent orders" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Show orders from the last 30 days"}]}'

test_endpoint "Order summary by status" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Summarize orders by status"}]}'

print_section "Direct Oracle Server Tests"

if [ "$STARTED_SERVER" = true ]; then
    print_info "Testing direct Oracle Metadata Server (port 8081)..."
    
    # Initialize MCP session
    curl -s -X POST http://localhost:8081/ \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":"1","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{}}}' \
        > /dev/null 2>&1
    
    # Test list_tables directly
    response=$(curl -s -X POST http://localhost:8081/ \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"list_tables","arguments":{}},"id":"2"}')
    
    if echo "$response" | grep -q "CUSTOMERS\|ORDERS\|PRODUCTS"; then
        print_success "Direct metadata server working"
    else
        print_error "Direct metadata server not responding correctly"
    fi
    
    print_info "Testing direct Oracle Query Server (port 8085)..."
    
    # Initialize session
    curl -s -X POST http://localhost:8085/ \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05"},"id":"1"}' \
        > /dev/null 2>&1
    
    # Execute a simple query
    response=$(curl -s -X POST http://localhost:8085/ \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"execute_query","arguments":{"sql":"SELECT COUNT(*) as total FROM CUSTOMERS"}},"id":"2"}')
    
    if echo "$response" | grep -q "total\|result"; then
        print_success "Direct query server working"
    else
        print_error "Direct query server not responding correctly"
    fi
else
    print_info "Skipping direct server tests (server was already running)"
fi

print_section "Oracle Discovery Tests"

print_info "Testing interactive discovery features..."

test_endpoint "Ambiguous data request" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"show all data"}]}'

test_endpoint "Business terms" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"find pending items"}]}'

test_endpoint "Entity recognition" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"list customers"}]}'

test_endpoint "Complex business query" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"show pending deliveries from California"}]}'

print_section "Oracle Error Handling"

test_endpoint "Invalid table name" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Describe the NONEXISTENT_TABLE"}]}'

test_endpoint "Invalid SQL syntax" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Execute query: SELECT * FROM WHERE"}]}'

test_endpoint "Malformed request" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Query the Oracle database with SELECT *"}]}'

print_section "Test Summary"

# Check for errors in server log if we started it
if [ "$STARTED_SERVER" = true ]; then
    print_info "Checking server log for Oracle errors..."
    ERROR_COUNT=$(grep -c "ORA-\|SQLException" server.log 2>/dev/null || echo "0")
    if [ "$ERROR_COUNT" -gt 0 ]; then
        print_error "Found $ERROR_COUNT Oracle error(s) in server log"
        echo "Recent errors:"
        grep "ORA-\|SQLException" server.log | tail -3
    else
        print_success "No Oracle errors in server log"
    fi
fi

print_header "Oracle Test Results"

print_success "Oracle Testing Complete!"
echo
print_info "Oracle capabilities tested:"
echo "  • Database connection and authentication"
echo "  • Metadata navigation (tables, columns, relationships)"
echo "  • SQL query execution"
echo "  • Natural language to SQL conversion"
echo "  • Enumeration detection and translation"
echo "  • Business query interpretation"
echo "  • Error handling and recovery"
echo
print_info "Oracle infrastructure:"
echo "  • Oracle Metadata Server (Port 8081)"
echo "  • Oracle Query Server (Port 8085)"
echo "  • Oracle Client integration via MCP"
echo "  • Connection pooling with UCP"

# Clean up if we started the server
if [ "$STARTED_SERVER" = true ]; then
    echo
    print_info "Stopping server (started by this test)..."
    stop_server
fi

print_success "Oracle test suite completed!"