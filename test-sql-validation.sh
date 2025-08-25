#!/bin/bash

# Simple test script for SQL schema validation
source ./test-common.sh

echo -e "${BLUE}Testing SQL Schema Validation${NC}"
echo -e "${BLUE}================================${NC}"

# Start server if not running
start_server_if_needed

# Test 1: Valid SQL with correct schema
echo -e "\n${BLUE}Test 1: Valid SQL (should pass)${NC}"
test_result=$(curl -s -X POST http://localhost:8086/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "validate_schema_sql",
      "arguments": {
        "sql": "SELECT CUSTOMER_ID, FIRST_NAME FROM CUSTOMERS WHERE CUSTOMER_ID = 1"
      }
    },
    "id": "test1"
  }' | jq -r '.result.valid // .error')

if [[ "$test_result" == "true" ]]; then
    echo -e "${GREEN}✓ Valid SQL passed validation${NC}"
else
    echo -e "${RED}✗ Valid SQL failed validation: $test_result${NC}"
fi

# Test 2: Invalid SQL with non-existent table
echo -e "\n${BLUE}Test 2: SQL with non-existent table (should fail)${NC}"
test_result=$(curl -s -X POST http://localhost:8086/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "validate_schema_sql",
      "arguments": {
        "sql": "SELECT * FROM COUNTRIES WHERE COUNTRY_CODE = \"US\""
      }
    },
    "id": "test2"
  }' | jq -r '.result.valid // .error')

if [[ "$test_result" == "false" ]]; then
    echo -e "${GREEN}✓ Invalid table correctly detected${NC}"
else
    echo -e "${RED}✗ Invalid table not detected: $test_result${NC}"
fi

# Test 3: Invalid SQL with wrong column name
echo -e "\n${BLUE}Test 3: SQL with wrong column name (should fail)${NC}"
test_result=$(curl -s -X POST http://localhost:8086/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "validate_schema_sql",
      "arguments": {
        "sql": "SELECT AMOUNT FROM ORDERS WHERE ORDER_ID = 1"
      }
    },
    "id": "test3"
  }' | jq -r '.result.valid // .error')

if [[ "$test_result" == "false" ]]; then
    echo -e "${GREEN}✓ Invalid column correctly detected${NC}"
else
    echo -e "${RED}✗ Invalid column not detected: $test_result${NC}"
fi

# Get validation details for the last test
echo -e "\n${BLUE}Validation details for last test:${NC}"
curl -s -X POST http://localhost:8086/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "validate_schema_sql",
      "arguments": {
        "sql": "SELECT AMOUNT FROM ORDERS WHERE ORDER_ID = 1"
      }
    },
    "id": "test3-details"
  }' | jq '.result'

echo -e "\n${BLUE}Test Complete${NC}"