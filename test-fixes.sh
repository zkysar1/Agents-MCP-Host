#!/bin/bash

# Test script for the two fixes made

echo "========================================="
echo "Testing Fixes for Oracle Query and SSE"
echo "========================================="

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}1. Verifying SQL generation prompt update...${NC}"
if grep -q "California cities include: San Francisco" src/main/java/AgentsMCPHost/mcp/servers/OracleToolsServerVerticle.java; then
    echo -e "${GREEN}[OK] SQL prompt updated with state-to-city mapping${NC}"
else
    echo -e "${RED}[ERROR] SQL prompt not updated${NC}"
fi

echo -e "${BLUE}2. Verifying Streamlit progress event handler...${NC}"
if grep -q "elif event.event == 'progress':" ../Streamlit-Interface-MCP-Host/pages/BasicChat.py; then
    echo -e "${GREEN}[OK] Progress event handler added to Streamlit${NC}"
else
    echo -e "${RED}[ERROR] Progress event handler not added${NC}"
fi

echo -e "${BLUE}3. Verifying populate script update...${NC}"
if grep -q "shipping_city, shipping_country_id" populate-oracle-data.sh; then
    echo -e "${GREEN}[OK] Populate script updated to include shipping_city${NC}"
else
    echo -e "${RED}[ERROR] Populate script not updated${NC}"
fi

echo ""
echo "========================================="
echo "Summary of Changes:"
echo "========================================="
echo ""
echo "1. SQL Generation Prompt (OracleToolsServerVerticle.java):"
echo "   - Added mapping of states to cities (CA -> San Francisco, etc.)"
echo "   - Added handling for state abbreviations"
echo "   - Fixed to use JOIN with CUSTOMERS when no state column exists"
echo ""
echo "2. Streamlit SSE Handler (BasicChat.py):"
echo "   - Added 'progress' event handler"
echo "   - Shows step progress with elapsed time"
echo ""
echo "3. Populate Script (populate-oracle-data.sh):"
echo "   - Orders now inherit shipping_city from customer's city"
echo "   - Orders now inherit shipping_country_id from customer"
echo ""
echo "========================================="
echo "Next Steps:"
echo "========================================="
echo ""
echo "1. Rebuild the JAR (when Gradle is working):"
echo "   ./gradlew shadowJar"
echo ""
echo "2. Re-populate the database:"
echo "   ./populate-oracle-data.sh"
echo ""
echo "3. Restart the backend server:"
echo "   java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar"
echo ""
echo "4. Test the query in Streamlit:"
echo "   'How many pending orders are in California?'"
echo ""
echo "Expected SQL generation:"
echo "   SELECT COUNT(*) FROM orders o"
echo "   JOIN customers c ON o.customer_id = c.customer_id"
echo "   WHERE c.city IN ('San Francisco', 'Los Angeles', 'San Diego', 'Sacramento')"
echo "   AND o.status_id = (SELECT status_id FROM order_status_enum WHERE status_code = 'PENDING')"
echo ""