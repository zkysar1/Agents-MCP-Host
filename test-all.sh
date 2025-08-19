#!/bin/bash

# Master test runner for Agents-MCP-Host
# Executes all test suites in sequence with summary reporting

# Source common utilities
source ./test-common.sh

# Configuration
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
FAILED_TESTS=""

print_header "Agents-MCP-Host Complete Test Suite"

# Function to run a test and track result
run_test() {
    local test_name="$1"
    local test_script="$2"
    
    TESTS_RUN=$((TESTS_RUN + 1))
    
    print_section "Running: $test_name"
    
    if [ -f "$test_script" ]; then
        if bash "$test_script"; then
            print_success "$test_name completed successfully"
            TESTS_PASSED=$((TESTS_PASSED + 1))
        else
            print_error "$test_name failed"
            TESTS_FAILED=$((TESTS_FAILED + 1))
            FAILED_TESTS="$FAILED_TESTS\n  - $test_name"
        fi
    else
        print_error "Test script not found: $test_script"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        FAILED_TESTS="$FAILED_TESTS\n  - $test_name (script not found)"
    fi
    
    echo
}

# Check prerequisites
print_section "Prerequisites Check"

# Check if JAR is built
if [ -f "build/libs/Agents-MCP-Host-1.0.0-fat.jar" ]; then
    print_success "JAR file found"
else
    print_error "JAR file not found. Building..."
    print_info "Note: Build may take up to 3 minutes..."
    timeout 180 ./gradlew shadowJar  # 3 minute timeout
    if [ $? -ne 0 ]; then
        print_error "Build failed or timed out. Please fix compilation errors and try again."
        exit 1
    fi
fi

# Check if server is already running
if check_server; then
    print_info "Server is already running. Will use existing instance."
    USE_EXISTING_SERVER=true
else
    print_info "Server not running. Will start for tests."
    USE_EXISTING_SERVER=false
fi

echo

# Run test suites
print_header "Test Execution"

# Core functionality tests
run_test "MCP Infrastructure Tests" "./test-mcp.sh"

# Oracle database tests (if password is configured)
if [ -n "$ORACLE_TESTING_DATABASE_PASSWORD" ]; then
    run_test "Oracle Database Tests" "./test-oracle.sh"
else
    print_info "Skipping Oracle tests (no password configured)"
fi

# OpenAI integration tests (if API key is available)
if [ -n "$OPENAI_API_KEY" ]; then
    run_test "OpenAI Integration Tests" "./test-openai.sh"
else
    print_info "Skipping OpenAI tests (no API key configured)"
fi

# SSE streaming tests
run_test "SSE Streaming Tests" "./test-sse.sh"

# Java unit tests
print_section "Running: Java Unit Tests"
TESTS_RUN=$((TESTS_RUN + 1))

if ./gradlew test; then
    print_success "Java unit tests passed"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    print_error "Java unit tests failed"
    TESTS_FAILED=$((TESTS_FAILED + 1))
    FAILED_TESTS="$FAILED_TESTS\n  - Java unit tests"
fi

# Test Summary
print_header "Test Results Summary"

echo "Tests Run: $TESTS_RUN"
echo "Tests Passed: $TESTS_PASSED"
echo "Tests Failed: $TESTS_FAILED"

if [ $TESTS_FAILED -gt 0 ]; then
    echo
    print_error "Failed Tests:"
    echo -e "$FAILED_TESTS"
    echo
    print_error "TEST SUITE FAILED"
    exit 1
else
    echo
    print_success "ALL TESTS PASSED!"
fi

echo
print_info "Test Coverage:"
echo "  ✓ MCP Infrastructure (servers, clients, tools)"
echo "  ✓ Oracle Database Integration (if configured)"
echo "  ✓ OpenAI Integration (if configured)"
echo "  ✓ SSE Streaming"
echo "  ✓ Java Unit Tests"
echo

# Cleanup note
if [ "$USE_EXISTING_SERVER" = false ]; then
    print_info "Note: Test servers may still be running. Use 'pkill -f Agents-MCP-Host' to stop them."
fi

print_success "Test suite completed!"