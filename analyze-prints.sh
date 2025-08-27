#!/bin/bash

echo "=== Analyzing System.out.println statements ==="
echo

echo "1. CRITICAL STARTUP/SHUTDOWN (Keep as console):"
grep -n "System\.out\.println.*===" src/main/java/agents/director/Driver.java
grep -n "System\.out\.println.*Java version\|Working directory\|Data path\|Agent path" src/main/java/agents/director/Driver.java
grep -n "System\.out\.println.*READY" src/main/java/agents/director/Driver.java
echo

echo "2. WARNINGS (Keep as console):"
grep -n "System\.out\.println.*WARNING" src/main/java/ -R --include="*.java"
echo

echo "3. DEBUG MESSAGES (Convert to logs):"
grep -n "System\.out\.println.*\[DEBUG\]" src/main/java/ -R --include="*.java"
echo

echo "4. CONVERSATION/REQUEST PROCESSING (Convert to logs):"
grep -n "System\.out\.println.*\[Conversation\]" src/main/java/ -R --include="*.java" | head -10
echo

echo "5. ERROR MESSAGES (Keep as System.err):"
grep -n "System\.err\.println" src/main/java/ -R --include="*.java" | head -10
echo

echo "=== Summary ==="
echo "Total System.out.println: $(grep -r "System\.out\.println" src/main/java/ --include="*.java" | wc -l)"
echo "Total System.err.println: $(grep -r "System\.err\.println" src/main/java/ --include="*.java" | wc -l)"