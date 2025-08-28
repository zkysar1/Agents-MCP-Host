# Fix for Empty Tool Arguments Error

## Problem
The system was failing with error: "Tool call failed: -32602 - Query is required" when executing strategies. The `evaluate_query_intent` tool was being called with empty arguments `{}`.

## Root Cause
In `OracleDBAnswererHost.java`, the `buildToolArguments` method had an empty case for `evaluate_query_intent`:
```java
case "evaluate_query_intent":
    // Already handled in strategy selection
    break;
```

This resulted in the tool being called with no arguments, but the tool requires a "query" parameter.

## Solution
Updated the `buildToolArguments` method to properly provide required arguments:

```java
case "evaluate_query_intent":
    // Extract the query from the most recent user message
    args.put("query", context.history.getJsonObject(context.history.size() - 1).getString("content"));
    // Include recent conversation history for context
    args.put("conversationHistory", context.getRecentHistory(5));
    break;
```

## Additional Improvements
1. Added logging at the beginning of `buildToolArguments` to track which tool's arguments are being built
2. Verified the default case handles unknown tools appropriately

## Expected Behavior
- The `evaluate_query_intent` tool will now receive the required "query" parameter
- Optional "conversationHistory" is also included for better context
- Improved logging will show: "Building tool arguments for: evaluate_query_intent"
- Tool calls should succeed instead of failing with missing parameter errors

## Files Modified
- `/mnt/c/Users/Zachary/OneDrive/Zak/SmartNPCs/MCPThink/Agents-MCP-Host/src/main/java/agents/director/hosts/OracleDBAnswererHost.java`