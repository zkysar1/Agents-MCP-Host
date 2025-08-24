package AgentsMCPHost.api;

import AgentsMCPHost.llm.LlmAPIService;
import AgentsMCPHost.mcp.core.orchestration.ToolSelection;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.eventbus.Message;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.UUID;
import AgentsMCPHost.conversation.InterruptManager;
import AgentsMCPHost.streaming.StreamingEventPublisher;

/**
 * Unified conversation verticle with intelligent tool selection.
 * Uses the new ToolSelection for all tool routing decisions,
 * eliminating fragmented detection logic and providing LLM-powered selection.
 */
public class Conversation extends AbstractVerticle {
  private static boolean mcpEnabled = false;
  private static int availableTools = 0;
  private static boolean systemFullyReady = false;
  
  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    // Listen for complete system ready event
    vertx.eventBus().consumer("system.fully.ready", msg -> {
      systemFullyReady = true;
      System.out.println("[ConversationVerticle] System fully ready - accepting requests");
    });
    
    // Listen for MCP system ready events
    vertx.eventBus().consumer("mcp.system.ready", msg -> {
      JsonObject status = (JsonObject) msg.body();
      mcpEnabled = true;
      availableTools = status.getInteger("tools", 0);
      System.out.println("[DEBUG] ConversationVerticle - MCP system ready with " + availableTools + " tools");
    });
    
    // Listen for aggregated tool updates
    vertx.eventBus().consumer("mcp.tools.aggregated", msg -> {
      JsonObject update = (JsonObject) msg.body();
      int newToolCount = update.getInteger("totalTools", 0);
      if (newToolCount != availableTools) {
        availableTools = newToolCount;
        System.out.println("[DEBUG] ConversationVerticle - Tool count updated: " + availableTools + " tools available");
        if (newToolCount > 0) {
          mcpEnabled = true;  // Enable MCP if we have tools
        }
      }
    });
    
    // Listen for tool registration events
    vertx.eventBus().consumer("mcp.tools.registered", msg -> {
      JsonObject update = (JsonObject) msg.body();
      int totalTools = update.getInteger("totalTools", 0);
      if (totalTools > 0) {
        mcpEnabled = true;
        availableTools = totalTools;
        System.out.println("MCP tools enabled for conversation endpoint: " + availableTools + " tools available");
      }
    });
    
    // Register streaming processor - now uses unified tool selection
    vertx.eventBus().consumer("conversation.process.streaming", msg -> {
      JsonObject request = (JsonObject) msg.body();
      String streamId = request.getString("streamId");
      JsonArray streamMessages = request.getJsonArray("messages");
      String userMsg = request.getString("userMessage");
      
      // IMMEDIATELY REPLY to prevent timeout - this is critical!
      // StreamingConversationHandler uses request() which expects a reply
      msg.reply(new JsonObject()
        .put("status", "processing")
        .put("streamId", streamId)
        .put("message", "Request acknowledged, processing asynchronously"));
      
      // Log that we're starting processing
      System.out.println("[Conversation] Streaming request acknowledged for stream: " + streamId);
      
      // Then process asynchronously - results will be delivered via event bus
      if (mcpEnabled && availableTools > 0) {
        System.out.println("[Conversation] Processing with unified tool selection (tools available: " + availableTools + ")");
        handleWithUnifiedSelection(vertx, streamId, userMsg, streamMessages);
      } else {
        System.out.println("[Conversation] Processing with standard LLM (no tools available)");
        handleStandardLLM(vertx, streamId, streamMessages);
      }
    });
    
    startPromise.complete();
  }
  
  /**
   * Configure the router with conversation endpoint
   * @param parentRouter The parent router to attach to
   */
  public static void setRouter(Router parentRouter) {
    // Conversation API endpoint - OpenAI-compatible format
    parentRouter.post("/host/v1/conversations").handler(Conversation::handleConversation);
    
    // User intervention endpoints
    parentRouter.post("/host/v1/conversations/:streamId/interrupt").handler(Conversation::handleInterrupt);
    parentRouter.post("/host/v1/conversations/:streamId/feedback").handler(Conversation::handleFeedback);
    parentRouter.get("/host/v1/conversations/:streamId/status").handler(Conversation::handleStatus);
  }
  
  /**
   * Handle conversation requests (always uses SSE streaming)
   * @param ctx The routing context
   */
  private static void handleConversation(RoutingContext ctx) {
    // Check if system is ready
    if (!systemFullyReady) {
      sendError(ctx, 503, "System is starting up. Please try again in a few seconds.");
      System.out.println("[ConversationVerticle] Request rejected - system not ready");
      return;
    }
    
    try {
      // Parse the request body
      JsonObject requestBody = ctx.body().asJsonObject();
      
      // Validate request has messages array
      if (requestBody == null || !requestBody.containsKey("messages")) {
        sendError(ctx, 400, "Request must include 'messages' array");
        return;
      }
      
      JsonArray messages = requestBody.getJsonArray("messages");
      if (messages == null || messages.isEmpty()) {
        sendError(ctx, 400, "Messages array cannot be empty");
        return;
      }
      
      // Validate we have at least one user message
      boolean hasUserMessage = false;
      for (int i = 0; i < messages.size(); i++) {
        JsonObject message = messages.getJsonObject(i);
        if (message != null && "user".equals(message.getString("role"))) {
          hasUserMessage = true;
          break;
        }
      }
      
      if (!hasUserMessage) {
        sendError(ctx, 400, "No user message found in conversation");
        return;
      }

      StreamingConversationHandler.handle(ctx, messages, ctx.vertx());
        
    } catch (Exception e) {
      sendError(ctx, 500, "Internal server error: " + e.getMessage());
    }
  }
  
  /**
   * Send an error response
   * @param ctx The routing context
   * @param statusCode HTTP status code
   * @param message Error message
   */
  private static void sendError(RoutingContext ctx, int statusCode, String message) {
    JsonObject error = new JsonObject()
      .put("error", new JsonObject()
        .put("message", message)
        .put("type", "invalid_request_error")
        .put("code", statusCode));
    
    ctx.response()
      .putHeader("Content-Type", "application/json")
      .setStatusCode(statusCode)
      .end(error.encode());
  }
  
  
  /**
   * Handle conversation with unified tool selection
   */
  private static void handleWithUnifiedSelection(Vertx vertx, String streamId, String userMessage, JsonArray messages) {
    // Create streaming event publisher
    StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
    
    // Publish tool selection phase
    publisher.publishProgress("tool_selection", "Analyzing query for tool requirements", 
      new JsonObject()
        .put("phase", "tool_selection")
        .put("userQuery", userMessage)
        .put("strategy", "unified"));
    
    // Request tool selection analysis
    JsonObject selectionRequest = new JsonObject()
      .put("query", userMessage)
      .put("history", messages)
      .put("sessionId", streamId);
    
    vertx.eventBus().<JsonObject>request("tool.selection.analyze", selectionRequest)
      .onSuccess(reply -> {
        JsonObject decision = reply.body();
        String strategy = decision.getString("strategy");
        
        // Publish tool selection result
        publisher.publishProgress("tool_selection", "Tool selection completed",
          new JsonObject()
            .put("phase", "tool_selection")
            .put("strategy", strategy)
            .put("selectedTools", decision.getJsonArray("additionalTools", new JsonArray()))
            .put("primaryTool", decision.getString("primaryTool", "none")));
        
        // Route based on unified decision - beautifully simple!
        System.out.println("[Conversation] Tool selection decision: " + strategy);
        
        switch (ToolSelection.ToolStrategy.valueOf(strategy)) {
          case SINGLE_TOOL:
            System.out.println("[Conversation] Routing to single tool: " + decision.getString("primaryTool"));
            handleSingleTool(vertx, streamId, decision, userMessage);
            break;
            
          case MULTIPLE_TOOLS:
            System.out.println("[Conversation] Executing multiple tools in sequence");
            handleMultipleTools(vertx, streamId, decision, userMessage);
            break;
            
          case ORCHESTRATION:
            String orchestrationName = decision.getString("orchestrationName");
            System.out.println("[Conversation] Delegating to orchestration: " + orchestrationName);
            handleOrchestration(vertx, streamId, orchestrationName, userMessage, messages);
            break;
            
          case STANDARD_LLM:
          default:
            System.out.println("[Conversation] No tools needed - using standard LLM");
            handleStandardLLM(vertx, streamId, messages);
            break;
        }
      })
      .onFailure(err -> {
        System.err.println("Tool selection failed: " + err.getMessage());
        // Fallback to standard LLM on selection failure
        handleStandardLLM(vertx, streamId, messages);
      });
  }
  
  /**
   * Handle single tool execution - clean and simple
   */
  private static void handleSingleTool(Vertx vertx, String streamId, JsonObject decision, String userMessage) {
    String tool = decision.getString("primaryTool");
    
    System.out.println("[Conversation] Executing single tool: " + tool);
    
    // Create streaming event publisher
    StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
    
    // Notify tool call start if streaming
    if (streamId != null) {
      vertx.eventBus().publish("conversation." + streamId + ".tool.start", 
        new JsonObject().put("tool", tool));
      
      // Publish tool execution start
      publisher.publishProgress("tool_execution", "Starting tool: " + tool,
        new JsonObject()
          .put("phase", "tool_execution")
          .put("tool", tool)
          .put("status", "starting"));
    }
    
    // Check if interrupted before proceeding
    InterruptManager interruptManager = new InterruptManager(vertx);
    if (interruptManager.isInterrupted(streamId)) {
      publisher.publishExecutionPaused("User requested interrupt");
      return;
    }
    
    // Create tool arguments based on the tool and message
    JsonObject toolArguments = createSimpleToolArguments(userMessage, tool);
    
    // Route through MCP host manager
    JsonObject routeRequest = new JsonObject()
      .put("tool", tool)
      .put("arguments", toolArguments)
      .put("userMessage", userMessage)
      .put("streamId", streamId);
    
    vertx.eventBus().request("mcp.host.route", routeRequest, ar -> {
      if (ar.succeeded()) {
        JsonObject toolResult = (JsonObject) ar.result().body();
        
        System.out.println("[Conversation] Tool " + tool + " completed successfully");
        
        // Notify tool completion if streaming
        if (streamId != null) {
          // Publish tool completion with detailed results
          StreamingEventPublisher completionPublisher = new StreamingEventPublisher(vertx, streamId);
          completionPublisher.publishProgress("tool_execution", "Tool completed: " + tool,
            new JsonObject()
              .put("phase", "tool_execution")
              .put("tool", tool)
              .put("status", "completed")
              .put("resultPreview", toolResult.encodePrettily().substring(0, Math.min(200, toolResult.encodePrettily().length()))));
          
          vertx.eventBus().publish("conversation." + streamId + ".tool.complete", 
            new JsonObject()
              .put("tool", tool)
              .put("result", toolResult));
        }
        
        // Send final response
        String response = formatMcpToolResponse(tool, toolResult);
        if (streamId != null) {
          vertx.eventBus().publish("conversation." + streamId + ".final", 
            new JsonObject().put("content", response));
        }
      } else {
        String error = "Tool execution failed: " + ar.cause().getMessage();
        System.err.println("[Conversation] " + error);
        
        // Error handling
        if (streamId != null) {
          vertx.eventBus().publish("conversation." + streamId + ".error", 
            new JsonObject()
              .put("error", error)
              .put("tool", tool)
              .put("available_tools", getAvailableToolsList(vertx)));
        }
      }
    });
  }
  
  /**
   * Handle multiple tools execution in sequence
   */
  private static void handleMultipleTools(Vertx vertx, String streamId, JsonObject decision, String userMessage) {
    JsonArray tools = decision.getJsonArray("additionalTools", new JsonArray());
    
    if (decision.getString("primaryTool") != null) {
      tools.add(0, decision.getString("primaryTool"));  // Add primary tool first
    }
    
    System.out.println("[Conversation] Executing " + tools.size() + " tools in sequence");
    
    // For now, execute tools sequentially
    // Future enhancement: parallel execution where possible
    executeToolsSequentially(vertx, streamId, tools, userMessage, 0, new JsonObject());
  }
  
  /**
   * Execute tools sequentially with result passing
   */
  private static void executeToolsSequentially(Vertx vertx, String streamId, JsonArray tools, 
                                              String userMessage, int index, JsonObject previousResults) {
    if (index >= tools.size()) {
      // All tools completed
      String finalResponse = formatMultiToolResponse(previousResults);
      if (streamId != null) {
        vertx.eventBus().publish("conversation." + streamId + ".final",
          new JsonObject().put("content", finalResponse));
      }
      return;
    }
    
    String tool = tools.getString(index);
    JsonObject arguments = createSimpleToolArguments(userMessage, tool);
    
    // Add previous results to arguments if available
    if (!previousResults.isEmpty()) {
      arguments.put("previous_results", previousResults);
    }
    
    JsonObject routeRequest = new JsonObject()
      .put("tool", tool)
      .put("arguments", arguments)
      .put("streamId", streamId);
    
    vertx.eventBus().request("mcp.host.route", routeRequest, ar -> {
      if (ar.succeeded()) {
        JsonObject result = (JsonObject) ar.result().body();
        previousResults.put(tool, result);
        
        // Continue to next tool
        executeToolsSequentially(vertx, streamId, tools, userMessage, index + 1, previousResults);
      } else {
        // Handle error but try to continue
        System.err.println("[Conversation] Tool " + tool + " failed: " + ar.cause().getMessage());
        executeToolsSequentially(vertx, streamId, tools, userMessage, index + 1, previousResults);
      }
    });
  }
  
  /**
   * Handle orchestration strategy execution
   */
  private static void handleOrchestration(Vertx vertx, String streamId, String orchestrationName,
                                         String userMessage, JsonArray messages) {
    System.out.println("[Conversation] Starting orchestration: " + orchestrationName);
    
    // Build orchestration request
    JsonObject orchestrationRequest = new JsonObject()
      .put("query", userMessage)
      .put("messages", messages)
      .put("sessionId", UUID.randomUUID().toString())
      .put("streamId", streamId);
    
    // Send to orchestration handler
    vertx.eventBus().request("orchestration." + orchestrationName, orchestrationRequest, ar -> {
      if (ar.succeeded()) {
        JsonObject result = (JsonObject) ar.result().body();
        
        System.out.println("[Conversation] Orchestration completed: " + orchestrationName);
        
        // Extract the formatted response from MCP format or direct format
        String response = extractContentFromResult(result);
        
        System.out.println("[Conversation] Extracted response: " + 
            (response.length() > 200 ? response.substring(0, 200) + "..." : response));
        
        if (streamId != null) {
          System.out.println("[Conversation] Publishing final response to stream: " + streamId);
          JsonObject finalMessage = new JsonObject().put("content", response);
          vertx.eventBus().publish("conversation." + streamId + ".final", finalMessage);
          System.out.println("[Conversation] Final response published successfully");
          
          // Log to CSV
          vertx.eventBus().publish("log",
            "Conversation final response sent for " + streamId + ",2,Conversation,Response,SSE");
        } else {
          System.out.println("[Conversation] WARNING: No streamId for response delivery!");
        }
      } else {
        String error = "Orchestration failed: " + ar.cause().getMessage();
        System.err.println("[Conversation] " + error);
        
        if (streamId != null) {
          vertx.eventBus().publish("conversation." + streamId + ".error",
            new JsonObject()
              .put("error", error)
              .put("orchestration", orchestrationName));
        }
      }
    });
  }
  
  /**
   * Create simple tool arguments (backward compatibility)
   */
  private static JsonObject createSimpleToolArguments(String message, String tool) {
    JsonObject arguments = new JsonObject();
    
    // Basic argument creation for known tools
    if (tool.contains("describe_table")) {
      // Extract table name if mentioned
      String lower = message.toLowerCase();
      if (lower.contains("orders")) arguments.put("table_name", "ORDERS");
      else if (lower.contains("customers")) arguments.put("table_name", "CUSTOMERS");
      else if (lower.contains("products")) arguments.put("table_name", "PRODUCTS");
    } else if (tool.contains("execute_query") && message.toUpperCase().contains("SELECT")) {
      // If SQL is provided directly, use it
      arguments.put("sql", message);
    }
    
    return arguments;
  }
  
  /**
   * DEPRECATED: Handle conversation with MCP tools - replaced by unified selection
   */
  private static void handleWithTools_DEPRECATED(Vertx vertx, String streamId, String userMessage, JsonArray messages) {
    // This method is deprecated - kept for reference only
    // All tool selection now goes through ToolSelection
    handleStandardLLM(vertx, streamId, messages);
  }
  
  // REMOVED: isOracleQuery - now handled by ToolSelection
  
  /**
   * Handle queries with Oracle Agent Loop for intelligent SQL generation - streaming version
   */
  private static void handleWithOracleAgent(Vertx vertx, String streamId, String userMessage, JsonArray messages) {
    // Create streaming event publisher
    StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
    
    // Notify starting Oracle Agent
    vertx.eventBus().publish("conversation." + streamId + ".tool.start", 
      new JsonObject().put("tool", "oracle_agent").put("message", "Starting Oracle intelligent query processing..."));
    
    // Publish Oracle agent start
    publisher.publishProgress("oracle_agent", "Starting Oracle intelligent query processing",
      new JsonObject()
        .put("phase", "oracle_agent")
        .put("status", "initializing")
        .put("query", userMessage));
    
    // Create request for Oracle Agent Loop with streamId
    JsonObject agentRequest = new JsonObject()
      .put("query", userMessage)
      .put("messages", messages)
      .put("sessionId", UUID.randomUUID().toString())
      .put("streamId", streamId);  // Pass streamId for progress events
    
    // Send to Oracle Agent Loop via event bus
    vertx.eventBus().request("oracle.agent.process", agentRequest)
      .onSuccess(reply -> {
        JsonObject result = (JsonObject) reply.body();
        
        // Check if agent produced a result
        if (result.getBoolean("success", false)) {
          String response = result.getString("result", "Query processed successfully");
          
          // Notify tool completion
          vertx.eventBus().publish("conversation." + streamId + ".tool.complete", 
            new JsonObject()
              .put("tool", "oracle_agent")
              .put("result", result));
          
          // Send final response
          vertx.eventBus().publish("conversation." + streamId + ".final", 
            new JsonObject().put("content", response));
        } else {
          // If agent fails, send error message
          String errorMsg = "Oracle Agent encountered an issue: " + result.getString("error", "Unknown error");
          vertx.eventBus().publish("conversation." + streamId + ".error", 
            new JsonObject().put("error", errorMsg));
        }
      })
      .onFailure(err -> {
        System.err.println("Oracle Agent Loop failed: " + err.getMessage());
        // Send error event
        vertx.eventBus().publish("conversation." + streamId + ".error", 
          new JsonObject().put("error", "Oracle Agent failed: " + err.getMessage()));
      });
  }
  
  
  /**
   * Handle standard LLM processing - unified streaming version
   */
  private static void handleStandardLLM(Vertx vertx, String streamId, JsonArray messages) {
    LlmAPIService llmService = LlmAPIService.getInstance();
    
    if (!llmService.isInitialized()) {
      // Send fallback message if not initialized
      vertx.eventBus().publish("conversation." + streamId + ".final", 
        new JsonObject().put("content", "I'm currently running without an AI backend. Please set the OPENAI_API_KEY environment variable to enable AI responses."));
      return;
    }
    
    // Create streaming event publisher
    StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
    publisher.publishProgress("llm", "Sending request to LLM",
      new JsonObject()
        .put("phase", "llm")
        .put("status", "sending"));
    
    Future<JsonObject> llmFuture = llmService.chatCompletion(messages, streamId);
    
    llmFuture.onSuccess(response -> {
      // Extract content from OpenAI response
      if (response.containsKey("choices") && response.getJsonArray("choices").size() > 0) {
        String content = response.getJsonArray("choices")
          .getJsonObject(0)
          .getJsonObject("message")
          .getString("content");
        
        vertx.eventBus().publish("conversation." + streamId + ".final", 
          new JsonObject().put("content", content));
      }
    }).onFailure(error -> {
      vertx.eventBus().publish("conversation." + streamId + ".error", 
        new JsonObject().put("error", "LLM service error: " + error.getMessage()));
    });
  }
  
  // REMOVED: needsTools - now handled by ToolSelection
  
  // REMOVED: shouldUseOracleAgent - now handled by ToolSelection
  
  
  // REMOVED: detectTool - now handled by ToolSelection
  
  // REMOVED: createToolArguments - simplified version in createSimpleToolArguments
  
  // REMOVED: extractNumbers - no longer needed
  
  // REMOVED: extractTableName - handled by ToolSelection
  
  // REMOVED: extractPath - no longer needed
  
  // REMOVED: extractContent - no longer needed
  
  // REMOVED: extractLimit - no longer needed
  
  /**
   * Extract content from various result formats (MCP, direct, etc.)
   */
  private static String extractContentFromResult(JsonObject result) {
    // Check for formatted field first (from formatResults tool)
    if (result.containsKey("formatted")) {
      return result.getString("formatted");
    }
    
    // Check for result field
    if (result.containsKey("result")) {
      Object resultObj = result.getValue("result");
      if (resultObj instanceof String) {
        return (String) resultObj;
      } else if (resultObj instanceof JsonObject) {
        return ((JsonObject) resultObj).encodePrettily();
      }
    }
    
    // Check if it's MCP format with content array
    if (result.containsKey("content") && result.getValue("content") instanceof JsonArray) {
      JsonArray content = result.getJsonArray("content");
      if (!content.isEmpty()) {
        JsonObject firstContent = content.getJsonObject(0);
        if (firstContent != null && firstContent.containsKey("text")) {
          return firstContent.getString("text");
        }
      }
    }
    
    // Check for success field with message
    if (result.containsKey("success")) {
      boolean success = result.getBoolean("success", false);
      if (!success && result.containsKey("error")) {
        return "Error: " + result.getString("error");
      }
    }
    
    // Last resort - return the entire result as JSON
    return result.encodePrettily();
  }
  
  /**
   * Format MCP tool response from the new infrastructure
   */
  private static String formatMcpToolResponse(String tool, JsonObject result) {
    // Check if this is an MCP protocol response with content array
    if (result.containsKey("content")) {
      JsonArray content = result.getJsonArray("content");
      if (content != null && !content.isEmpty()) {
        JsonObject firstContent = content.getJsonObject(0);
        if (firstContent != null && firstContent.containsKey("text")) {
          return firstContent.getString("text");
        }
      }
    }
    
    // Check if this has a direct result field
    if (result.containsKey("result")) {
      Object res = result.getValue("result");
      if (res != null) {
        return "Result: " + res.toString();
      }
    }
    
    // Fallback to legacy format
    return formatToolResponse(tool, result);
  }
  
  /**
   * Format tool response (legacy)
   */
  private static String formatToolResponse(String tool, JsonObject result) {
    StringBuilder response = new StringBuilder();
    
    switch (tool) {
      case "calculate":
        response.append("Calculation result: ")
               .append(result.getDouble("result"));
        break;
      case "weather":
        response.append("Current weather: ")
               .append(result.getInteger("temperature"))
               .append("°F, ")
               .append(result.getString("conditions"));
        break;
      case "database":
        JsonArray results = result.getJsonArray("results");
        response.append("Database query returned ")
               .append(results.size())
               .append(" records");
        break;
      case "filesystem":
        response.append("File operation completed: ")
               .append(result.getString("message"));
        break;
    }
    
    return response.toString();
  }
  
  
  /**
   * Extract last user message from conversation
   */
  private static String extractLastUserMessage(JsonArray messages) {
    for (int i = messages.size() - 1; i >= 0; i--) {
      JsonObject message = messages.getJsonObject(i);
      if ("user".equals(message.getString("role"))) {
        return message.getString("content");
      }
    }
    return null;
  }
  
  // REMOVED: generateSqlFromQuery - now handled by Oracle Agent Loop
  
  /**
   * Format response from multiple tools
   */
  private static String formatMultiToolResponse(JsonObject results) {
    StringBuilder response = new StringBuilder();
    response.append("Executed ").append(results.size()).append(" tools:\n\n");
    
    results.forEach(entry -> {
      response.append("**").append(entry.getKey()).append("**:\n");
      Object value = entry.getValue();
      if (value instanceof JsonObject) {
        JsonObject toolResult = (JsonObject) value;
        response.append(formatMcpToolResponse(entry.getKey(), toolResult));
      } else {
        response.append(value.toString());
      }
      response.append("\n\n");
    });
    
    return response.toString();
  }
  
  /**
   * Get list of available tools for error messages
   */
  private static JsonArray getAvailableToolsList(Vertx vertx) {
    JsonArray tools = new JsonArray();
    
    // Request tool list from MCP host synchronously for error message
    // In production, this should be cached
    vertx.eventBus().<JsonObject>request("mcp.host.tools", new JsonObject(), ar -> {
      if (ar.succeeded()) {
        JsonObject response = ar.result().body();
        JsonArray toolsList = response.getJsonArray("tools", new JsonArray());
        toolsList.forEach(tool -> {
          if (tool instanceof JsonObject) {
            tools.add(((JsonObject) tool).getString("name"));
          }
        });
      }
    });
    
    // Return what we have (may be empty on first call)
    return tools;
  }
  
  /**
   * Handle interrupt request
   * @param ctx The routing context
   */
  private static void handleInterrupt(RoutingContext ctx) {
    String streamId = ctx.pathParam("streamId");
    JsonObject body = ctx.body().asJsonObject();
    String reason = body != null ? body.getString("reason", "user_requested") : "user_requested";
    
    // Create interrupt manager instance
    InterruptManager interruptManager = new InterruptManager(ctx.vertx());
    interruptManager.interrupt(streamId, reason);
    
    // Publish interrupt event
    ctx.vertx().eventBus().publish("conversation." + streamId + ".interrupt", 
      new JsonObject()
        .put("streamId", streamId)
        .put("reason", reason)
        .put("timestamp", System.currentTimeMillis()));
    
    // Send response
    ctx.response()
      .putHeader("Content-Type", "application/json")
      .end(new JsonObject()
        .put("success", true)
        .put("streamId", streamId)
        .put("message", "Interrupt request received")
        .encode());
  }
  
  /**
   * Handle user feedback
   * @param ctx The routing context
   */
  private static void handleFeedback(RoutingContext ctx) {
    String streamId = ctx.pathParam("streamId");
    JsonObject feedback = ctx.body().asJsonObject();
    
    if (feedback == null || !feedback.containsKey("action")) {
      sendError(ctx, 400, "Feedback must include 'action' field");
      return;
    }
    
    // Create interrupt manager instance
    InterruptManager interruptManager = new InterruptManager(ctx.vertx());
    
    // Store feedback
    interruptManager.storeFeedback(streamId, feedback);
    
    // Update state based on action
    String action = feedback.getString("action");
    switch (action) {
      case "continue":
        interruptManager.clearInterrupt(streamId);
        interruptManager.setState(streamId, InterruptManager.ConversationState.EXECUTING);
        break;
      case "modify":
        interruptManager.setState(streamId, InterruptManager.ConversationState.PLANNING);
        break;
      case "cancel":
        interruptManager.setState(streamId, InterruptManager.ConversationState.CANCELLED);
        break;
    }
    
    // Publish feedback event
    ctx.vertx().eventBus().publish("conversation." + streamId + ".feedback", feedback);
    
    // Send response
    ctx.response()
      .putHeader("Content-Type", "application/json")
      .end(new JsonObject()
        .put("success", true)
        .put("streamId", streamId)
        .put("action", action)
        .encode());
  }
  
  /**
   * Get conversation status
   * @param ctx The routing context
   */
  private static void handleStatus(RoutingContext ctx) {
    String streamId = ctx.pathParam("streamId");
    
    // Create interrupt manager instance
    InterruptManager interruptManager = new InterruptManager(ctx.vertx());
    
    InterruptManager.ConversationState state = interruptManager.getState(streamId);
    boolean interrupted = interruptManager.isInterrupted(streamId);
    
    // Send status response
    ctx.response()
      .putHeader("Content-Type", "application/json")
      .end(new JsonObject()
        .put("streamId", streamId)
        .put("state", state.name())
        .put("interrupted", interrupted)
        .encode());
  }
  
}