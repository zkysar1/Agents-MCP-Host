package AgentsMCPHost.hostAPI;

import AgentsMCPHost.services.LlmAPIService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.UUID;

/**
 * Unified conversation verticle with automatic MCP tool detection.
 * Provides OpenAI-compatible chat completions with optional tool support.
 * Now routes through the full MCP infrastructure via McpHostManager.
 */
public class ConversationVerticle extends AbstractVerticle {
  private static boolean mcpEnabled = false;
  private static int availableTools = 0;
  
  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    // Listen for MCP system ready events
    vertx.eventBus().consumer("mcp.system.ready", msg -> {
      JsonObject status = (JsonObject) msg.body();
      mcpEnabled = true;
      availableTools = status.getInteger("tools", 0);
      System.out.println("MCP tools enabled for conversation endpoint: " + availableTools + " tools available");
    });
    
    // Listen for aggregated tool updates
    vertx.eventBus().consumer("mcp.tools.aggregated", msg -> {
      JsonObject update = (JsonObject) msg.body();
      availableTools = update.getInteger("totalTools", 0);
    });
    
    // Register streaming processor
    vertx.eventBus().consumer("conversation.process.streaming", msg -> {
      JsonObject request = (JsonObject) msg.body();
      String streamId = request.getString("streamId");
      JsonArray streamMessages = request.getJsonArray("messages");
      String userMsg = request.getString("userMessage");
      
      if (mcpEnabled && needsTools(userMsg)) {
        handleWithToolsStreaming(vertx, streamId, userMsg, streamMessages);
      } else {
        handleStandardLLMStreaming(vertx, streamId, streamMessages);
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
    parentRouter.post("/host/v1/conversations").handler(ConversationVerticle::handleConversation);
  }
  
  /**
   * Handle conversation requests (supports both regular JSON and SSE streaming)
   * @param ctx The routing context
   */
  private static void handleConversation(RoutingContext ctx) {
    try {
      // Check if client wants SSE streaming
      String acceptHeader = ctx.request().getHeader("Accept");
      boolean wantsStreaming = "text/event-stream".equals(acceptHeader);
      
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
      
      // Extract last user message for tool detection
      String lastUserMessage = extractLastUserMessage(messages);
      
      // If streaming is requested, handle with SSE
      if (wantsStreaming) {
        StreamingConversationHandler.handle(ctx, messages, ctx.vertx());
        return;
      }
      
      // Check if message needs MCP tools (non-streaming path)
      if (mcpEnabled && needsTools(lastUserMessage)) {
        handleWithTools(ctx, lastUserMessage, messages);
      } else {
        // Standard LLM processing
        LlmAPIService llmService = LlmAPIService.getInstance();
        if (llmService.isInitialized()) {
          // Fall back to hardcoded response if service not initialized
          sendHardcodedResponse(ctx);
          return;
        }
        
        // Make async call to OpenAI
        Future<JsonObject> llmFuture = llmService.chatCompletion(messages);
        
        llmFuture.onSuccess(openAiResponse -> {
          // Forward the OpenAI response directly to the client
          ctx.response()
            .putHeader("Content-Type", "application/json")
            .setStatusCode(200)
            .end(openAiResponse.encode());
        }).onFailure(error -> {
          // Handle different types of errors
          String errorMessage = error.getMessage();
          
          if (errorMessage.contains("Rate limit")) {
            sendError(ctx, 429, "Rate limit exceeded. Please try again later.");
          } else if (errorMessage.contains("timeout")) {
            sendError(ctx, 504, "Request timeout. The AI service took too long to respond.");
          } else if (errorMessage.contains("Invalid OpenAI API key")) {
            sendError(ctx, 401, "AI service authentication failed.");
          } else {
            sendError(ctx, 503, "AI service temporarily unavailable: " + errorMessage);
          }
        });
      }
        
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
   * Send a hardcoded response when LLM service is not available
   * @param ctx The routing context
   */
  private static void sendHardcodedResponse(RoutingContext ctx) {
    String conversationId = "conv-" + UUID.randomUUID().toString().substring(0, 8);
    
    JsonObject response = new JsonObject()
      .put("id", conversationId)
      .put("object", "chat.completion")
      .put("created", System.currentTimeMillis() / 1000)
      .put("model", "fallback-v1")
      .put("choices", new JsonArray()
        .add(new JsonObject()
          .put("index", 0)
          .put("message", new JsonObject()
            .put("role", "assistant")
            .put("content", "I'm currently running without an AI backend. Please set the OPENAI_API_KEY environment variable to enable AI responses."))
          .put("finish_reason", "stop")))
      .put("usage", new JsonObject()
        .put("prompt_tokens", 0)
        .put("completion_tokens", 0)
        .put("total_tokens", 0));
    
    ctx.response()
      .putHeader("Content-Type", "application/json")
      .setStatusCode(200)
      .end(response.encode());
  }
  
  /**
   * Handle conversation with MCP tools - routes through MCP host manager
   */
  private static void handleWithTools(RoutingContext ctx, String userMessage, JsonArray messages) {
    String tool = detectTool(userMessage);
    
    if (tool != null) {
      JsonObject toolArguments = createToolArguments(userMessage, tool);
      
      // Route through MCP host manager for proper tool execution
      JsonObject routeRequest = new JsonObject()
        .put("tool", tool)
        .put("arguments", toolArguments)
        .put("userMessage", userMessage);
      
      ctx.vertx().eventBus().request("mcp.host.route", routeRequest, ar -> {
        if (ar.succeeded()) {
          JsonObject toolResult = (JsonObject) ar.result().body();
          String response = formatMcpToolResponse(tool, toolResult);
          sendToolResponse(ctx, response);
        } else {
          // Fallback to standard LLM if tool fails
          System.err.println("MCP tool execution failed: " + ar.cause().getMessage());
          handleStandardLLM(ctx, messages);
        }
      });
    } else {
      handleStandardLLM(ctx, messages);
    }
  }
  
  /**
   * Handle standard LLM processing
   */
  private static void handleStandardLLM(RoutingContext ctx, JsonArray messages) {
    LlmAPIService llmService = LlmAPIService.getInstance();
    
    if (llmService.isInitialized()) {
      sendHardcodedResponse(ctx);
      return;
    }
    
    Future<JsonObject> llmFuture = llmService.chatCompletion(messages);
    
    llmFuture.onSuccess(response -> {
      ctx.response()
        .putHeader("Content-Type", "application/json")
        .setStatusCode(200)
        .end(response.encode());
    }).onFailure(error -> {
      sendError(ctx, 503, "LLM service error: " + error.getMessage());
    });
  }
  
  /**
   * Check if message needs MCP tools
   */
  private static boolean needsTools(String message) {
    if (message == null) return false;
    String lower = message.toLowerCase();
    return lower.contains("calculate") || lower.contains("add") || lower.contains("subtract") ||
           lower.contains("multiply") || lower.contains("divide") ||
           lower.contains("weather") || lower.contains("temperature") || lower.contains("forecast") ||
           lower.contains("database") || lower.contains("query") || lower.contains("users") ||
           lower.contains("file") || lower.contains("save") || lower.contains("list files");
  }
  
  /**
   * Detect which tool to use - matches MCP server tool names
   */
  private static String detectTool(String message) {
    String lower = message.toLowerCase();
    
    // Calculator tools - use prefixed names for consistency
    if (lower.contains("add") || lower.contains("plus") || lower.contains("sum")) {
      return "calculator__add";
    }
    if (lower.contains("subtract") || lower.contains("minus") || lower.contains("difference")) {
      return "calculator__subtract";
    }
    if (lower.contains("multiply") || lower.contains("times") || lower.contains("product")) {
      return "calculator__multiply";
    }
    if (lower.contains("divide") || lower.contains("quotient") || lower.contains("division")) {
      return "calculator__divide";
    }
    if (lower.contains("calculate")) {
      // Generic calculate defaults to add
      return "calculator__add";
    }
    
    // Weather tools
    if (lower.contains("forecast")) {
      return "weather__forecast";
    }
    if (lower.contains("weather") || lower.contains("temperature")) {
      return "weather__weather";
    }
    
    // Database operations - detect specific operations
    if (lower.contains("insert") && (lower.contains("database") || lower.contains("table") || lower.contains("user"))) {
      return "database__insert";
    }
    if (lower.contains("update") && (lower.contains("database") || lower.contains("table") || lower.contains("record"))) {
      return "database__update";
    }
    if (lower.contains("delete") && (lower.contains("database") || lower.contains("table") || lower.contains("record"))) {
      return "database__delete";
    }
    if (lower.contains("query") || lower.contains("select") || 
        (lower.contains("database") && !lower.contains("insert") && !lower.contains("update") && !lower.contains("delete"))) {
      return "database__query";
    }
    
    // Filesystem operations - detect specific operations
    if (lower.contains("read") && (lower.contains("file") || lower.contains(".txt") || lower.contains(".json"))) {
      return "filesystem__read";
    }
    if (lower.contains("write") && (lower.contains("file") || lower.contains("save"))) {
      return "filesystem__write";
    }
    if (lower.contains("delete") && lower.contains("file")) {
      return "filesystem__delete";
    }
    if (lower.contains("list") && (lower.contains("file") || lower.contains("directory") || lower.contains("folder"))) {
      return "filesystem__list";
    }
    // Additional filesystem detection patterns
    if (lower.contains("files in") || lower.contains("ls ") || lower.contains("dir ")) {
      return "filesystem__list";
    }
    
    return null;
  }
  
  /**
   * Create tool arguments based on message and tool type
   */
  private static JsonObject createToolArguments(String message, String tool) {
    JsonObject arguments = new JsonObject();
    
    // Handle calculator operations - extract numbers for ALL operations
    if (tool.startsWith("calculator__")) {
      double[] numbers = extractNumbers(message);
      if (numbers.length >= 2) {
        arguments.put("a", numbers[0])
                .put("b", numbers[1]);
      } else {
        // Fallback defaults if no numbers found
        arguments.put("a", 10)
                .put("b", 5);
      }
      return arguments;
    }
    
    // Handle weather operations
    if (tool.startsWith("weather__")) {
      double[] coords = extractNumbers(message);
      if (coords.length >= 2) {
        arguments.put("latitude", coords[0])
                .put("longitude", coords[1]);
      } else {
        // Default to San Francisco
        arguments.put("latitude", 37.7749)
                .put("longitude", -122.4194);
      }
      if (tool.equals("weather__forecast")) {
        // Extract days if specified
        arguments.put("days", 3); // Default 3 days
      }
      return arguments;
    }
    
    // Handle database operations
    if (tool.startsWith("database__")) {
      String tableName = extractTableName(message);
      arguments.put("table", tableName);
      
      switch (tool) {
        case "database__insert":
          arguments.put("data", new JsonObject()
            .put("name", "Sample User")
            .put("email", "user@example.com"));
          break;
        case "database__update":
          arguments.put("filter", new JsonObject().put("id", 1))
                  .put("data", new JsonObject().put("status", "active"));
          break;
        case "database__delete":
          arguments.put("filter", new JsonObject().put("id", 1));
          break;
        case "database__query":
          arguments.put("limit", extractLimit(message));
          break;
      }
      return arguments;
    }
    
    // Handle filesystem operations
    if (tool.startsWith("filesystem__")) {
      String path = extractPath(message);
      
      switch (tool) {
        case "filesystem__read":
        case "filesystem__delete":
          arguments.put("path", path);
          break;
        case "filesystem__write":
          arguments.put("path", path)
                  .put("content", extractContent(message))
                  .put("append", false);
          break;
        case "filesystem__list":
          arguments.put("path", path)
                  .put("recursive", message.toLowerCase().contains("recursive"));
          break;
      }
      return arguments;
    }
    
    return arguments;
  }
  
  /**
   * Extract numbers from a message string
   */
  private static double[] extractNumbers(String message) {
    // Use regex to find all numbers (including decimals and negatives)
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("-?\\d+\\.?\\d*");
    java.util.regex.Matcher matcher = pattern.matcher(message);
    
    java.util.List<Double> numbers = new java.util.ArrayList<>();
    while (matcher.find()) {
      try {
        numbers.add(Double.parseDouble(matcher.group()));
      } catch (NumberFormatException e) {
        // Skip invalid numbers
      }
    }
    
    return numbers.stream().mapToDouble(Double::doubleValue).toArray();
  }
  
  /**
   * Extract table name from database message
   */
  private static String extractTableName(String message) {
    String lower = message.toLowerCase();
    
    // Common table names
    if (lower.contains("orders")) return "orders";
    if (lower.contains("products")) return "products";
    if (lower.contains("customers")) return "customers";
    if (lower.contains("users")) return "users";
    if (lower.contains("employees")) return "employees";
    
    // Try to extract "table_name table" pattern
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(\\w+)\\s+table");
    java.util.regex.Matcher matcher = pattern.matcher(lower);
    if (matcher.find()) {
      return matcher.group(1);
    }
    
    // Default
    return "users";
  }
  
  /**
   * Extract file path from filesystem message
   */
  private static String extractPath(String message) {
    // Look for paths starting with /
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(/[\\w\\-./]+)");
    java.util.regex.Matcher matcher = pattern.matcher(message);
    if (matcher.find()) {
      return matcher.group(1);
    }
    
    // Look for specific directories
    if (message.contains("/tmp")) return "/tmp/mcp-sandbox";
    if (message.contains("sandbox")) return "/tmp/mcp-sandbox";
    
    // Look for file extensions
    pattern = java.util.regex.Pattern.compile("([\\w\\-]+\\.(txt|json|xml|csv|log))");
    matcher = pattern.matcher(message);
    if (matcher.find()) {
      return "/tmp/mcp-sandbox/" + matcher.group(1);
    }
    
    // Default
    return "/tmp/mcp-sandbox";
  }
  
  /**
   * Extract content for file write operations
   */
  private static String extractContent(String message) {
    // Look for quoted content
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\"([^\"]+)\"|'([^']+)'");
    java.util.regex.Matcher matcher = pattern.matcher(message);
    if (matcher.find()) {
      return matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
    }
    
    // Common content patterns
    if (message.toLowerCase().contains("hello world")) {
      return "Hello World!";
    }
    
    return "Sample content";
  }
  
  /**
   * Extract limit for database queries
   */
  private static int extractLimit(String message) {
    // Look for "limit N" pattern
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("limit\\s+(\\d+)");
    java.util.regex.Matcher matcher = pattern.matcher(message.toLowerCase());
    if (matcher.find()) {
      try {
        return Integer.parseInt(matcher.group(1));
      } catch (NumberFormatException e) {
        // Ignore
      }
    }
    
    // Look for numbers that might be limits
    double[] numbers = extractNumbers(message);
    for (double num : numbers) {
      if (num > 10 && num <= 1000 && num == (int)num) {
        return (int)num;
      }
    }
    
    // Default
    return 100;
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
   * Send tool response
   */
  private static void sendToolResponse(RoutingContext ctx, String content) {
    JsonObject response = new JsonObject()
      .put("id", "msg-" + UUID.randomUUID().toString())
      .put("object", "chat.completion")
      .put("created", System.currentTimeMillis() / 1000)
      .put("model", "mcp-enhanced")
      .put("choices", new JsonArray()
        .add(new JsonObject()
          .put("index", 0)
          .put("message", new JsonObject()
            .put("role", "assistant")
            .put("content", content))
          .put("finish_reason", "stop")));
    
    ctx.response()
      .putHeader("Content-Type", "application/json")
      .setStatusCode(200)
      .end(response.encode());
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
  
  /**
   * Handle conversation with tools - streaming version
   */
  private static void handleWithToolsStreaming(Vertx vertx, String streamId, String userMessage, JsonArray messages) {
    String tool = detectTool(userMessage);
    
    if (tool != null) {
      // Notify tool call start
      vertx.eventBus().publish("conversation." + streamId + ".tool.start", 
        new JsonObject().put("tool", tool));
      
      JsonObject toolArguments = createToolArguments(userMessage, tool);
      
      // Route through MCP host manager for proper tool execution
      JsonObject routeRequest = new JsonObject()
        .put("tool", tool)
        .put("arguments", toolArguments)
        .put("userMessage", userMessage)
        .put("streamId", streamId);
      
      vertx.eventBus().request("mcp.host.route", routeRequest, ar -> {
        if (ar.succeeded()) {
          JsonObject toolResult = (JsonObject) ar.result().body();
          
          // Notify tool completion
          vertx.eventBus().publish("conversation." + streamId + ".tool.complete", 
            new JsonObject()
              .put("tool", tool)
              .put("result", toolResult));
          
          // Send final response
          String response = formatMcpToolResponse(tool, toolResult);
          vertx.eventBus().publish("conversation." + streamId + ".final", 
            new JsonObject().put("content", response));
        } else {
          // Handle error
          vertx.eventBus().publish("conversation." + streamId + ".error", 
            new JsonObject().put("error", "Tool execution failed: " + ar.cause().getMessage()));
        }
      });
    } else {
      handleStandardLLMStreaming(vertx, streamId, messages);
    }
  }
  
  /**
   * Handle standard LLM processing - streaming version
   */
  private static void handleStandardLLMStreaming(Vertx vertx, String streamId, JsonArray messages) {
    LlmAPIService llmService = LlmAPIService.getInstance();
    
    if (llmService.isInitialized()) {
      // Send fallback message if not initialized
      vertx.eventBus().publish("conversation." + streamId + ".final", 
        new JsonObject().put("content", "I'm currently running without an AI backend. Please set the OPENAI_API_KEY environment variable to enable AI responses."));
      return;
    }
    
    Future<JsonObject> llmFuture = llmService.chatCompletion(messages);
    
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
}