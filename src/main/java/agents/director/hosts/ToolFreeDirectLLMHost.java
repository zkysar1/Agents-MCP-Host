package agents.director.hosts;

import agents.director.services.LlmAPIService;
import agents.director.services.InterruptManager;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import static agents.director.Driver.logLevel;

/**
 * Tool-Free Direct LLM Host - Provides direct LLM responses without any tools.
 * This host is for questions that can be answered from the model's knowledge
 * without database access or other tools.
 * 
 * Key responsibilities:
 * 1. Maintain conversation context
 * 2. Call LLM directly with appropriate prompts
 * 3. Handle streaming responses
 * 4. Manage conversation memory
 * 5. Provide fallback when LLM is unavailable
 */
public class ToolFreeDirectLLMHost extends AbstractVerticle {
    
    
    
    // Service references
    private EventBus eventBus;
    private LlmAPIService llmService;
    
    // Conversation management
    private final Map<String, ConversationState> conversations = new ConcurrentHashMap<>();
    
    // Configuration
    private static final int MAX_CONVERSATION_HISTORY = 10;
    private static final long CONVERSATION_TIMEOUT = 30 * 60 * 1000; // 30 minutes
    private static final int MAX_TOKENS = 2000;
    
    // Performance tracking
    private final Map<String, Long> responseMetrics = new ConcurrentHashMap<>();
    
    // System prompt for the LLM
    private static final String SYSTEM_PROMPT = """
        You are a helpful, knowledgeable assistant. You provide direct, accurate answers 
        based on your training data. You do not have access to any external tools, 
        databases, or real-time information.
        
        Guidelines:
        1. Be concise and direct in your responses
        2. If you don't know something, say so clearly
        3. Don't make up information or pretend to access data you don't have
        4. For questions about current events or data, explain that you don't have real-time access
        5. Provide helpful context and explanations when appropriate
        6. Be friendly and professional
        
        Important: You cannot execute database queries, access files, or use any external tools.
        If asked about specific data or to perform operations, explain what you can't do and 
        suggest alternatives when possible.
        """;
    
    // Inner class for conversation state
    private static class ConversationState {
        String conversationId;
        JsonArray messages = new JsonArray();
        long lastActivity;
        Map<String, Object> contextData = new HashMap<>();
        
        ConversationState(String conversationId) {
            this.conversationId = conversationId;
            this.lastActivity = System.currentTimeMillis();
            
            // Add system prompt as first message
            addMessage("system", SYSTEM_PROMPT);
        }
        
        void addMessage(String role, String content) {
            messages.add(new JsonObject()
                .put("role", role)
                .put("content", content)
                .put("timestamp", System.currentTimeMillis()));
            
            lastActivity = System.currentTimeMillis();
            
            // Maintain conversation size limit
            if (messages.size() > MAX_CONVERSATION_HISTORY + 1) { // +1 for system prompt
                // Keep system prompt and recent messages
                JsonArray trimmed = new JsonArray();
                trimmed.add(messages.getValue(0)); // System prompt
                
                int start = messages.size() - MAX_CONVERSATION_HISTORY;
                for (int i = start; i < messages.size(); i++) {
                    trimmed.add(messages.getValue(i));
                }
                messages = trimmed;
            }
        }
        
        JsonArray getMessagesForLLM() {
            // Return all messages in the format expected by LLM
            JsonArray llmMessages = new JsonArray();
            for (int i = 0; i < messages.size(); i++) {
                JsonObject msg = messages.getJsonObject(i);
                llmMessages.add(new JsonObject()
                    .put("role", msg.getString("role"))
                    .put("content", msg.getString("content")));
            }
            return llmMessages;
        }
        
        boolean isExpired() {
            return System.currentTimeMillis() - lastActivity > CONVERSATION_TIMEOUT;
        }
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        eventBus = vertx.eventBus();
        llmService = LlmAPIService.getInstance();
        
        // Check if LLM service is available
        if (!llmService.isInitialized()) {
            String warningMsg = "LLM service not initialized - API key may be missing or invalid";
            vertx.eventBus().publish("log", warningMsg + ",1,ToolFreeDirectLLMHost,Host,System");
            // Already logged above
        }
        
        // Register event bus consumers
        registerEventBusConsumers();
        
        // Start cleanup timer
        startConversationCleanup();
        
        vertx.eventBus().publish("log", "ToolFreeDirectLLMHost started successfully,2,ToolFreeDirectLLMHost,Host,System");
        startPromise.complete();
    }
    
    private void registerEventBusConsumers() {
        // Main processing endpoint
        eventBus.<JsonObject>consumer("host.toolfreedirectllm.process", this::processQuery);
        
        // Status endpoint
        eventBus.<JsonObject>consumer("host.toolfreedirectllm.status", message -> {
            message.reply(new JsonObject()
                .put("status", llmService.isInitialized() ? "ready" : "limited")
                .put("llmAvailable", llmService.isInitialized())
                .put("activeConversations", conversations.size())
                .put("totalResponses", responseMetrics.size()));
        });
        
        // Clear conversation endpoint
        eventBus.<JsonObject>consumer("host.toolfreedirectllm.clear", message -> {
            String conversationId = message.body().getString("conversationId");
            if (conversationId != null) {
                conversations.remove(conversationId);
                message.reply(new JsonObject().put("cleared", true));
            } else {
                message.fail(400, "conversationId required");
            }
        });
    }
    
    /**
     * Process incoming query
     */
    private void processQuery(Message<JsonObject> message) {
        try {
            JsonObject request = message.body();
            String query = request.getString("query");
            String conversationId = request.getString("conversationId", UUID.randomUUID().toString());
            String sessionId = request.getString("sessionId"); // For streaming
            JsonArray history = request.getJsonArray("history", new JsonArray());
            boolean streaming = request.getBoolean("streaming", false);
            JsonObject options = request.getJsonObject("options", new JsonObject());
        
        vertx.eventBus().publish("log", "Processing direct LLM query for conversation " + conversationId + ": " + query + "" + ",2,ToolFreeDirectLLMHost,Host,System");
        
        // Publish start event if streaming
        if (sessionId != null && streaming) {
            publishStreamingEvent(conversationId, "progress", new JsonObject()
                .put("step", "host_started")
                .put("message", "Starting direct LLM processing")
                .put("details", new JsonObject()
                    .put("query", query)
                    .put("sessionId", sessionId)));
        }
        
        // Track performance
        long startTime = System.currentTimeMillis();
        
        // Get or create conversation state
        ConversationState conversation = conversations.computeIfAbsent(
            conversationId,
            k -> createConversationWithHistory(k, history)
        );
        
        // Add user message
        conversation.addMessage("user", query);
        
        // Check for interrupts if streaming
        if (sessionId != null && streaming) {
            InterruptManager im = new InterruptManager(vertx);
            if (im.isInterrupted(sessionId)) {
                publishStreamingEvent(conversationId, "interrupt", new JsonObject()
                    .put("reason", "User interrupted")
                    .put("message", "Processing interrupted by user"));
                message.reply(new JsonObject()
                    .put("interrupted", true)
                    .put("message", "Processing interrupted by user"));
                return;
            }
        }
        
        // Process with LLM or fallback
        if (llmService.isInitialized()) {
            // Publish tool start event if streaming
            if (sessionId != null && streaming) {
                publishStreamingEvent(conversationId, "tool.start", new JsonObject()
                    .put("tool", "direct_llm_chat")
                    .put("description", "Direct LLM conversation without tools")
                    .put("parameters", new JsonObject()
                        .put("model", options.getString("model", "default"))
                        .put("temperature", options.getDouble("temperature", 0.7))));
            }
            processWithLLM(conversation, query, options, streaming)
                .onComplete(ar -> {
                    long duration = System.currentTimeMillis() - startTime;
                    responseMetrics.put(conversationId + "-" + System.currentTimeMillis(), duration);
                    
                    if (ar.succeeded()) {
                        JsonObject response = ar.result();
                        response.put("conversationId", conversationId);
                        response.put("duration", duration);
                        response.put("method", "llm");
                        
                        // Publish complete event if streaming
                        if (sessionId != null && streaming) {
                            publishStreamingEvent(conversationId, "tool.complete", new JsonObject()
                                .put("tool", "direct_llm_chat")
                                .put("success", true)
                                .put("resultSummary", "Generated response of " + 
                                    response.getString("answer", "").length() + " characters"));
                            
                            // Publish final event
                            publishStreamingEvent(conversationId, "final", new JsonObject()
                                .put("content", response.getString("answer", ""))
                                .put("conversationId", conversationId)
                                .put("method", "llm"));
                        }
                        
                        message.reply(response);
                        
                        // Add assistant response to conversation
                        conversation.addMessage("assistant", response.getString("answer"));
                    } else {
                        String errorMsg = ar.cause() != null && ar.cause().getMessage() != null ? 
                            ar.cause().getMessage() : "LLM processing failed";
                        
                        // Check for specific error types
                        if (errorMsg.contains("API key") || errorMsg.contains("401")) {
                            errorMsg = "LLM service authentication failed - please check API key configuration";
                        } else if (errorMsg.contains("timeout")) {
                            errorMsg = "LLM service request timed out - OpenAI may be slow or unavailable";
                        } else if (errorMsg.contains("Rate limit") || errorMsg.contains("429")) {
                            errorMsg = "LLM service rate limit exceeded - too many requests";
                        }
                        
                        vertx.eventBus().publish("log", "LLM processing failed: " + errorMsg + ",0,ToolFreeDirectLLMHost,Host,System");
                        
                        // Publish error event if streaming
                        if (sessionId != null && streaming) {
                            publishStreamingEvent(conversationId, "error", new JsonObject()
                                .put("error", errorMsg)
                                .put("severity", "ERROR")
                                .put("tool", "direct_llm_chat")
                                .put("errorType", "LLM processing error"));
                        }
                        
                        message.fail(500, errorMsg);
                    }
                });
        } else {
            // LLM service not initialized - provide detailed error
            String errorMsg = "LLM service not initialized - OpenAI API key may be missing or invalid. Please set OPENAI_API_KEY environment variable.";
            
            // Publish error for no LLM
            if (sessionId != null && streaming) {
                publishStreamingEvent(conversationId, "error", new JsonObject()
                    .put("error", errorMsg)
                    .put("severity", "ERROR")
                    .put("errorType", "Service unavailable")
                    .put("suggestion", "Set OPENAI_API_KEY environment variable and restart the service"));
            }
            
            // Create error response instead of fallback
            JsonObject errorResponse = new JsonObject()
                .put("error", errorMsg)
                .put("errorType", "Service unavailable")
                .put("host", "ToolFreeDirectLLMHost")
                .put("conversationId", conversationId)
                .put("duration", System.currentTimeMillis() - startTime)
                .put("timestamp", System.currentTimeMillis());
            
            // Log the error
            vertx.eventBus().publish("log", errorMsg + ",0,ToolFreeDirectLLMHost,Host,System");
            
            // Fail the message with proper error
            message.fail(503, errorMsg);
        }
        } catch (Exception e) {
            // Handle any unexpected errors
            String errorMessage = "Failed to process query: " + (e.getMessage() != null ? e.getMessage() : "Internal error");
            vertx.eventBus().publish("log", errorMessage + ": " + e.getMessage() + ",0,ToolFreeDirectLLMHost,ProcessQuery,Error");
            
            // Send error response
            JsonObject errorResponse = new JsonObject()
                .put("error", errorMessage)
                .put("errorType", "Processing error")
                .put("host", "ToolFreeDirectLLMHost")
                .put("timestamp", System.currentTimeMillis());
            
            if (message != null) {
                message.fail(500, errorMessage);
            }
        }
    }
    
    /**
     * Create conversation with initial history
     */
    private ConversationState createConversationWithHistory(String conversationId, JsonArray history) {
        ConversationState conversation = new ConversationState(conversationId);
        
        // Add provided history (excluding system prompt which is already added)
        for (int i = 0; i < history.size(); i++) {
            JsonObject msg = history.getJsonObject(i);
            String role = msg.getString("role");
            String content = msg.getString("content");
            
            if (!"system".equals(role)) {
                conversation.addMessage(role, content);
            }
        }
        
        return conversation;
    }
    
    /**
     * Process query with LLM
     */
    private Future<JsonObject> processWithLLM(ConversationState conversation, 
                                            String query, 
                                            JsonObject options,
                                            boolean streaming) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Get conversation messages
        JsonArray messages = conversation.getMessagesForLLM();
        
        // Convert to format expected by LLM service
        List<String> messageStrings = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            messageStrings.add(messages.getJsonObject(i).encode());
        }
        
        // LLM parameters
        double temperature = options.getDouble("temperature", 0.7);
        int maxTokens = options.getInteger("maxTokens", MAX_TOKENS);
        
        // Handle streaming vs non-streaming
        if (streaming) {
            // For streaming, we'd need to handle SSE responses
            // For now, we'll use non-streaming and simulate
            processNonStreaming(messageStrings, temperature, maxTokens, promise);
        } else {
            processNonStreaming(messageStrings, temperature, maxTokens, promise);
        }
        
        return promise.future();
    }
    
    /**
     * Process non-streaming LLM request
     */
    private void processNonStreaming(List<String> messages, 
                                    double temperature, 
                                    int maxTokens,
                                    Promise<JsonObject> promise) {
        llmService.chatCompletion(messages, temperature, maxTokens)
            .whenComplete((result, error) -> {
                if (error == null) {
                    try {
                        String answer = result.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message")
                            .getString("content");
                        
                        JsonObject response = new JsonObject()
                            .put("answer", answer)
                            .put("model", result.getString("model", "unknown"))
                            .put("usage", result.getJsonObject("usage", new JsonObject()));
                        
                        promise.complete(response);
                    } catch (Exception e) {
                        vertx.eventBus().publish("log", "Failed to parse LLM response" + ",0,ToolFreeDirectLLMHost,Host,System");
                        promise.fail(e);
                    }
                } else {
                    promise.fail(error);
                }
            });
    }
    
    /**
     * Create fallback response when LLM is unavailable
     */
    private JsonObject createFallbackResponse(String query) {
        String answer;
        
        // Provide helpful fallback messages based on query patterns
        String lowerQuery = query.toLowerCase();
        
        if (lowerQuery.contains("database") || lowerQuery.contains("sql") || 
            lowerQuery.contains("query") || lowerQuery.contains("data")) {
            answer = "I'm currently unable to access the language model service. " +
                    "For database-related questions, you might want to try the Oracle DB Answerer " +
                    "or SQL Builder services which can help with data retrieval and SQL generation.";
        } else if (lowerQuery.contains("help") || lowerQuery.contains("what can you do")) {
            answer = "I'm a direct response service that normally uses a language model to answer questions. " +
                    "However, the language model is currently unavailable. " +
                    "I can't access databases or external tools. " +
                    "Please try again later or use one of the specialized services for specific tasks.";
        } else if (lowerQuery.contains("hello") || lowerQuery.contains("hi")) {
            answer = "Hello! I'm currently operating in limited mode as the language model service is unavailable. " +
                    "I can maintain our conversation, but my responses will be basic. " +
                    "Please try again later for full functionality.";
        } else {
            answer = "I apologize, but I'm unable to process your request at this time. " +
                    "The language model service is currently unavailable. " +
                    "This service provides direct answers without database access or tools, " +
                    "but requires the language model to function properly. Please try again later.";
        }
        
        return new JsonObject()
            .put("answer", answer)
            .put("error", "LLM service unavailable")
            .put("fallback", true);
    }
    
    /**
     * Start periodic cleanup of expired conversations
     */
    private void startConversationCleanup() {
        // Run cleanup every 5 minutes
        vertx.setPeriodic(5 * 60 * 1000, id -> {
            int removed = 0;
            
            Iterator<Map.Entry<String, ConversationState>> iterator = conversations.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, ConversationState> entry = iterator.next();
                if (entry.getValue().isExpired()) {
                    iterator.remove();
                    removed++;
                }
            }
            
            if (removed > 0) {
                vertx.eventBus().publish("log", "Cleaned up " + removed + " expired conversations" + ",3,ToolFreeDirectLLMHost,Host,System");
            }
            
            // Also clean up old metrics
            cleanupOldMetrics();
        });
    }
    
    /**
     * Clean up old performance metrics
     */
    private void cleanupOldMetrics() {
        long cutoff = System.currentTimeMillis() - (60 * 60 * 1000); // 1 hour
        
        responseMetrics.entrySet().removeIf(entry -> {
            String key = entry.getKey();
            String[] parts = key.split("-");
            if (parts.length > 1) {
                try {
                    long timestamp = Long.parseLong(parts[parts.length - 1]);
                    return timestamp < cutoff;
                } catch (NumberFormatException e) {
                    return true; // Remove invalid entries
                }
            }
            return true;
        });
    }
    
    /**
     * Get conversation statistics
     */
    public JsonObject getConversationStats() {
        JsonObject stats = new JsonObject();
        
        // Conversation metrics
        stats.put("activeConversations", conversations.size());
        stats.put("totalMessages", conversations.values().stream()
            .mapToInt(c -> c.messages.size())
            .sum());
        
        // Performance metrics
        if (!responseMetrics.isEmpty()) {
            LongSummaryStatistics perfStats = responseMetrics.values().stream()
                .mapToLong(Long::longValue)
                .summaryStatistics();
            
            stats.put("averageResponseTime", perfStats.getAverage());
            stats.put("minResponseTime", perfStats.getMin());
            stats.put("maxResponseTime", perfStats.getMax());
            stats.put("totalResponses", perfStats.getCount());
        }
        
        return stats;
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Clean up resources
        conversations.clear();
        responseMetrics.clear();
        
        vertx.eventBus().publish("log", "ToolFreeDirectLLMHost stopped,2,ToolFreeDirectLLMHost,Host,System");
        stopPromise.complete();
    }
    
    /**
     * Publish streaming event to the correct event bus address
     */
    private void publishStreamingEvent(String conversationId, String eventType, JsonObject data) {
        // Use "streaming." prefix to match ConversationStreaming expectations
        String address = "streaming." + conversationId + "." + eventType;
        data.put("timestamp", System.currentTimeMillis());
        data.put("host", "ToolFreeDirectLLMHost");
        eventBus.publish(address, data);
    }
}