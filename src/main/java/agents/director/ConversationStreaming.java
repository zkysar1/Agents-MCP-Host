package agents.director;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.core.http.HttpServerResponse;

import agents.director.services.MCPRouterService;
import agents.director.services.InterruptManager;
import agents.director.services.OracleConnectionManager;
import static agents.director.Driver.logLevel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Master Streaming Conversation API with host-based routing.
 * This is the SOLE API entry point for all HTTP requests in the MCP architecture.
 * Provides health, status, MCP management, and conversation endpoints.
 * Automatically routes requests to the appropriate host based on content.
 */
public class ConversationStreaming extends AbstractVerticle {
    
    // Service start time for uptime tracking
    private static final long SERVICE_START_TIME = System.currentTimeMillis();
    
    private EventBus eventBus;
    private Router router;
    private InterruptManager interruptManager;
    
    // Track active streaming sessions
    private final Map<String, StreamingSession> activeSessions = new ConcurrentHashMap<>();
    
    // Host availability tracking
    private final Map<String, Boolean> hostAvailability = new ConcurrentHashMap<>();
    
    // Performance metrics
    private final Map<String, Long> requestMetrics = new ConcurrentHashMap<>();
    
    // Configuration
    private static final long DEFAULT_TIMEOUT = 120000; // 2 minutes
    private static final String DEFAULT_HOST = "oracledbanswerer";
    private static final long HEARTBEAT_INTERVAL = 8000; // 8 seconds
    private static final int MAX_ACTIVE_SESSIONS = 100; // Limit active sessions
    private static final int MAX_CRITICAL_ERRORS = 50; // Limit stored errors
    
    // Critical error tracking
    private final Map<String, JsonObject> criticalErrors = new ConcurrentHashMap<>();
    
    // System readiness flag
    private volatile boolean systemReady = false;
    
    // Inner class for enhanced streaming session management
    private class EnhancedStreamingSession extends StreamingSession {
        List<MessageConsumer<?>> consumers = new CopyOnWriteArrayList<>();
        Long heartbeatTimerId;
        Long timeoutTimerId;
        volatile boolean responseEnded = false;
        String host; // track which host is handling
        HttpServerResponse response;
        
        EnhancedStreamingSession(String sessionId, String conversationId, RoutingContext context, String host) {
            super(sessionId, conversationId, context);
            this.host = host;
            this.response = context.response();
        }
        
        // Thread-safe SSE write
        synchronized void sendSSEEvent(String eventType, JsonObject data) {
            if (!responseEnded && response != null && !response.closed()) {
                try {
                    String event = "event: " + eventType + "\n" +
                                  "data: " + data.encode() + "\n\n";
                    response.write(event);
                    // Force flush to ensure event is sent immediately
                    response.drainHandler(null);
                } catch (Exception e) {
                    // Response closed during write
                    responseEnded = true;
                    if (logLevel >= 3) vertx.eventBus().publish("log", "SSE write failed - connection likely closed,3,ConversationStreaming,SSE,Write");
                }
            }
        }
        
        void cleanup() {
            // Unregister all event consumers
            consumers.forEach(consumer -> {
                try {
                    consumer.unregister();
                } catch (Exception e) {
                    // Ignore errors during cleanup
                }
            });
            consumers.clear();
            
            // Cancel timers with error handling
            if (heartbeatTimerId != null) {
                try {
                    vertx.cancelTimer(heartbeatTimerId);
                } catch (Exception e) {
                    // Ignore timer cancellation errors
                }
                heartbeatTimerId = null;
            }
            if (timeoutTimerId != null) {
                try {
                    vertx.cancelTimer(timeoutTimerId);
                } catch (Exception e) {
                    // Ignore timer cancellation errors
                }
                timeoutTimerId = null;
            }
            
            // Mark as cleaned up
            completed = true;
            responseEnded = true;
        }
    }
    
    // Keep original StreamingSession for backward compatibility
    private static class StreamingSession {
        String sessionId;
        String conversationId;
        RoutingContext context;
        long startTime;
        boolean completed;
        
        StreamingSession(String sessionId, String conversationId, RoutingContext context) {
            this.sessionId = sessionId;
            this.conversationId = conversationId;
            this.context = context;
            this.startTime = System.currentTimeMillis();
            this.completed = false;
        }
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        eventBus = vertx.eventBus();
        interruptManager = new InterruptManager(vertx);
        
        // Listen for system ready event
        eventBus.consumer("system.fully.ready", msg -> {
            systemReady = true;
            if (logLevel >= 1) vertx.eventBus().publish("log", "System fully ready - accepting requests,1,ConversationStreaming,System,Ready");
        });
        
        // Listen for critical error events
        eventBus.consumer("critical.error", msg -> {
            JsonObject error = (JsonObject) msg.body();
            String errorId = "error_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString();
            criticalErrors.put(errorId, error);
            
            vertx.eventBus().publish("log", "[CRITICAL ERROR] " + error.encode() + ",0,ConversationStreaming,Error,Critical");
            
            // Auto-cleanup after 5 minutes
            vertx.setTimer(300000, id -> criticalErrors.remove(errorId));
            
            // Limit critical errors to prevent memory issues
            if (criticalErrors.size() > MAX_CRITICAL_ERRORS) {
                // Remove oldest errors
                List<String> errorIds = new ArrayList<>(criticalErrors.keySet());
                errorIds.sort(String::compareTo);
                for (int i = 0; i < errorIds.size() - MAX_CRITICAL_ERRORS; i++) {
                    criticalErrors.remove(errorIds.get(i));
                }
            }
            
            // Notify all active sessions
            activeSessions.values().forEach(session -> {
                if (session instanceof EnhancedStreamingSession) {
                    EnhancedStreamingSession enhancedSession = (EnhancedStreamingSession) session;
                    enhancedSession.sendSSEEvent("critical_error", new JsonObject()
                        .put("message", error.getString("userMessage", "System critical error occurred"))
                        .put("severity", "CRITICAL"));
                }
            });
        });
        
        // Create router
        router = Router.router(vertx);
        
        // Add handlers
        router.route().handler(BodyHandler.create());
        
        // Root welcome page
        router.get("/").handler(this::handleRoot);
        
        // Main conversation endpoint (streaming-only)
        router.post("/conversations").handler(this::handleConversation);
        
        // Host status endpoint
        router.get("/hosts/status").handler(this::handleHostStatus);
        
        // Session management
        router.delete("/conversations/:sessionId").handler(this::handleSessionCancel);
        
        // Interrupt control endpoints
        router.post("/conversations/:sessionId/interrupt").handler(this::handleInterrupt);
        router.post("/conversations/:sessionId/feedback").handler(this::handleFeedback);
        router.get("/conversations/:sessionId/status").handler(this::handleSessionStatus);
        
        // Health and status endpoints
        router.get("/health").handler(this::handleHealth);
        router.get("/status").handler(this::handleStatus);
        router.get("/mcp/status").handler(this::handleMcpStatus);
        router.get("/mcp/tools").handler(this::handleMcpTools);
        router.get("/mcp/clients").handler(this::handleMcpClients);
        
        // Register router with MCPRouterService
        registerRouterWithMCPService();
        
        // Check host availability
        checkHostAvailability();
        
        // Periodic cleanup
        startSessionCleanup();
        
        if (logLevel >= 1) vertx.eventBus().publish("log", "ConversationStreaming API initialized,1,ConversationStreaming,API,System");
        startPromise.complete();
    }
    
    /**
     * Main conversation handler with host routing (streaming-only)
     */
    private void handleConversation(RoutingContext context) {
        // Check if system is ready
        if (!systemReady) {
            sendEarlyError(context, 503, "System is starting up. Please try again in a few seconds.");
            if (logLevel >= 3) vertx.eventBus().publish("log", "Request rejected - system not ready,3,ConversationStreaming,Request,Validation");
            return;
        }
        
        // Check for critical errors
        if (!criticalErrors.isEmpty()) {
            JsonObject firstError = criticalErrors.values().iterator().next();
            String userErrorMessage = firstError.getString("userMessage", 
                "System critical error occurred. Please restart the system or contact support.");
            sendEarlyError(context, 503, userErrorMessage);
            vertx.eventBus().publish("log", "Request blocked due to critical error,0,ConversationStreaming,Request,Critical");
            return;
        }
        
        JsonObject request = context.body().asJsonObject();
        
        // Validate request
        if (request == null || !request.containsKey("messages")) {
            sendEarlyError(context, 400, "Invalid request: messages array required");
            return;
        }
        
        JsonArray messages = request.getJsonArray("messages");
        if (messages.isEmpty()) {
            sendEarlyError(context, 400, "Invalid request: messages array cannot be empty");
            return;
        }
        
        // Extract parameters
        String host = request.getString("host", DEFAULT_HOST);
        String conversationId = request.getString("conversationId", UUID.randomUUID().toString());
        JsonObject options = request.getJsonObject("options", new JsonObject());
        
        // Get the latest user message
        JsonObject lastMessage = messages.size() > 0 ? messages.getJsonObject(messages.size() - 1) : null;
        String query = lastMessage != null ? lastMessage.getString("content", "") : "";
        
        // Log request
        vertx.eventBus().publish("log", "Processing conversation request - Host: " + host + ", ConvId: " + conversationId + ", Query: " + query + ",2,ConversationStreaming,API,System");
        
        // Track request
        String requestId = UUID.randomUUID().toString();
        requestMetrics.put(requestId, System.currentTimeMillis());
        
        // Now that validation passed, set up SSE headers for streaming
        context.response()
            .putHeader("Content-Type", "text/event-stream")
            .putHeader("Cache-Control", "no-cache")
            .putHeader("Connection", "keep-alive")
            .putHeader("Access-Control-Allow-Origin", "*")
            .setChunked(true);
        
        // Route to appropriate host (always streaming)
        routeToHost(context, host, conversationId, query, messages, options, requestId);
    }
    
    
    /**
     * Route request to the appropriate host (streaming-only)
     */
    private void routeToHost(RoutingContext context, 
                           String host, 
                           String conversationId, 
                           String query,
                           JsonArray messages,
                           JsonObject options,
                           String requestId) {
        
        // Validate host availability
        String selectedHost = host;
        if (!isHostAvailable(selectedHost)) {
            // Try fallback hosts
            selectedHost = selectFallbackHost(selectedHost);
            if (selectedHost == null) {
                sendStreamingError(context, 503, "No available hosts to process request");
                return;
            }
            if (logLevel >= 1) vertx.eventBus().publish("log", "Original host unavailable; falling back to: " + selectedHost + ",1,ConversationStreaming,API,System");
        }
        final String finalHost = selectedHost;
        
        // Create streaming session
        // Check session limit
        if (activeSessions.size() >= MAX_ACTIVE_SESSIONS) {
            sendStreamingError(context, 503, "Server at capacity - too many active sessions");
            return;
        }
        
        String sessionId = UUID.randomUUID().toString();
        
        // Create enhanced session
        EnhancedStreamingSession enhancedSession = new EnhancedStreamingSession(sessionId, conversationId, context, finalHost);
        activeSessions.put(sessionId, enhancedSession);
        
        // Send initial connection event
        enhancedSession.sendSSEEvent("connected", new JsonObject()
            .put("sessionId", sessionId)
            .put("conversationId", conversationId)
            .put("host", finalHost)
            .put("timestamp", System.currentTimeMillis()));
        
        // Notify schema resolver about new session
        eventBus.publish("session.schema.resolver.init", new JsonObject()
            .put("sessionId", sessionId)
            .put("timestamp", System.currentTimeMillis()));
        
        // Set up event consumers BEFORE sending to host
        boolean setupSuccessful = false;
        try {
            setupEventConsumers(enhancedSession, options);
            setupSuccessful = true;
        } catch (Exception e) {
            // Log error
            vertx.eventBus().publish("log", "Failed to setup event consumers: " + e.getMessage() + ",0,ConversationStreaming,Session,Setup");
        } finally {
            // Clean up on error - ensure session is removed from activeSessions
            if (!setupSuccessful) {
                activeSessions.remove(sessionId);
                sendStreamingError(context, 500, "Failed to initialize streaming session");
                return;
            }
        }
        
        // Set up cleanup handlers
        final String finalSessionId = sessionId;
        final EnhancedStreamingSession finalEnhancedSession = enhancedSession;
        context.response().closeHandler(v -> {
            finalEnhancedSession.responseEnded = true;
            cleanupSession(finalSessionId);
            if (logLevel >= 3) vertx.eventBus().publish("log", "Client closed connection for session: " + finalSessionId + ",3,ConversationStreaming,Session,Cleanup");
        });
        
        context.response().exceptionHandler(err -> {
            finalEnhancedSession.responseEnded = true;
            cleanupSession(finalSessionId);
            vertx.eventBus().publish("log", "Connection error for session: " + finalSessionId + " - " + err.getMessage() + ",0,ConversationStreaming,Session,Error");
        });
        
        // Build message for host
        JsonObject hostMessage = new JsonObject()
            .put("query", query)
            .put("conversationId", conversationId)
            .put("history", messages)
            .put("options", options)
            .put("streaming", true)  // Always streaming
            .put("requestId", requestId)
            .put("sessionId", finalSessionId);
        
        // Send to host via event bus
        String hostAddress = "host." + finalHost.toLowerCase() + ".process";
        
        // Use custom timeout for event bus request (2 minutes instead of default 30 seconds)
        DeliveryOptions deliveryOptions = new DeliveryOptions().setSendTimeout(DEFAULT_TIMEOUT);
        
        eventBus.<JsonObject>request(hostAddress, hostMessage, deliveryOptions, ar -> {
            long duration = System.currentTimeMillis() - requestMetrics.getOrDefault(requestId, System.currentTimeMillis());
            requestMetrics.remove(requestId);
            
            if (ar.succeeded()) {
                Message<JsonObject> result = ar.result();
                if (result == null || result.body() == null) {
                    // Handle null response with context
                    String errorMessage = String.format("Host '%s' returned null response after %dms", 
                        finalHost, duration);
                    JsonObject errorDetails = new JsonObject()
                        .put("error", errorMessage)
                        .put("host", finalHost)
                        .put("requestId", requestId)
                        .put("duration", duration)
                        .put("timestamp", System.currentTimeMillis())
                        .put("querySnippet", query.length() > 100 ? query.substring(0, 100) + "..." : query);
                    
                    sendSSEEvent(context, "error", errorDetails);
                    context.response().end();
                    cleanupSession(finalSessionId);
                    vertx.eventBus().publish("log", errorMessage + ",0,ConversationStreaming,Host,NullResponse");
                    return;
                }
                
                JsonObject response = result.body();
                response.put("host", finalHost);
                response.put("processingTime", duration);
                
                // For streaming, the host will publish events to the event bus
                // We just need to wait for them. The response here is just an acknowledgment.
                // Do NOT send the response body or end the stream here!
                
                // The event consumers set up earlier will handle:
                // - progress events
                // - tool events  
                // - final response event (which will end the stream)
                // - error events
                
                // Log that streaming has started
                if (logLevel >= 3) {
                    vertx.eventBus().publish("log", "Streaming started for session " + finalSessionId + " with host " + finalHost + ",3,ConversationStreaming,Streaming,Started");
                }
            } else {
                // Enhanced error handling with context
                Throwable cause = ar.cause();
                String errorType = "Unknown error";
                String errorMessage = null;
                
                if (cause != null) {
                    errorMessage = cause.getMessage();
                    // Classify error types
                    if (errorMessage != null) {
                        if (errorMessage.contains("NO_HANDLERS") || errorMessage.contains("No handlers")) {
                            errorType = "Host not available";
                            errorMessage = String.format("Host '%s' is not available. The service may not be deployed or is not responding.", finalHost);
                        } else if (errorMessage.contains("timed out") || errorMessage.contains("timeout")) {
                            errorType = "Request timeout";
                            errorMessage = String.format("Host '%s' did not respond within the timeout period", finalHost);
                        } else if (errorMessage.contains("LLM") || errorMessage.contains("OpenAI")) {
                            errorType = "LLM service error";
                        }
                    } else {
                        errorMessage = "Host processing failed without error details";
                    }
                } else {
                    errorMessage = String.format("Host '%s' processing failed with null error", finalHost);
                }
                
                // Log detailed error
                vertx.eventBus().publish("log", String.format(
                    "Host processing failed - Type: %s, Host: %s, RequestId: %s, Error: %s",
                    errorType, finalHost, requestId, errorMessage) + ",0,ConversationStreaming,API,System");
                
                // Create detailed error object
                JsonObject errorDetails = new JsonObject()
                    .put("error", errorMessage)
                    .put("errorType", errorType)
                    .put("host", finalHost)
                    .put("requestId", requestId)
                    .put("duration", duration)
                    .put("timestamp", System.currentTimeMillis())
                    .put("querySnippet", query.length() > 100 ? query.substring(0, 100) + "..." : query);
                
                sendSSEEvent(context, "error", errorDetails);
                context.response().end();
                cleanupSession(finalSessionId);
            }
        });
    }
    
    /**
     * Handle host status request
     */
    private void handleHostStatus(RoutingContext context) {
        JsonObject status = new JsonObject();
        JsonArray hosts = new JsonArray();
        
        // Check each host
        for (String hostName : Arrays.asList("oracledbanswerer", "oraclesqlbuilder", "toolfreedirectllm")) {
            JsonObject hostInfo = new JsonObject()
                .put("name", hostName)
                .put("available", isHostAvailable(hostName));
            
            // Get additional status from host
            String statusAddress = "host." + hostName + ".status";
            eventBus.<JsonObject>request(statusAddress, new JsonObject(), ar -> {
                if (ar.succeeded()) {
                    hostInfo.mergeIn(ar.result().body());
                }
            });
            
            hosts.add(hostInfo);
        }
        
        status.put("hosts", hosts);
        status.put("activeSessions", activeSessions.size());
        status.put("defaultHost", DEFAULT_HOST);
        
        sendJsonResponse(context, 200, status);
    }
    
    /**
     * Handle session cancellation
     */
    private void handleSessionCancel(RoutingContext context) {
        String sessionId = context.pathParam("sessionId");
        
        StreamingSession session = activeSessions.remove(sessionId);
        if (session != null) {
            vertx.eventBus().publish("log", "Cancelled session: " + sessionId + "" + ",2,ConversationStreaming,API,System");
            
            // Notify host about cancellation
            eventBus.publish("session.cancelled", new JsonObject()
                .put("sessionId", sessionId)
                .put("conversationId", session.conversationId));
            
            sendJsonResponse(context, 200, new JsonObject()
                .put("cancelled", true)
                .put("sessionId", sessionId));
        } else {
            sendError(context, 404, "Session not found");
        }
    }
    
    /**
     * Register this router with the MCPRouterService
     */
    private void registerRouterWithMCPService() {
        // Register our router with the MCPRouterService
        // The static method handles both immediate mounting (if router service is ready)
        // or deferred mounting (if router service starts later)
        MCPRouterService.registerRouter("/host/v1", router);
        vertx.eventBus().publish("log", "Registered ConversationStreaming router for path /host/v1,2,ConversationStreaming,API,System");
    }
    
    /**
     * Check host availability
     */
    private void checkHostAvailability() {
        // Initial check
        updateHostAvailability();
        
        // Periodic checks every 30 seconds
        vertx.setPeriodic(30000, id -> updateHostAvailability());
    }
    
    private void updateHostAvailability() {
        for (String hostName : Arrays.asList("oracledbanswerer", "oraclesqlbuilder", "toolfreedirectllm")) {
            String statusAddress = "host." + hostName + ".status";
            
            eventBus.<JsonObject>request(statusAddress, new JsonObject(), ar -> {
                boolean available = ar.succeeded();
                hostAvailability.put(hostName, available);
                
                if (!available) {
                    vertx.eventBus().publish("log", "Host " + hostName + " is not available" + ",1,ConversationStreaming,API,System");
                }
            });
        }
    }
    
    private boolean isHostAvailable(String host) {
        return hostAvailability.getOrDefault(host.toLowerCase(), false);
    }
    
    /**
     * Select fallback host based on availability
     */
    private String selectFallbackHost(String originalHost) {
        // Fallback order based on original host
        List<String> fallbackOrder;
        
        switch (originalHost.toLowerCase()) {
            case "oracledbanswerer":
                fallbackOrder = Arrays.asList("oraclesqlbuilder", "toolfreedirectllm");
                break;
            case "oraclesqlbuilder":
                fallbackOrder = Arrays.asList("oracledbanswerer", "toolfreedirectllm");
                break;
            case "toolfreedirectllm":
                fallbackOrder = Arrays.asList("oracledbanswerer", "oraclesqlbuilder");
                break;
            default:
                fallbackOrder = Arrays.asList("oracledbanswerer", "oraclesqlbuilder", "toolfreedirectllm");
        }
        
        for (String fallback : fallbackOrder) {
            if (isHostAvailable(fallback)) {
                return fallback;
            }
        }
        
        return null; // No available hosts
    }
    
    /**
     * Send SSE event
     */
    private void sendSSEEvent(RoutingContext context, String event, JsonObject data) {
        if (!context.response().ended()) {
            String eventData = "event: " + event + "\n" +
                              "data: " + data.encode() + "\n\n";
            
            context.response().write(eventData);
        }
    }
    
    /**
     * Send JSON response
     */
    private void sendJsonResponse(RoutingContext context, int statusCode, JsonObject response) {
        if (!context.response().ended()) {
            context.response()
                .setStatusCode(statusCode)
                .putHeader("Content-Type", "application/json")
                .end(response.encode());
        }
    }
    
    /**
     * Send error response
     */
    private void sendError(RoutingContext context, int statusCode, String message) {
        JsonObject error = new JsonObject()
            .put("error", message)
            .put("statusCode", statusCode)
            .put("timestamp", System.currentTimeMillis())
            .put("service", "ConversationStreaming");
        
        // Add request context if available
        if (context.body() != null && context.body().asJsonObject() != null) {
            JsonObject requestBody = context.body().asJsonObject();
            String host = requestBody.getString("host", "unknown");
            error.put("requestedHost", host);
        }
        
        sendJsonResponse(context, statusCode, error);
    }
    
    /**
     * Send early error response (non-SSE) for errors that occur before session creation
     */
    private void sendEarlyError(RoutingContext context, int statusCode, String message) {
        JsonObject error = new JsonObject()
            .put("error", message)
            .put("statusCode", statusCode)
            .put("timestamp", System.currentTimeMillis())
            .put("service", "ConversationStreaming")
            .put("severity", "ERROR");
        
        // Add request context if available
        if (context.body() != null && context.body().asJsonObject() != null) {
            JsonObject requestBody = context.body().asJsonObject();
            String host = requestBody.getString("host", "unknown");
            error.put("requestedHost", host);
        }
        
        // Send as regular JSON response, not SSE
        sendJsonResponse(context, statusCode, error);
    }
    
    /**
     * Send error response via SSE for streaming
     */
    private void sendStreamingError(RoutingContext context, int statusCode, String message) {
        JsonObject error = new JsonObject()
            .put("error", message)
            .put("statusCode", statusCode)
            .put("timestamp", System.currentTimeMillis())
            .put("service", "ConversationStreaming")
            .put("severity", "ERROR");
        
        // Add request context if available
        if (context.body() != null && context.body().asJsonObject() != null) {
            JsonObject requestBody = context.body().asJsonObject();
            String host = requestBody.getString("host", "unknown");
            error.put("requestedHost", host);
        }
        
        // Send error event and close stream
        sendSSEEvent(context, "error", error);
        // Add response flushing to ensure data is sent before closing
        context.response().drainHandler(v -> context.response().end());
    }
    
    /**
     * Set up event consumers for a streaming session
     */
    private void setupEventConsumers(EnhancedStreamingSession session, JsonObject options) {
        String conversationId = session.conversationId;
        String sessionId = session.sessionId;
        
        // Progress events from hosts
        MessageConsumer<JsonObject> progressConsumer = eventBus.consumer("streaming." + conversationId + ".progress", msg -> {
            JsonObject progress = msg.body();
            session.sendSSEEvent("progress", progress
                .put("sessionId", sessionId)
                .put("timestamp", System.currentTimeMillis()));
        });
        session.consumers.add(progressConsumer);
        
        // Tool start events
        MessageConsumer<JsonObject> toolStartConsumer = eventBus.consumer("streaming." + conversationId + ".tool.start", msg -> {
            JsonObject toolInfo = msg.body();
            session.sendSSEEvent("tool_start", toolInfo
                .put("sessionId", sessionId)
                .put("timestamp", System.currentTimeMillis()));
        });
        session.consumers.add(toolStartConsumer);
        
        // Tool complete events
        MessageConsumer<JsonObject> toolCompleteConsumer = eventBus.consumer("streaming." + conversationId + ".tool.complete", msg -> {
            JsonObject toolResult = msg.body();
            session.sendSSEEvent("tool_complete", toolResult
                .put("sessionId", sessionId)
                .put("timestamp", System.currentTimeMillis()));
        });
        session.consumers.add(toolCompleteConsumer);
        
        // Final response events
        MessageConsumer<JsonObject> finalConsumer = eventBus.consumer("streaming." + conversationId + ".final", msg -> {
            JsonObject finalResponse = msg.body();
            session.sendSSEEvent("final", finalResponse
                .put("sessionId", sessionId)
                .put("timestamp", System.currentTimeMillis()));
            
            // Mark session as completed
            session.completed = true;
            
            // End the response properly after sending the final event
            vertx.setTimer(100, id -> {
                if (!session.responseEnded && session.response != null && !session.response.closed()) {
                    try {
                        session.response.end();
                        session.responseEnded = true;
                    } catch (Exception e) {
                        // Response already closed
                    }
                }
                
                // Schedule cleanup after ending response
                vertx.setTimer(5000, cleanupId -> cleanupSession(sessionId));
            });
        });
        session.consumers.add(finalConsumer);
        
        // Error events
        MessageConsumer<JsonObject> errorConsumer = eventBus.consumer("streaming." + conversationId + ".error", msg -> {
            JsonObject error = msg.body();
            session.sendSSEEvent("error", error
                .put("sessionId", sessionId)
                .put("severity", error.getString("severity", "ERROR"))
                .put("timestamp", System.currentTimeMillis()));
        });
        session.consumers.add(errorConsumer);
        
        // Interrupt events
        MessageConsumer<JsonObject> interruptConsumer = eventBus.consumer("streaming." + conversationId + ".interrupt", msg -> {
            JsonObject interrupt = msg.body();
            session.sendSSEEvent("interrupt", interrupt
                .put("sessionId", sessionId)
                .put("timestamp", System.currentTimeMillis()));
        });
        session.consumers.add(interruptConsumer);
        
        // Agent question events (for user interaction)
        MessageConsumer<JsonObject> questionConsumer = eventBus.consumer("streaming." + conversationId + ".agent_question", msg -> {
            JsonObject question = msg.body();
            session.sendSSEEvent("agent_question", question
                .put("sessionId", sessionId)
                .put("requiresResponse", true)
                .put("timestamp", System.currentTimeMillis()));
        });
        session.consumers.add(questionConsumer);
        
        // Set up heartbeat timer
        session.heartbeatTimerId = vertx.setPeriodic(HEARTBEAT_INTERVAL, id -> {
            if (!session.responseEnded) {
                session.sendSSEEvent("heartbeat", new JsonObject()
                    .put("sessionId", sessionId)
                    .put("timestamp", System.currentTimeMillis()));
            }
        });
        
        // Set up timeout timer
        long timeout = options.getLong("timeout", DEFAULT_TIMEOUT);
        session.timeoutTimerId = vertx.setTimer(timeout, id -> {
            if (!session.completed && !session.responseEnded) {
                // Check if interrupted
                if (interruptManager != null && interruptManager.isInterrupted(sessionId)) {
                    session.sendSSEEvent("interrupted", new JsonObject()
                        .put("sessionId", sessionId)
                        .put("message", "Request was interrupted")
                        .put("timestamp", System.currentTimeMillis()));
                    cleanupSession(sessionId);
                    return;
                }
                session.sendSSEEvent("timeout", new JsonObject()
                    .put("sessionId", sessionId)
                    .put("message", "Request timed out after " + (timeout / 1000) + " seconds")
                    .put("timestamp", System.currentTimeMillis()));
                
                // Clean up the session
                cleanupSession(sessionId);
                
                // Notify host about timeout
                eventBus.publish("session.timeout", new JsonObject()
                    .put("sessionId", sessionId)
                    .put("conversationId", conversationId)
                    .put("host", session.host));
            }
        });
        
        if (logLevel >= 3) vertx.eventBus().publish("log", "Set up " + session.consumers.size() + " event consumers for session: " + sessionId + ",3,ConversationStreaming,Session,Setup");
    }
    
    /**
     * Clean up a streaming session
     */
    private void cleanupSession(String sessionId) {
        StreamingSession session = activeSessions.remove(sessionId);
        if (session != null && session instanceof EnhancedStreamingSession) {
            EnhancedStreamingSession enhancedSession = (EnhancedStreamingSession) session;
            enhancedSession.cleanup();
            if (logLevel >= 3) vertx.eventBus().publish("log", "Cleaned up session: " + sessionId + ",3,ConversationStreaming,Session,Cleanup");
        }
        
        // Clean up interrupt manager state
        if (interruptManager != null) {
            interruptManager.clearInterrupt(sessionId);
        }
        
        // Notify schema resolver about session cleanup
        eventBus.publish("session.schema.resolver.cleanup", new JsonObject()
            .put("sessionId", sessionId)
            .put("timestamp", System.currentTimeMillis()));
    }
    
    /**
     * Handle interrupt request for a session
     */
    private void handleInterrupt(RoutingContext context) {
        String sessionId = context.pathParam("sessionId");
        JsonObject request = context.body().asJsonObject();
        
        StreamingSession session = activeSessions.get(sessionId);
        if (session == null) {
            sendError(context, 404, "Session not found");
            return;
        }
        
        // Extract interrupt details
        String reason = request.getString("reason", "User requested interrupt");
        boolean graceful = request.getBoolean("graceful", true);
        
        // Mark conversation as interrupted using InterruptManager
        if (interruptManager != null) {
            interruptManager.interrupt(sessionId, reason);
        }
        
        // Publish interrupt event to the host using consistent addressing
        eventBus.publish("streaming." + session.conversationId + ".interrupt", new JsonObject()
            .put("sessionId", sessionId)
            .put("conversationId", session.conversationId)
            .put("reason", reason)
            .put("graceful", graceful)
            .put("timestamp", System.currentTimeMillis()));
        
        // Send interrupt confirmation
        if (session instanceof EnhancedStreamingSession) {
            EnhancedStreamingSession enhancedSession = (EnhancedStreamingSession) session;
            enhancedSession.sendSSEEvent("interrupt_acknowledged", new JsonObject()
                .put("sessionId", sessionId)
                .put("reason", reason)
                .put("timestamp", System.currentTimeMillis()));
        }
        
        sendJsonResponse(context, 200, new JsonObject()
            .put("interrupted", true)
            .put("sessionId", sessionId)
            .put("graceful", graceful));
        
        if (logLevel >= 1) vertx.eventBus().publish("log", "Session interrupted: " + sessionId + " - " + reason + ",1,ConversationStreaming,Session,Interrupt");
    }
    
    /**
     * Handle feedback submission for a session
     */
    private void handleFeedback(RoutingContext context) {
        String sessionId = context.pathParam("sessionId");
        JsonObject feedback = context.body().asJsonObject();
        
        StreamingSession session = activeSessions.get(sessionId);
        if (session == null) {
            // Session might have completed, but we can still accept feedback
            if (logLevel >= 3) vertx.eventBus().publish("log", "Feedback received for completed session: " + sessionId + ",3,ConversationStreaming,Session,Feedback");
        }
        
        // Add metadata to feedback
        feedback.put("sessionId", sessionId);
        feedback.put("timestamp", System.currentTimeMillis());
        
        if (session != null) {
            feedback.put("conversationId", session.conversationId);
            feedback.put("sessionDuration", System.currentTimeMillis() - session.startTime);
            
            if (session instanceof EnhancedStreamingSession) {
                EnhancedStreamingSession enhancedSession = (EnhancedStreamingSession) session;
                feedback.put("host", enhancedSession.host);
            }
        }
        
        // Store feedback using InterruptManager
        if (interruptManager != null) {
            interruptManager.storeFeedback(sessionId, feedback);
        }
        
        // Publish feedback event
        eventBus.publish("session.feedback", feedback);
        
        sendJsonResponse(context, 200, new JsonObject()
            .put("received", true)
            .put("sessionId", sessionId));
    }
    
    /**
     * Get session status
     */
    private void handleSessionStatus(RoutingContext context) {
        String sessionId = context.pathParam("sessionId");
        
        StreamingSession session = activeSessions.get(sessionId);
        if (session == null) {
            sendError(context, 404, "Session not found");
            return;
        }
        
        JsonObject status = new JsonObject()
            .put("sessionId", sessionId)
            .put("conversationId", session.conversationId)
            .put("startTime", session.startTime)
            .put("duration", System.currentTimeMillis() - session.startTime)
            .put("completed", session.completed);
        
        if (session instanceof EnhancedStreamingSession) {
            EnhancedStreamingSession enhancedSession = (EnhancedStreamingSession) session;
            status.put("host", enhancedSession.host)
                  .put("eventConsumers", enhancedSession.consumers.size())
                  .put("connectionActive", !enhancedSession.responseEnded);
        }
        
        sendJsonResponse(context, 200, status);
    }
    
    /**
     * Handle health check endpoint
     */
    private void handleHealth(RoutingContext context) {
        JsonObject health = new JsonObject()
            .put("status", systemReady ? "healthy" : "starting")
            .put("systemReady", systemReady)
            .put("activeSessions", activeSessions.size())
            .put("criticalErrors", criticalErrors.size())
            .put("timestamp", System.currentTimeMillis());
        
        sendJsonResponse(context, systemReady ? 200 : 503, health);
    }
    
    /**
     * Handle root endpoint - API welcome page
     */
    private void handleRoot(RoutingContext context) {
        context.response()
            .putHeader("Content-Type", "text/html")
            .end("<h1>Welcome to ZAK-Agent Host API</h1>" +
                 "<h2>Available Endpoints:</h2>" +
                 "<ul>" +
                 "<li><a href='/host/v1/health'>/host/v1/health</a> - Health check</li>" +
                 "<li><a href='/host/v1/status'>/host/v1/status</a> - System status</li>" +
                 "<li>/host/v1/conversations - Conversation API (POST)</li>" +
                 "<li><a href='/host/v1/mcp/status'>/host/v1/mcp/status</a> - MCP status</li>" +
                 "<li><a href='/host/v1/mcp/tools'>/host/v1/mcp/tools</a> - Available tools</li>" +
                 "<li><a href='/host/v1/mcp/clients'>/host/v1/mcp/clients</a> - MCP clients</li>" +
                 "</ul>");
    }
    
    /**
     * Handle status endpoint - comprehensive system status
     */
    private void handleStatus(RoutingContext context) {
        JsonObject status = new JsonObject()
            .put("status", "running")
            .put("version", "1.0.0")
            .put("timestamp", System.currentTimeMillis())
            .put("uptime", System.currentTimeMillis() - SERVICE_START_TIME)
            .put("environment", System.getProperty("os.name", "Unknown"))
            .put("activeSessions", activeSessions.size())
            .put("criticalErrors", criticalErrors.size());
        
        // Add database connection status
        try {
            OracleConnectionManager oracleManager = OracleConnectionManager.getInstance();
            JsonObject dbStatus = oracleManager.getConnectionStatus();
            status.put("database", dbStatus);
        } catch (Exception e) {
            status.put("database", new JsonObject()
                .put("healthy", false)
                .put("error", "Not initialized or error: " + 
                    (e.getMessage() != null ? e.getMessage() : "Unknown error")));
        }
        
        // Request MCP status from HostManager
        eventBus.<JsonObject>request("mcp.host.status", new JsonObject())
            .onSuccess(reply -> {
                status.put("mcp", reply.body() != null ? reply.body() : new JsonObject());
                sendJsonResponse(context, 200, status);
            })
            .onFailure(err -> {
                status.put("mcp", new JsonObject()
                    .put("error", "Could not retrieve MCP status")
                    .put("message", err.getMessage() != null ? err.getMessage() : "Unknown error"));
                sendJsonResponse(context, 200, status);
            });
    }
    
    /**
     * Handle MCP status endpoint
     */
    private void handleMcpStatus(RoutingContext context) {
        // Request MCP status via event bus
        eventBus.<JsonObject>request("mcp.status", new JsonObject(), ar -> {
            if (ar.succeeded() && ar.result() != null) {
                sendJsonResponse(context, 200, ar.result().body() != null ? ar.result().body() : new JsonObject());
            } else {
                sendError(context, 503, "MCP system not available");
            }
        });
    }
    
    /**
     * Handle MCP tools listing endpoint
     */
    private void handleMcpTools(RoutingContext context) {
        // Request tools list via event bus
        eventBus.<JsonObject>request("mcp.tools.list", new JsonObject(), ar -> {
            if (ar.succeeded() && ar.result() != null) {
                sendJsonResponse(context, 200, ar.result().body() != null ? ar.result().body() : new JsonObject());
            } else {
                sendError(context, 503, "Unable to retrieve MCP tools");
            }
        });
    }
    
    /**
     * Handle MCP clients listing endpoint
     */
    private void handleMcpClients(RoutingContext context) {
        // Request clients list via event bus
        eventBus.<JsonObject>request("mcp.clients.list", new JsonObject(), ar -> {
            if (ar.succeeded() && ar.result() != null) {
                sendJsonResponse(context, 200, ar.result().body() != null ? ar.result().body() : new JsonObject());
            } else {
                sendError(context, 503, "Unable to retrieve MCP clients");
            }
        });
    }
    
    /**
     * Start periodic session cleanup
     */
    private void startSessionCleanup() {
        // Clean up old sessions every minute
        vertx.setPeriodic(60000, id -> {
            long cutoff = System.currentTimeMillis() - (5 * 60 * 1000); // 5 minutes
            
            List<String> sessionsToRemove = new ArrayList<>();
            
            activeSessions.forEach((sessionId, session) -> {
                boolean shouldRemove = session.completed || session.startTime < cutoff;
                
                // Also check if response is closed for EnhancedStreamingSession
                if (!shouldRemove && session instanceof EnhancedStreamingSession) {
                    EnhancedStreamingSession enhancedSession = (EnhancedStreamingSession) session;
                    shouldRemove = enhancedSession.responseEnded || 
                                   (enhancedSession.response != null && enhancedSession.response.closed());
                }
                
                if (shouldRemove) {
                    sessionsToRemove.add(sessionId);
                }
            });
            
            // Clean up identified sessions
            sessionsToRemove.forEach(this::cleanupSession);
            
            // Log cleanup statistics
            if (!sessionsToRemove.isEmpty()) {
                if (logLevel >= 3) vertx.eventBus().publish("log", "Session cleanup: removed " + sessionsToRemove.size() + " sessions; active: " + activeSessions.size() + ",3,ConversationStreaming,Session,Cleanup");
            }
        });
    }
    
    /**
     * Get the router for mounting in main HTTP server
     */
    public Router getRouter() {
        return router;
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        activeSessions.clear();
        requestMetrics.clear();
        hostAvailability.clear();
        
        vertx.eventBus().publish("log", "ConversationStreaming API stopped,2,ConversationStreaming,API,System");
        stopPromise.complete();
    }
}