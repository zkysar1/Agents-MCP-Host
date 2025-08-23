package AgentsMCPHost.api;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.RoutingContext;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles Server-Sent Events (SSE) streaming for conversation API.
 * Streams tool call notifications and responses in real-time.
 * Uses Vert.x concurrency primitives exclusively - no Java concurrent utilities.
 */
public class StreamingConversationHandler {
    
    private static final long HEARTBEAT_INTERVAL = 8000; // 8 seconds - keeps connection alive
    private static final long TIMEOUT_MS = 60000; // 60 seconds - longer for complex queries
    private static final long PROGRESS_UPDATE_INTERVAL = 5000; // 5 seconds
    
    /**
     * Handle a streaming conversation request
     * @param ctx The routing context
     * @param messages The conversation messages
     * @param vertx The Vertx instance
     */
    public static void handle(RoutingContext ctx, JsonArray messages, Vertx vertx) {
        HttpServerResponse response = ctx.response();
        EventBus eventBus = vertx.eventBus();
        String streamId = "stream-" + UUID.randomUUID().toString();
        
        // Use Vert.x SharedData for thread-safe state management
        LocalMap<String, String> streamState = vertx.sharedData().getLocalMap("stream-states");
        streamState.put(streamId, "active");
        
        // Set up SSE headers
        response.putHeader("Content-Type", "text/event-stream");
        response.putHeader("Cache-Control", "no-cache");
        response.putHeader("Connection", "keep-alive");
        response.putHeader("Access-Control-Allow-Origin", "*");
        response.setChunked(true);
        
        // Store cleanup handlers in context
        List<MessageConsumer<?>> consumers = new ArrayList<>();
        ctx.put("consumers", consumers);
        ctx.put("streamId", streamId);
        
        // Handle connection close using Vert.x handlers
        response.closeHandler(v -> {
            streamState.put(streamId, "closed");
            cleanupStream(streamId, vertx, ctx);
        });
        
        response.exceptionHandler(error -> {
            System.err.println("SSE connection error: " + error.getMessage());
            streamState.put(streamId, "error");
            cleanupStream(streamId, vertx, ctx);
        });
        
        // Set up heartbeat timer to keep connection alive
        Long heartbeatTimerId = vertx.setPeriodic(HEARTBEAT_INTERVAL, id -> {
            String state = streamState.get(streamId);
            if ("active".equals(state)) {
                sendEvent(response, "heartbeat", new JsonObject()
                    .put("timestamp", System.currentTimeMillis())
                    .put("message", "Connection alive"));
            } else {
                // Cancel heartbeat if stream is no longer active
                vertx.cancelTimer(id);
            }
        });
        ctx.put("heartbeatTimerId", heartbeatTimerId);
        
        // Set up timeout timer
        Long timeoutTimerId = vertx.setTimer(TIMEOUT_MS, id -> {
            String state = streamState.get(streamId);
            if ("active".equals(state) && !response.ended()) {
                sendEvent(response, "timeout", new JsonObject()
                    .put("message", "Operation timed out after " + TIMEOUT_MS + "ms"));
                streamState.put(streamId, "timeout");
                response.end();
                cleanupStream(streamId, vertx, ctx);
            }
        });
        ctx.put("timeoutTimerId", timeoutTimerId);
        
        // Send initial connection event
        sendEvent(response, "connected", new JsonObject()
            .put("streamId", streamId)
            .put("message", "Connected to conversation stream"));
        
        // Register event consumers for tool notifications
        MessageConsumer<JsonObject> toolStartConsumer = eventBus.consumer("conversation." + streamId + ".tool.start", msg -> {
            String state = streamState.get(streamId);
            if ("active".equals(state)) {
                JsonObject data = msg.body();
                sendEvent(response, "tool_call_start", new JsonObject()
                    .put("tool", data.getString("tool"))
                    .put("message", "[TOOL] Calling tool: " + data.getString("tool") + "..."));
            }
        });
        consumers.add(toolStartConsumer);
        
        MessageConsumer<JsonObject> toolCompleteConsumer = eventBus.consumer("conversation." + streamId + ".tool.complete", msg -> {
            String state = streamState.get(streamId);
            if ("active".equals(state)) {
                JsonObject data = msg.body();
                sendEvent(response, "tool_call_complete", new JsonObject()
                    .put("tool", data.getString("tool"))
                    .put("result", data.getValue("result"))
                    .put("message", "[OK] Tool completed: " + data.getString("tool")));
            }
        });
        consumers.add(toolCompleteConsumer);
        
        // Add progress update consumer
        MessageConsumer<JsonObject> progressConsumer = eventBus.consumer("conversation." + streamId + ".progress", msg -> {
            String state = streamState.get(streamId);
            System.out.println("[StreamingHandler] Received progress event for stream: " + streamId + ", state: " + state);
            if ("active".equals(state)) {
                JsonObject data = msg.body();
                System.out.println("[StreamingHandler] Sending progress to client: " + data.getString("step"));
                sendEvent(response, "progress", new JsonObject()
                    .put("step", data.getString("step"))
                    .put("message", data.getString("message"))
                    .put("elapsed", data.getLong("elapsed", 0L)));
            } else {
                System.out.println("[StreamingHandler] Ignoring progress event - stream not active: " + state);
            }
        });
        consumers.add(progressConsumer);
        
        MessageConsumer<JsonObject> finalResponseConsumer = eventBus.consumer("conversation." + streamId + ".final", msg -> {
            String state = streamState.get(streamId);
            if ("active".equals(state)) {
                JsonObject data = msg.body();
                sendEvent(response, "final_response", new JsonObject()
                    .put("content", data.getString("content"))
                    .put("done", true));
                
                // Mark as completed and close after short delay
                streamState.put(streamId, "completed");
                
                // Close the stream after final response
                vertx.setTimer(100, id -> {
                    if (!response.ended()) {
                        sendEvent(response, "done", new JsonObject().put("message", "Stream complete"));
                        response.end();
                    }
                    cleanupStream(streamId, vertx, ctx);
                });
            }
        });
        consumers.add(finalResponseConsumer);
        
        MessageConsumer<JsonObject> errorConsumer = eventBus.consumer("conversation." + streamId + ".error", msg -> {
            String state = streamState.get(streamId);
            if ("active".equals(state)) {
                JsonObject data = msg.body();
                sendEvent(response, "error", new JsonObject()
                    .put("error", data.getString("error"))
                    .put("message", "[ERROR] Error: " + data.getString("error")));
                
                streamState.put(streamId, "error");
                if (!response.ended()) {
                    response.end();
                }
                cleanupStream(streamId, vertx, ctx);
            }
        });
        consumers.add(errorConsumer);
        
        // Process the conversation with streaming context
        processStreamingConversation(messages, streamId, eventBus);
    }
    
    /**
     * Send an SSE event safely with synchronization
     * @param response The HTTP response
     * @param eventType The event type
     * @param data The event data
     */
    private static void sendEvent(HttpServerResponse response, String eventType, JsonObject data) {
        // Synchronize on the response object to prevent concurrent writes
        synchronized (response) {
            // Check if response is still writable
            if (!response.ended() && !response.closed()) {
                try {
                    StringBuilder event = new StringBuilder();
                    event.append("event: ").append(eventType).append("\n");
                    event.append("data: ").append(data.encode()).append("\n\n");
                    response.write(event.toString());
                } catch (Exception e) {
                    // Response was closed during write, ignore
                    System.err.println("SSE write failed (connection likely closed): " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Process conversation with streaming context
     * @param messages The conversation messages
     * @param streamId The stream identifier
     * @param eventBus The event bus
     */
    private static void processStreamingConversation(JsonArray messages, String streamId, EventBus eventBus) {
        // Extract last user message for tool detection
        String lastUserMessage = extractLastUserMessage(messages);
        
        // Create a request with stream context
        JsonObject request = new JsonObject()
            .put("messages", messages)
            .put("streamId", streamId)
            .put("userMessage", lastUserMessage);
        
        // Send to conversation processor with streaming context
        eventBus.request("conversation.process.streaming", request, ar -> {
            if (ar.succeeded()) {
                // Request acknowledged - processing continues asynchronously
                JsonObject reply = (JsonObject) ar.result().body();
                System.out.println("[StreamingHandler] Conversation processor acknowledged: " + reply.getString("message"));
                // Progress updates and final response will come via event bus
            } else {
                // Request failed - notify client
                System.err.println("[StreamingHandler] Conversation processor failed: " + ar.cause().getMessage());
                eventBus.publish("conversation." + streamId + ".error", 
                    new JsonObject().put("error", ar.cause().getMessage()));
            }
        });
    }
    
    /**
     * Extract last user message from conversation
     * @param messages The messages array
     * @return The last user message content
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
     * Clean up stream resources using Vert.x patterns
     * @param streamId The stream identifier
     * @param vertx The Vertx instance
     * @param ctx The routing context
     */
    private static void cleanupStream(String streamId, Vertx vertx, RoutingContext ctx) {
        // Clean up state
        LocalMap<String, String> streamState = vertx.sharedData().getLocalMap("stream-states");
        streamState.remove(streamId);
        
        // Cancel timers if they exist
        Long heartbeatTimerId = ctx.get("heartbeatTimerId");
        if (heartbeatTimerId != null) {
            vertx.cancelTimer(heartbeatTimerId);
        }
        
        Long timeoutTimerId = ctx.get("timeoutTimerId");
        if (timeoutTimerId != null) {
            vertx.cancelTimer(timeoutTimerId);
        }
        
        // Unregister all consumers
        List<MessageConsumer<?>> consumers = ctx.get("consumers");
        if (consumers != null) {
            consumers.forEach(MessageConsumer::unregister);
        }
        
        // Notify that stream is closing
        vertx.eventBus().publish("conversation.stream.closed", new JsonObject()
            .put("streamId", streamId));
    }
}