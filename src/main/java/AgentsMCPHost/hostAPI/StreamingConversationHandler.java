package AgentsMCPHost.hostAPI;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles Server-Sent Events (SSE) streaming for conversation API.
 * Streams tool call notifications and responses in real-time.
 */
public class StreamingConversationHandler {
    
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
        AtomicBoolean connectionClosed = new AtomicBoolean(false);
        
        // Set up SSE headers
        response.putHeader("Content-Type", "text/event-stream");
        response.putHeader("Cache-Control", "no-cache");
        response.putHeader("Connection", "keep-alive");
        response.putHeader("Access-Control-Allow-Origin", "*");
        response.setChunked(true);
        
        // Handle connection close
        response.closeHandler(v -> {
            connectionClosed.set(true);
            cleanupStream(streamId, eventBus);
        });
        
        response.exceptionHandler(error -> {
            System.err.println("SSE connection error: " + error.getMessage());
            connectionClosed.set(true);
            cleanupStream(streamId, eventBus);
        });
        
        // Send initial connection event
        sendEvent(response, "connected", new JsonObject()
            .put("streamId", streamId)
            .put("message", "Connected to conversation stream"));
        
        // Register event consumers for tool notifications
        MessageConsumer<JsonObject> toolStartConsumer = eventBus.consumer("conversation." + streamId + ".tool.start", msg -> {
            if (!connectionClosed.get()) {
                JsonObject data = msg.body();
                sendEvent(response, "tool_call_start", new JsonObject()
                    .put("tool", data.getString("tool"))
                    .put("message", "Calling tool: " + data.getString("tool") + "..."));
            }
        });
        
        MessageConsumer<JsonObject> toolCompleteConsumer = eventBus.consumer("conversation." + streamId + ".tool.complete", msg -> {
            if (!connectionClosed.get()) {
                JsonObject data = msg.body();
                sendEvent(response, "tool_call_complete", new JsonObject()
                    .put("tool", data.getString("tool"))
                    .put("result", data.getValue("result"))
                    .put("message", "Tool completed: " + data.getString("tool")));
            }
        });
        
        MessageConsumer<JsonObject> finalResponseConsumer = eventBus.consumer("conversation." + streamId + ".final", msg -> {
            if (!connectionClosed.get()) {
                JsonObject data = msg.body();
                sendEvent(response, "final_response", new JsonObject()
                    .put("content", data.getString("content"))
                    .put("done", true));
                
                // Close the stream after final response
                vertx.setTimer(100, id -> {
                    if (!connectionClosed.get()) {
                        sendEvent(response, "done", new JsonObject().put("message", "Stream complete"));
                        response.end();
                        cleanupStream(streamId, eventBus);
                    }
                });
            }
        });
        
        MessageConsumer<JsonObject> errorConsumer = eventBus.consumer("conversation." + streamId + ".error", msg -> {
            if (!connectionClosed.get()) {
                JsonObject data = msg.body();
                sendEvent(response, "error", new JsonObject()
                    .put("error", data.getString("error"))
                    .put("message", "Error: " + data.getString("error")));
                response.end();
                cleanupStream(streamId, eventBus);
            }
        });
        
        // Store consumers for cleanup
        ctx.put("toolStartConsumer", toolStartConsumer);
        ctx.put("toolCompleteConsumer", toolCompleteConsumer);
        ctx.put("finalResponseConsumer", finalResponseConsumer);
        ctx.put("errorConsumer", errorConsumer);
        
        // Process the conversation with streaming context
        processStreamingConversation(messages, streamId, eventBus);
    }
    
    /**
     * Send an SSE event
     * @param response The HTTP response
     * @param eventType The event type
     * @param data The event data
     */
    private static void sendEvent(HttpServerResponse response, String eventType, JsonObject data) {
        StringBuilder event = new StringBuilder();
        event.append("event: ").append(eventType).append("\n");
        event.append("data: ").append(data.encode()).append("\n\n");
        response.write(event.toString());
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
            if (ar.failed()) {
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
     * Clean up stream resources
     * @param streamId The stream identifier
     * @param eventBus The event bus
     */
    private static void cleanupStream(String streamId, EventBus eventBus) {
        // Notify that stream is closing
        eventBus.publish("conversation.stream.closed", new JsonObject()
            .put("streamId", streamId));
    }
}