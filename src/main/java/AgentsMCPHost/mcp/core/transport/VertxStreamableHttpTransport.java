package AgentsMCPHost.mcp.core.transport;

import io.vertx.core.Vertx;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Vert.x-based implementation of streamable HTTP transport for MCP.
 * This adapter bridges Vert.x's async HTTP capabilities with MCP protocol requirements.
 */
public class VertxStreamableHttpTransport {
    
    private final Vertx vertx;
    private final WebClient webClient;
    private final String host;
    private final int port;
    private final String sessionId;
    private final Map<String, Consumer<JsonObject>> sseHandlers;
    
    // Protocol version as per MCP spec
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    
    /**
     * Create a new transport for client-side connections
     */
    public VertxStreamableHttpTransport(Vertx vertx, String host, int port) {
        this.vertx = vertx;
        this.host = host;
        this.port = port;
        this.sessionId = UUID.randomUUID().toString();
        this.sseHandlers = new ConcurrentHashMap<>();
        
        WebClientOptions options = new WebClientOptions()
            .setDefaultHost(host)
            .setDefaultPort(port)
            .setConnectTimeout(5000)
            .setIdleTimeout(30);
            
        this.webClient = WebClient.create(vertx, options);
    }
    
    /**
     * Send a JSON-RPC message to the server
     * @param message The JSON-RPC message to send
     * @return Future containing the response
     */
    public Future<JsonObject> sendMessage(JsonObject message) {
        Promise<JsonObject> promise = Promise.promise();
        
        webClient.post("/")
            .putHeader("Content-Type", "application/json")
            .putHeader("MCP-Protocol-Version", MCP_PROTOCOL_VERSION)
            .putHeader("Mcp-Session-Id", sessionId)
            .putHeader("Origin", "http://localhost:8080")
            .sendJsonObject(message, ar -> {
                if (ar.succeeded()) {
                    HttpResponse<Buffer> response = ar.result();
                    
                    if (response.statusCode() == 202 || response.statusCode() == 200) {
                        try {
                            JsonObject body = response.bodyAsJsonObject();
                            System.out.println("[VertxStreamableHttpTransport] Response: " + body.encodePrettily());
                            promise.complete(body);
                        } catch (Exception e) {
                            System.err.println("[VertxStreamableHttpTransport] Failed to parse response: " + response.bodyAsString());
                            promise.fail("Failed to parse response: " + e.getMessage());
                        }
                    } else if (response.statusCode() == 400) {
                        System.err.println("[VertxStreamableHttpTransport] Bad request (400): " + response.bodyAsString());
                        promise.fail("Bad request: " + response.bodyAsString());
                    } else {
                        System.err.println("[VertxStreamableHttpTransport] Unexpected status " + response.statusCode() + ": " + response.bodyAsString());
                        promise.fail("Unexpected status: " + response.statusCode());
                    }
                } else {
                    System.err.println("[VertxStreamableHttpTransport] Request failed: " + ar.cause().getMessage());
                    promise.fail(ar.cause());
                }
            });
            
        return promise.future();
    }
    
    /**
     * Start an SSE stream for server-initiated messages
     * @param handler Consumer to handle incoming SSE events
     * @return Future indicating stream setup success
     */
    public Future<Void> startSseStream(Consumer<JsonObject> handler) {
        Promise<Void> promise = Promise.promise();
        
        String streamId = UUID.randomUUID().toString();
        sseHandlers.put(streamId, handler);
        
        webClient.get("/")
            .putHeader("Accept", "text/event-stream")
            .putHeader("MCP-Protocol-Version", MCP_PROTOCOL_VERSION)
            .putHeader("Mcp-Session-Id", sessionId)
            .putHeader("Origin", "http://localhost:8080")
            .as(BodyCodec.string())
            .send(ar -> {
                if (ar.succeeded()) {
                    HttpResponse<String> response = ar.result();
                    
                    if (response.statusCode() == 200) {
                        // Parse SSE data
                        String body = response.body();
                        processSseData(body, handler);
                        promise.complete();
                    } else {
                        promise.fail("Failed to start SSE stream: " + response.statusCode());
                    }
                } else {
                    System.err.println("[VertxStreamableHttpTransport] Request failed: " + ar.cause().getMessage());
                    promise.fail(ar.cause());
                }
            });
            
        return promise.future();
    }
    
    /**
     * Process SSE data and call handler for each event
     */
    private void processSseData(String sseData, Consumer<JsonObject> handler) {
        if (sseData == null || sseData.isEmpty()) {
            return;
        }
        
        String[] lines = sseData.split("\n");
        StringBuilder eventData = new StringBuilder();
        
        for (String line : lines) {
            if (line.startsWith("data: ")) {
                String data = line.substring(6);
                eventData.append(data);
            } else if (line.isEmpty() && eventData.length() > 0) {
                // End of event, process it
                try {
                    JsonObject event = new JsonObject(eventData.toString());
                    handler.accept(event);
                } catch (Exception e) {
                    System.err.println("Failed to parse SSE event: " + e.getMessage());
                }
                eventData.setLength(0);
            }
        }
    }
    
    /**
     * Execute tools/list request
     */
    public Future<JsonArray> listTools() {
        JsonObject request = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("method", "tools/list")
            .put("id", UUID.randomUUID().toString())
            .put("params", new JsonObject());
            
        return sendMessage(request).map(response -> {
            if (response.containsKey("result")) {
                JsonObject result = response.getJsonObject("result");
                JsonArray tools = result.getJsonArray("tools", new JsonArray());
                System.out.println("[VertxStreamableHttpTransport] Received " + tools.size() + " tools from server");
                return tools;
            } else if (response.containsKey("error")) {
                JsonObject error = response.getJsonObject("error");
                System.err.println("[VertxStreamableHttpTransport] Error listing tools: " + error.getString("message"));
                return new JsonArray();
            }
            System.err.println("[VertxStreamableHttpTransport] Unexpected response format: " + response.encode());
            return new JsonArray();
        });
    }
    
    /**
     * Execute tools/call request
     */
    public Future<JsonObject> callTool(String toolName, JsonObject arguments) {
        JsonObject request = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("method", "tools/call")
            .put("id", UUID.randomUUID().toString())
            .put("params", new JsonObject()
                .put("name", toolName)
                .put("arguments", arguments));
                
        return sendMessage(request).map(response -> {
            if (response.containsKey("result")) {
                return response.getJsonObject("result");
            }
            return new JsonObject().put("error", "No result in response");
        });
    }
    
    /**
     * Initialize connection (protocol negotiation)
     */
    public Future<JsonObject> initialize() {
        JsonObject request = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("method", "initialize")
            .put("id", UUID.randomUUID().toString())
            .put("params", new JsonObject()
                .put("protocolVersion", MCP_PROTOCOL_VERSION)
                .put("capabilities", new JsonObject()
                    .put("tools", new JsonObject())
                    .put("resources", new JsonObject())
                    .put("prompts", new JsonObject())));
                    
        return sendMessage(request);
    }
    
    /**
     * Initialize connection with custom parameters (for FileSystemClient roots)
     */
    public Future<JsonObject> initializeWithParams(JsonObject params) {
        JsonObject request = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("method", "initialize")
            .put("id", UUID.randomUUID().toString())
            .put("params", params
                .put("protocolVersion", MCP_PROTOCOL_VERSION)
                .put("capabilities", new JsonObject()
                    .put("tools", new JsonObject())
                    .put("resources", new JsonObject()
                        .put("subscribe", true))
                    .put("roots", new JsonObject())
                    .put("prompts", new JsonObject())));
                    
        return sendMessage(request);
    }
    
    /**
     * List available resources
     */
    public Future<JsonArray> listResources() {
        JsonObject request = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("method", "resources/list")
            .put("id", UUID.randomUUID().toString())
            .put("params", new JsonObject());
            
        return sendMessage(request).map(response -> {
            if (response.containsKey("result")) {
                JsonObject result = response.getJsonObject("result");
                return result.getJsonArray("resources", new JsonArray());
            }
            return new JsonArray();
        });
    }
    
    /**
     * List allowed roots
     */
    public Future<JsonArray> listRoots() {
        JsonObject request = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("method", "roots/list")
            .put("id", UUID.randomUUID().toString())
            .put("params", new JsonObject());
            
        return sendMessage(request).map(response -> {
            if (response.containsKey("result")) {
                JsonObject result = response.getJsonObject("result");
                return result.getJsonArray("roots", new JsonArray());
            }
            return new JsonArray();
        });
    }
    
    /**
     * Close the transport connection
     */
    public void close() {
        webClient.close();
        sseHandlers.clear();
    }
    
    /**
     * Get the session ID
     */
    public String getSessionId() {
        return sessionId;
    }
}