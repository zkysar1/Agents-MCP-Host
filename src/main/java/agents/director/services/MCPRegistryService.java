package agents.director.services;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import static agents.director.Driver.logLevel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Central registry service for tracking all MCP clients and their tools.
 * This service maintains a comprehensive view of the MCP ecosystem, including:
 * - All registered MCP clients across hosts
 * - Available tools and their associations
 * - Client health status
 * - Tool usage statistics
 * 
 * Provides event bus endpoints for API queries:
 * - mcp.status: Overall MCP system status
 * - mcp.tools.list: List of all available tools
 * - mcp.clients.list: List of all registered clients
 * - mcp.host.status: Aggregated host-level status
 */
public class MCPRegistryService extends AbstractVerticle {
    
    // Registry data structures
    private final Map<String, MCPClientInfo> clients = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> toolToClients = new ConcurrentHashMap<>();
    private final Map<String, ToolStatistics> toolStats = new ConcurrentHashMap<>();
    
    // Health check configuration
    private static final long HEALTH_CHECK_INTERVAL = 30000; // 30 seconds
    private static final long INACTIVE_THRESHOLD = 60000; // 60 seconds
    
    // Performance tracking
    private final AtomicInteger totalRegistrations = new AtomicInteger(0);
    private final AtomicInteger activeClients = new AtomicInteger(0);
    
    // Service start time
    private final long serviceStartTime = System.currentTimeMillis();
    
    private EventBus eventBus;
    
    // Inner class for client information
    private static class MCPClientInfo {
        String clientId;
        String serverName;
        String serverUrl;
        String eventBusAddress;
        long registrationTime;
        long lastHeartbeat;
        boolean active;
        JsonArray tools;
        Map<String, Object> metadata;
        
        MCPClientInfo(String clientId, String serverName, String serverUrl, String eventBusAddress) {
            this.clientId = clientId;
            this.serverName = serverName;
            this.serverUrl = serverUrl;
            this.eventBusAddress = eventBusAddress;
            this.registrationTime = System.currentTimeMillis();
            this.lastHeartbeat = System.currentTimeMillis();
            this.active = true;
            this.tools = new JsonArray();
            this.metadata = new HashMap<>();
        }
        
        JsonObject toJson() {
            return new JsonObject()
                .put("clientId", clientId)
                .put("serverName", serverName)
                .put("serverUrl", serverUrl)
                .put("eventBusAddress", eventBusAddress)
                .put("registrationTime", registrationTime)
                .put("lastHeartbeat", lastHeartbeat)
                .put("active", active)
                .put("uptime", System.currentTimeMillis() - registrationTime)
                .put("toolCount", tools.size())
                .put("metadata", metadata);
        }
    }
    
    // Inner class for tool statistics
    private static class ToolStatistics {
        final String toolName;
        final long firstSeen;
        final AtomicInteger totalCalls = new AtomicInteger(0);
        final AtomicInteger successfulCalls = new AtomicInteger(0);
        final AtomicInteger failedCalls = new AtomicInteger(0);
        final AtomicLong totalDuration = new AtomicLong(0);
        final AtomicLong lastUsed = new AtomicLong(0);
        
        ToolStatistics(String toolName) {
            this.toolName = toolName;
            this.firstSeen = System.currentTimeMillis();
        }
        
        void recordUsage(boolean success, long duration) {
            totalCalls.incrementAndGet();
            if (success) {
                successfulCalls.incrementAndGet();
            } else {
                failedCalls.incrementAndGet();
            }
            totalDuration.addAndGet(duration);
            lastUsed.set(System.currentTimeMillis());
        }
        
        JsonObject toJson() {
            int total = totalCalls.get();
            double avgDuration = total > 0 ? (double) totalDuration.get() / total : 0.0;
            double successRate = total > 0 ? (double) successfulCalls.get() / total * 100 : 0.0;
            
            return new JsonObject()
                .put("toolName", toolName)
                .put("firstSeen", firstSeen)
                .put("totalCalls", total)
                .put("successfulCalls", successfulCalls.get())
                .put("failedCalls", failedCalls.get())
                .put("successRate", successRate)
                .put("averageDuration", avgDuration)
                .put("lastUsed", lastUsed.get());
        }
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        eventBus = vertx.eventBus();
        
        // Register event bus consumers
        registerEventConsumers();
        
        // Start periodic health checks
        startHealthChecks();
        
        if (logLevel >= 1) vertx.eventBus().publish("log", "MCPRegistryService started - tracking MCP clients and tools,1,MCPRegistryService,Service,System");
        startPromise.complete();
    }
    
    private void registerEventConsumers() {
        // Client registration
        eventBus.<JsonObject>consumer("mcp.registry.register", this::handleClientRegistration);
        
        // Client deregistration
        eventBus.<JsonObject>consumer("mcp.registry.deregister", this::handleClientDeregistration);
        
        // Heartbeat
        eventBus.<JsonObject>consumer("mcp.registry.heartbeat", this::handleClientHeartbeat);
        
        // Tool discovery notification
        eventBus.<JsonObject>consumer("mcp.registry.tools.discovered", this::handleToolsDiscovered);
        
        // Tool usage statistics
        eventBus.<JsonObject>consumer("mcp.registry.tool.usage", this::handleToolUsage);
        
        // Status queries - These are what ConversationStreaming expects!
        eventBus.<JsonObject>consumer("mcp.status", this::handleMCPStatus);
        eventBus.<JsonObject>consumer("mcp.tools.list", this::handleToolsList);
        eventBus.<JsonObject>consumer("mcp.clients.list", this::handleClientsList);
        eventBus.<JsonObject>consumer("mcp.host.status", this::handleHostStatus);
        
        if (logLevel >= 3) vertx.eventBus().publish("log", "MCPRegistryService event consumers registered,3,MCPRegistryService,Service,System");
    }
    
    private void handleClientRegistration(Message<JsonObject> message) {
        JsonObject request = message.body();
        String clientId = request.getString("clientId");
        String serverName = request.getString("serverName");
        String serverUrl = request.getString("serverUrl");
        String eventBusAddress = request.getString("eventBusAddress");
        
        if (clientId == null || serverName == null || serverUrl == null) {
            message.fail(400, "Missing required fields: clientId, serverName, serverUrl");
            return;
        }
        
        // Create client info
        MCPClientInfo clientInfo = new MCPClientInfo(clientId, serverName, serverUrl, eventBusAddress);
        
        // Add any additional metadata
        JsonObject metadata = request.getJsonObject("metadata");
        if (metadata != null) {
            clientInfo.metadata.putAll(metadata.getMap());
        }
        
        // Register client
        clients.put(clientId, clientInfo);
        totalRegistrations.incrementAndGet();
        updateActiveClientCount();
        
        if (logLevel >= 2) vertx.eventBus().publish("log", "MCP client registered: " + serverName + " (" + clientId + "),2,MCPRegistryService,Registry,Client");
        
        // Publish registration event
        eventBus.publish("mcp.client.registered", new JsonObject()
            .put("clientId", clientId)
            .put("serverName", serverName)
            .put("timestamp", System.currentTimeMillis()));
        
        message.reply(new JsonObject()
            .put("status", "registered")
            .put("clientId", clientId));
    }
    
    private void handleClientDeregistration(Message<JsonObject> message) {
        String clientId = message.body().getString("clientId");
        
        if (clientId == null) {
            message.fail(400, "Missing clientId");
            return;
        }
        
        MCPClientInfo clientInfo = clients.remove(clientId);
        if (clientInfo != null) {
            // Remove from tool mappings
            if (clientInfo.tools != null) {
                for (Object toolObj : clientInfo.tools) {
                    JsonObject tool = (JsonObject) toolObj;
                    String toolName = tool.getString("name");
                    if (toolName != null) {
                        Set<String> clientSet = toolToClients.get(toolName);
                        if (clientSet != null) {
                            clientSet.remove(clientId);
                            if (clientSet.isEmpty()) {
                                toolToClients.remove(toolName);
                            }
                        }
                    }
                }
            }
            
            updateActiveClientCount();
            
            if (logLevel >= 2) vertx.eventBus().publish("log", "MCP client deregistered: " + clientInfo.serverName + " (" + clientId + "),2,MCPRegistryService,Registry,Client");
            
            // Publish deregistration event
            eventBus.publish("mcp.client.deregistered", new JsonObject()
                .put("clientId", clientId)
                .put("serverName", clientInfo.serverName)
                .put("timestamp", System.currentTimeMillis()));
        }
        
        message.reply(new JsonObject().put("status", "deregistered"));
    }
    
    private void handleClientHeartbeat(Message<JsonObject> message) {
        String clientId = message.body().getString("clientId");
        
        if (clientId == null) {
            message.fail(400, "Missing clientId");
            return;
        }
        
        MCPClientInfo clientInfo = clients.get(clientId);
        if (clientInfo != null) {
            synchronized (clientInfo) {
                clientInfo.lastHeartbeat = System.currentTimeMillis();
                if (!clientInfo.active) {
                    clientInfo.active = true;
                    updateActiveClientCount();
                    if (logLevel >= 3) vertx.eventBus().publish("log", "MCP client reactivated: " + clientInfo.serverName + ",3,MCPRegistryService,Registry,Health");
                }
            }
        }
        
        message.reply(new JsonObject().put("status", "acknowledged"));
    }
    
    private void handleToolsDiscovered(Message<JsonObject> message) {
        JsonObject request = message.body();
        String clientId = request.getString("clientId");
        JsonArray tools = request.getJsonArray("tools");
        
        if (clientId == null || tools == null) {
            message.fail(400, "Missing clientId or tools");
            return;
        }
        
        MCPClientInfo clientInfo = clients.get(clientId);
        if (clientInfo != null) {
            // First, remove client from old tool mappings
            if (clientInfo.tools != null) {
                for (Object oldToolObj : clientInfo.tools) {
                    JsonObject oldTool = (JsonObject) oldToolObj;
                    String oldToolName = oldTool.getString("name");
                    if (oldToolName != null) {
                        Set<String> clientSet = toolToClients.get(oldToolName);
                        if (clientSet != null) {
                            clientSet.remove(clientId);
                            if (clientSet.isEmpty()) {
                                toolToClients.remove(oldToolName);
                            }
                        }
                    }
                }
            }
            
            // Now update with new tools
            clientInfo.tools = tools;
            
            // Update tool to client mappings
            for (Object toolObj : tools) {
                JsonObject tool = (JsonObject) toolObj;
                String toolName = tool.getString("name");
                if (toolName != null) {
                    toolToClients.computeIfAbsent(toolName, k -> ConcurrentHashMap.newKeySet())
                        .add(clientId);
                    
                    // Initialize tool statistics
                    toolStats.computeIfAbsent(toolName, k -> new ToolStatistics(toolName));
                }
            }
            
            if (logLevel >= 2) vertx.eventBus().publish("log", "Tools discovered for " + clientInfo.serverName + ": " + tools.size() + " tools,2,MCPRegistryService,Registry,Tools");
            
            // Publish tools discovered event
            eventBus.publish("mcp.tools.discovered", new JsonObject()
                .put("clientId", clientId)
                .put("serverName", clientInfo.serverName)
                .put("toolCount", tools.size())
                .put("timestamp", System.currentTimeMillis()));
        }
        
        message.reply(new JsonObject().put("status", "tools_registered"));
    }
    
    private void handleToolUsage(Message<JsonObject> message) {
        JsonObject usage = message.body();
        String toolName = usage.getString("toolName");
        String clientId = usage.getString("clientId");
        boolean success = usage.getBoolean("success", true);
        long duration = usage.getLong("duration", 0L);
        
        // Validate client exists
        if (clientId != null && !clients.containsKey(clientId)) {
            message.fail(404, "Unknown client: " + clientId);
            return;
        }
        
        if (toolName != null) {
            ToolStatistics stats = toolStats.get(toolName);
            if (stats != null) {
                stats.recordUsage(success, duration);
            } else {
                // Tool not found in statistics - might be a new tool
                if (logLevel >= 3) vertx.eventBus().publish("log", "Tool usage reported for unknown tool: " + toolName + ",3,MCPRegistryService,Registry,Tools");
            }
        }
        
        message.reply(new JsonObject().put("status", "recorded"));
    }
    
    private void handleMCPStatus(Message<JsonObject> message) {
        if (message == null) {
            return;
        }
        
        long now = System.currentTimeMillis();
        
        // Calculate overall status
        int totalClients = clients.size();
        int activeCount = (int) clients.values().stream()
            .filter(c -> c.active)
            .count();
        int totalTools = toolToClients.size();
        
        JsonObject status = new JsonObject()
            .put("healthy", activeCount > 0)
            .put("totalClients", totalClients)
            .put("activeClients", activeCount)
            .put("inactiveClients", totalClients - activeCount)
            .put("totalTools", totalTools)
            .put("totalRegistrations", totalRegistrations.get())
            .put("uptime", now - serviceStartTime)
            .put("timestamp", now);
        
        // Add client breakdown by server type
        JsonObject clientBreakdown = new JsonObject();
        clients.values().stream()
            .collect(Collectors.groupingBy(c -> c.serverName))
            .forEach((serverName, clientList) -> {
                clientBreakdown.put(serverName, new JsonObject()
                    .put("total", clientList.size())
                    .put("active", clientList.stream().filter(c -> c.active).count()));
            });
        status.put("clientBreakdown", clientBreakdown);
        
        // Add health warnings if any
        JsonArray warnings = new JsonArray();
        if (activeCount == 0 && totalClients > 0) {
            warnings.add("All MCP clients are inactive");
        }
        if (totalClients == 0) {
            warnings.add("No MCP clients registered");
        }
        status.put("warnings", warnings);
        
        message.reply(status);
    }
    
    private void handleToolsList(Message<JsonObject> message) {
        JsonObject response = new JsonObject();
        JsonArray toolsArray = new JsonArray();
        
        // Get unique tools with their client associations and statistics
        toolToClients.forEach((toolName, clientIds) -> {
            JsonObject toolInfo = new JsonObject()
                .put("name", toolName)
                .put("availableIn", clientIds.size())
                .put("clients", new JsonArray(new ArrayList<>(clientIds)));
            
            // Add statistics if available
            ToolStatistics stats = toolStats.get(toolName);
            if (stats != null) {
                toolInfo.put("statistics", stats.toJson());
            }
            
            // Add client details
            JsonArray clientDetails = new JsonArray();
            for (String clientId : clientIds) {
                MCPClientInfo clientInfo = clients.get(clientId);
                if (clientInfo != null) {
                    clientDetails.add(new JsonObject()
                        .put("clientId", clientId)
                        .put("serverName", clientInfo.serverName)
                        .put("active", clientInfo.active));
                }
            }
            toolInfo.put("clientDetails", clientDetails);
            
            toolsArray.add(toolInfo);
        });
        
        response.put("tools", toolsArray)
                .put("totalTools", toolsArray.size())
                .put("timestamp", System.currentTimeMillis());
        
        message.reply(response);
    }
    
    private void handleClientsList(Message<JsonObject> message) {
        JsonObject response = new JsonObject();
        JsonArray clientsArray = new JsonArray();
        
        // Convert all clients to JSON
        clients.values().forEach(clientInfo -> {
            JsonObject clientJson = clientInfo.toJson();
            
            // Add tool names
            JsonArray toolNames = new JsonArray();
            if (clientInfo.tools != null) {
                for (Object toolObj : clientInfo.tools) {
                    JsonObject tool = (JsonObject) toolObj;
                    toolNames.add(tool.getString("name", "unknown"));
                }
            }
            clientJson.put("toolNames", toolNames);
            
            clientsArray.add(clientJson);
        });
        
        response.put("clients", clientsArray)
                .put("totalClients", clientsArray.size())
                .put("activeClients", activeClients.get())
                .put("timestamp", System.currentTimeMillis());
        
        message.reply(response);
    }
    
    private void handleHostStatus(Message<JsonObject> message) {
        // Aggregate status by host type
        Map<String, JsonObject> hostStatus = new HashMap<>();
        
        // Group clients by their host type (extracted from serverName)
        clients.values().stream()
            .collect(Collectors.groupingBy(c -> extractHostType(c.serverName)))
            .forEach((hostType, clientList) -> {
                int active = (int) clientList.stream().filter(c -> c.active).count();
                int total = clientList.size();
                
                // Collect all tools for this host type
                Set<String> hostTools = new HashSet<>();
                for (MCPClientInfo client : clientList) {
                    if (client.tools != null) {
                        for (Object toolObj : client.tools) {
                            JsonObject tool = (JsonObject) toolObj;
                            String toolName = tool.getString("name");
                            if (toolName != null) {
                                hostTools.add(toolName);
                            }
                        }
                    }
                }
                
                hostStatus.put(hostType, new JsonObject()
                    .put("hostType", hostType)
                    .put("totalClients", total)
                    .put("activeClients", active)
                    .put("healthy", active > 0)
                    .put("totalTools", hostTools.size())
                    .put("tools", new JsonArray(new ArrayList<>(hostTools))));
            });
        
        JsonObject response = new JsonObject()
            .put("hosts", hostStatus)
            .put("totalHosts", hostStatus.size())
            .put("timestamp", System.currentTimeMillis());
        
        message.reply(response);
    }
    
    // Constants for host types
    private static final String HOST_TYPE_ORACLE = "oracle";
    private static final String HOST_TYPE_INTENT = "intent";
    private static final String HOST_TYPE_STRATEGY = "strategy";
    private static final String HOST_TYPE_BUSINESS = "business";
    private static final String HOST_TYPE_OTHER = "other";
    
    private String extractHostType(String serverName) {
        // Extract host type from server name
        // e.g., "OracleQueryAnalysis" -> "oracle"
        // "QueryIntentEvaluation" -> "intent"
        // "StrategyGeneration" -> "strategy"
        
        String lowerName = serverName.toLowerCase();
        
        if (lowerName.contains("oracle")) {
            return HOST_TYPE_ORACLE;
        } else if (lowerName.contains("intent")) {
            return HOST_TYPE_INTENT;
        } else if (lowerName.contains("strategy")) {
            return HOST_TYPE_STRATEGY;
        } else if (lowerName.contains("business")) {
            return HOST_TYPE_BUSINESS;
        }
        return HOST_TYPE_OTHER;
    }
    
    private void startHealthChecks() {
        // Periodic health check
        vertx.setPeriodic(HEALTH_CHECK_INTERVAL, id -> {
            long now = System.currentTimeMillis();
            List<String> inactiveClients = new ArrayList<>();
            
            // Use a snapshot of client IDs to avoid concurrent modification
            Set<String> clientIds = new HashSet<>(clients.keySet());
            
            for (String clientId : clientIds) {
                MCPClientInfo clientInfo = clients.get(clientId);
                if (clientInfo != null) {
                    // Synchronize on clientInfo for thread safety
                    synchronized (clientInfo) {
                        if (clientInfo.active && (now - clientInfo.lastHeartbeat) > INACTIVE_THRESHOLD) {
                            clientInfo.active = false;
                            inactiveClients.add(clientId);
                        }
                    }
                }
            }
            
            if (!inactiveClients.isEmpty()) {
                updateActiveClientCount();
                
                for (String clientId : inactiveClients) {
                    MCPClientInfo clientInfo = clients.get(clientId);
                    if (clientInfo != null) {
                        if (logLevel >= 2) vertx.eventBus().publish("log", "MCP client inactive: " + clientInfo.serverName + " (no heartbeat for " + 
                            ((now - clientInfo.lastHeartbeat) / 1000) + "s),2,MCPRegistryService,Registry,Health");
                        
                        // Publish inactive event
                        eventBus.publish("mcp.client.inactive", new JsonObject()
                            .put("clientId", clientId)
                            .put("serverName", clientInfo.serverName)
                            .put("lastHeartbeat", clientInfo.lastHeartbeat)
                            .put("timestamp", now));
                    }
                }
            }
        });
    }
    
    private void updateActiveClientCount() {
        activeClients.set((int) clients.values().stream()
            .filter(c -> c.active)
            .count());
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        if (logLevel >= 1) vertx.eventBus().publish("log", "MCPRegistryService stopping,1,MCPRegistryService,Service,System");
        clients.clear();
        toolToClients.clear();
        toolStats.clear();
        stopPromise.complete();
    }
}