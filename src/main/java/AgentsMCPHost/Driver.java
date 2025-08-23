package AgentsMCPHost;

import AgentsMCPHost.api.*;
import AgentsMCPHost.mcp.core.McpHostManager;
import AgentsMCPHost.mcp.servers.oracle.orchestration.OracleOrchestrationStrategy;
import AgentsMCPHost.mcp.core.orchestration.ToolSelection;
import AgentsMCPHost.llm.LlmAPIService;
import AgentsMCPHost.logging.Logger;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;

public class Driver {
  public static int logLevel = 3; // 0=errors, 1=info, 2=detail, 3=debug, 4=data
  public static Vertx vertx = Vertx.vertx(new VertxOptions()
      .setWorkerPoolSize(4)
      .setEventLoopPoolSize(1)
  );

  private static final String DATA_PATH = "./data";
  public static final String zakAgentPath = DATA_PATH + "/agent";
  
  // Track component readiness via events (not polling)
  private boolean mcpSystemReady = false;
  private boolean orchestrationReady = false;

  public static void main(String[] args) {
    Driver me = new Driver();
    me.doIt();
  }

  private void doIt() {
    // Log startup information
    System.out.println("=== ZAK-Agent Starting ===");
    System.out.println("Java version: " + System.getProperty("java.version"));
    System.out.println("Working directory: " + System.getProperty("user.dir"));
    System.out.println("Data path: " + DATA_PATH);
    System.out.println("ZAK Agent path: " + zakAgentPath);
    
    // Set up event listeners for component readiness
    setupReadinessListeners();
    
    // Deploy verticles directly - no pre-warming needed
    deployVerticles();
  }
  
  private void deployVerticles() {
    // Deploy core verticles
    setLogger();
    setHostAPI();
    setHealth();
    setStatus();
    setConversation();
    
    // Deploy unified tool selection (must be before conversation processing)
    setToolSelection();
    
    // Initialize services
    setLlmAPIService();
    
    // Deploy MCP infrastructure (manages servers and clients)
    setMcpHostManager();
    
    // Oracle Tools Server and Client are now deployed by McpHostManager via mcp-config.json
    // This ensures proper registration and systemReady flag is set correctly
    // setOracleToolsServer();
    // setOracleToolsClient();
    
    // Deploy Oracle Orchestration Strategy (replaces monolithic agent loop)
    setOracleOrchestrationStrategy();
  }
  
  private void setupReadinessListeners() {
    // Listen for MCP system ready event
    vertx.eventBus().consumer("mcp.system.ready", msg -> {
      JsonObject status = (JsonObject) msg.body();
      mcpSystemReady = true;
      System.out.println("MCP System Ready - Servers: " + status.getInteger("servers", 0) + 
                       ", Clients: " + status.getInteger("clients", 0) + 
                       ", Tools: " + status.getInteger("tools", 0));
      checkSystemReady();
    });
    
    // Listen for orchestration ready event
    vertx.eventBus().consumer("oracle.orchestration.ready", msg -> {
      orchestrationReady = true;
      System.out.println("Oracle Orchestration Strategy Ready");
      checkSystemReady();
    });
  }
  
  private void checkSystemReady() {
    // Check if all critical components are ready
    if (mcpSystemReady && orchestrationReady) {
      System.out.println("=== ZAK-Agent Started ===");
      System.out.println("MCP Infrastructure: READY");
      System.out.println("Orchestration Strategies: READY");
      System.out.println("Unified Tool Architecture: ACTIVE");
      vertx.eventBus().publish("log", "ZAK-Agent startup complete,0,Driver,StartUp,System");
      
      // Publish the final system ready event that triggers HTTP server start
      vertx.eventBus().publish("system.fully.ready", new JsonObject()
        .put("mcp", mcpSystemReady)
        .put("orchestration", orchestrationReady)
        .put("timestamp", System.currentTimeMillis()));
      
      System.out.println("[Driver] Published system.fully.ready - HTTP server will now start accepting requests");
    } else {
      System.out.println("[Driver] Waiting for components - MCP: " + mcpSystemReady + ", Orchestration: " + orchestrationReady);
    }
  }

  private void setLogger() {
    vertx.deployVerticle(new Logger(), res -> {
      if (res.succeeded()) {
        System.out.println("Logger Verticle initialized successfully");
        if (logLevel >= 3) vertx.eventBus().publish("log", "Logger deployed,3,Driver,StartUp,System");
      } else {
        System.err.println("Logger deployment failed: " + res.cause().getMessage());
      }
    });
  }

  private void setHostAPI() {
    // Deploy the host API
    vertx.deployVerticle(new HostAPI(), res -> {
      if (res.succeeded()) {
        System.out.println("HostAPI Verticle initialized successfully");
        if (logLevel >= 3) vertx.eventBus().publish("log", "Host API verticle deployed,3,Driver,StartUp,System");
      } else {
        System.err.println("Host API deployment failed: " + res.cause().getMessage());
      }
    });
  }
  
  private void setHealth() {
    // Deploy the health endpoint
    vertx.deployVerticle(new Health(), res -> {
      if (res.succeeded()) {
        System.out.println("Health Verticle initialized successfully");
        if (logLevel >= 3) vertx.eventBus().publish("log", "Health verticle deployed,3,Driver,StartUp,System");
      } else {
        System.err.println("Health verticle deployment failed: " + res.cause().getMessage());
      }
    });
  }
  
  private void setStatus() {
    // Deploy the status endpoint
    vertx.deployVerticle(new Status(), res -> {
      if (res.succeeded()) {
        System.out.println("Status Verticle initialized successfully");
        if (logLevel >= 3) vertx.eventBus().publish("log", "Status verticle deployed,3,Driver,StartUp,System");
      } else {
        System.err.println("Status verticle deployment failed: " + res.cause().getMessage());
      }
    });
  }
  
  private void setConversation() {
    // Deploy the unified conversation API with auto MCP tool detection
    vertx.deployVerticle(new Conversation(), res -> {
      if (res.succeeded()) {
        System.out.println("Conversation Verticle initialized successfully");
        if (logLevel >= 3) vertx.eventBus().publish("log", "Unified conversation verticle with MCP deployed,3,Driver,StartUp,System");
      } else {
        System.err.println("Conversation verticle deployment failed: " + res.cause().getMessage());
      }
    });
  }
  
  private void setToolSelection() {
    // Deploy the unified tool selection for intelligent tool routing
    vertx.deployVerticle(new ToolSelection(), res -> {
      if (res.succeeded()) {
        System.out.println("Tool Selection Verticle initialized successfully");
        System.out.println("Unified tool selection with LLM validation enabled");
        if (logLevel >= 3) vertx.eventBus().publish("log", "Tool Selection Verticle deployed,3,Driver,StartUp,System");
      } else {
        System.err.println("Tool Selection verticle deployment failed: " + res.cause().getMessage());
        System.err.println("WARNING: Falling back to pattern-based tool selection");
      }
    });
  }
  
  private void setLlmAPIService() {
    // Initialize the LLM API service
    boolean initialized = LlmAPIService.getInstance().setupService(vertx);
    
    if (initialized) {
      System.out.println("OpenAI API service initialized successfully");
      if (logLevel >= 3) vertx.eventBus().publish("log", "LLM API service initialized,3,Driver,StartUp,System");
    } else {
      System.out.println("WARNING: OpenAI API service not initialized (missing API key)");
      System.out.println("Set OPENAI_API_KEY environment variable to enable LLM responses");
    }
  }
  
  private void setMcpHostManager() {
    // Deploy MCP Host Manager which orchestrates all MCP servers and clients
    System.out.println("Deploying MCP infrastructure...");
    
    vertx.deployVerticle(new McpHostManager(), res -> {
      if (res.succeeded()) {
        System.out.println("MCP Host Manager deployed successfully");
        if (logLevel >= 3) vertx.eventBus().publish("log", "MCP Host Manager deployed,3,Driver,StartUp,MCP");
      } else {
        System.err.println("MCP Host Manager deployment failed: " + res.cause().getMessage());
        System.err.println("MCP tools will not be available");
      }
    });
  }
  
  private void setOracleOrchestrationStrategy() {
    // Deploy Oracle Orchestration Strategy - coordinates tool calls
    System.out.println("Deploying Oracle Orchestration Strategy...");
    
    vertx.deployVerticle(new OracleOrchestrationStrategy(), res -> {
      if (res.succeeded()) {
        System.out.println("Oracle Orchestration deployed - pure coordination, no hidden logic");
        if (logLevel >= 3) vertx.eventBus().publish("log", "Oracle Orchestration deployed,3,Driver,StartUp,Orchestration");
      } else {
        System.err.println("Oracle Orchestration deployment failed: " + res.cause().getMessage());
        System.err.println("Complex query orchestration will not be available");
      }
    });
  }
}