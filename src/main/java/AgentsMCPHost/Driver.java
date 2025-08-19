package AgentsMCPHost;

import AgentsMCPHost.hostAPI.*;
import AgentsMCPHost.mcp.host.McpHostManagerVerticle;
import AgentsMCPHost.mcp.orchestration.OracleAgentLoop;
import AgentsMCPHost.services.LlmAPIService;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;

public class Driver {
  public static int logLevel = 3; // 0=errors, 1=info, 2=detail, 3=debug, 4=data
  public static Vertx vertx = Vertx.vertx(new VertxOptions()
      .setWorkerPoolSize(4)
      .setEventLoopPoolSize(1)
  );


  public static final String awsRegion = System.getenv("AWS_REGION") != null ?
    System.getenv("AWS_REGION") : "us-east-2";
  private static final String DATA_PATH = System.getenv("DATA_PATH") != null ?
    System.getenv("DATA_PATH") : "./data";
  
  public static final String zakAgentPath = DATA_PATH + "/agent";

  public static String instanceId = "local-instance";
  
  // Track component readiness via events (not polling)
  private boolean mcpSystemReady = false;
  private boolean oracleAgentReady = false;

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
    
    // Deploy core verticles
    setLoggerVerticle();
    setHostAPIVerticle();
    setHealthVerticle();
    setStatusVerticle();
    setConversationVerticle();
    
    // Initialize services
    setLlmAPIService();
    
    // Deploy MCP infrastructure (manages servers and clients)
    setMcpHostManager();
    
    // Deploy Oracle Agent Loop for interactive discovery
    setOracleAgentLoop();
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
    
    // Listen for Oracle Agent ready event
    vertx.eventBus().consumer("oracle.agent.ready", msg -> {
      oracleAgentReady = true;
      System.out.println("Oracle Agent Loop Ready");
      checkSystemReady();
    });
  }
  
  private void checkSystemReady() {
    // Check if all critical components are ready
    if (mcpSystemReady && oracleAgentReady) {
      System.out.println("=== ZAK-Agent Started ===");
      System.out.println("MCP Infrastructure: READY");
      System.out.println("Oracle Agent Loop: READY");
      vertx.eventBus().publish("log", "ZAK-Agent startup complete,0,Driver,StartUp,System");
      
      // Publish overall system ready event
      vertx.eventBus().publish("system.ready", new JsonObject()
        .put("mcp", mcpSystemReady)
        .put("oracle", oracleAgentReady)
        .put("timestamp", System.currentTimeMillis()));
    }
  }

  private void setLoggerVerticle() {
    vertx.deployVerticle(new LoggerVerticle(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 3) vertx.eventBus().publish("log", "Logger deployed,3,Driver,StartUp,System");
      } else {
        System.err.println("Logger deployment failed: " + res.cause().getMessage());
      }
    });
  }

  private void setHostAPIVerticle() {
    // Deploy the host API verticle
    vertx.deployVerticle(new HostAPIVerticle(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 3) vertx.eventBus().publish("log", "Host API verticle deployed,3,Driver,StartUp,System");
      } else {
        System.err.println("Host API deployment failed: " + res.cause().getMessage());
      }
    });
  }
  
  private void setHealthVerticle() {
    // Deploy the health endpoint verticle
    vertx.deployVerticle(new HealthVerticle(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 3) vertx.eventBus().publish("log", "Health verticle deployed,3,Driver,StartUp,System");
      } else {
        System.err.println("Health verticle deployment failed: " + res.cause().getMessage());
      }
    });
  }
  
  private void setStatusVerticle() {
    // Deploy the status endpoint verticle
    vertx.deployVerticle(new StatusVerticle(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 3) vertx.eventBus().publish("log", "Status verticle deployed,3,Driver,StartUp,System");
      } else {
        System.err.println("Status verticle deployment failed: " + res.cause().getMessage());
      }
    });
  }
  
  private void setConversationVerticle() {
    // Deploy the unified conversation API verticle with auto MCP tool detection
    vertx.deployVerticle(new ConversationVerticle(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 3) vertx.eventBus().publish("log", "Unified conversation verticle with MCP deployed,3,Driver,StartUp,System");
      } else {
        System.err.println("Conversation verticle deployment failed: " + res.cause().getMessage());
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
    
    vertx.deployVerticle(new McpHostManagerVerticle(), res -> {
      if (res.succeeded()) {
        System.out.println("MCP Host Manager deployed successfully");
        if (logLevel >= 3) vertx.eventBus().publish("log", "MCP Host Manager deployed,3,Driver,StartUp,MCP");
      } else {
        System.err.println("MCP Host Manager deployment failed: " + res.cause().getMessage());
        System.err.println("MCP tools will not be available");
      }
    });
  }
  
  private void setOracleAgentLoop() {
    // Deploy Oracle Agent Loop for interactive SQL discovery
    System.out.println("Deploying Oracle Agent Loop...");
    
    vertx.deployVerticle(new OracleAgentLoop(), res -> {
      if (res.succeeded()) {
        System.out.println("Oracle Agent Loop deployed successfully");
        if (logLevel >= 3) vertx.eventBus().publish("log", "Oracle Agent Loop deployed,3,Driver,StartUp,Oracle");
      } else {
        System.err.println("Oracle Agent Loop deployment failed: " + res.cause().getMessage());
        System.err.println("Interactive Oracle discovery will not be available");
      }
    });
  }
}