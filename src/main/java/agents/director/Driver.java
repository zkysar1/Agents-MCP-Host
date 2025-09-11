package agents.director;

import agents.director.services.MCPRouterService;
import agents.director.services.MCPRegistryService;
import agents.director.services.LlmAPIService;
import agents.director.services.Logger;
import agents.director.services.OracleConnectionManager;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;

public class Driver {
  public static int logLevel = 3; // 0=errors, 1=info, 2=detail, 3=debug, 4=data
  public static Vertx vertx = Vertx.vertx(new VertxOptions()
      .setWorkerPoolSize(4)
      .setEventLoopPoolSize(1)
  );

  private static final String DATA_PATH = "./data";
  public static final String zakAgentPath = DATA_PATH + "/agent";
  public static final String BASE_URL = "http://localhost:8080";
  
  // Track component readiness via events
  private boolean mcpRouterReady = false;
  private boolean mcpServersReady = false;
  private boolean hostsReady = false;

  public static void main(String[] args) {
    Driver me = new Driver();
    me.doIt();
  }

  private void doIt() {
    // Log startup information - Keep as console output (critical startup info)
    System.out.println("=== MCP-Based Agent System Starting ===");
    System.out.println("Java version: " + System.getProperty("java.version"));
    System.out.println("Working directory: " + System.getProperty("user.dir"));
    System.out.println("Data path: " + DATA_PATH);
    System.out.println("Agent path: " + zakAgentPath);
    
    // Add shutdown hook to properly clean up Oracle connection pools
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("[Driver] Shutting down Oracle connection pools...");
      try {
        UniversalConnectionPoolManager mgr = 
            UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
        String[] poolNames = mgr.getConnectionPoolNames();
        if (poolNames != null && poolNames.length > 0) {
          for (String poolName : poolNames) {
            System.out.println("[Driver] Destroying pool: " + poolName);
            try {
              mgr.destroyConnectionPool(poolName);
              System.out.println("[Driver] Successfully destroyed pool: " + poolName);
            } catch (Exception e) {
              System.err.println("[Driver] Error destroying pool " + poolName + ": " + e.getMessage());
            }
          }
        } else {
          System.out.println("[Driver] No Oracle connection pools found to destroy");
        }
      } catch (Exception e) {
        System.err.println("[Driver] Error in shutdown hook: " + e.getMessage());
      }
    }));
    
    // Set up event listeners for component readiness
    setupReadinessListeners();
    
    // Start deployment sequence: Router first, then servers, then hosts
    deployMCPRouter();
  }
  
  // New deployment methods following MCP architecture
  private void deployMCPRouter() {
    if (logLevel >= 1) vertx.eventBus().publish("log", "Deploying MCP Router Service...,1,Driver,StartUp,MCP");
    
    vertx.deployVerticle(new MCPRouterService(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 1) vertx.eventBus().publish("log", "MCPRouterService deployed successfully,1,Driver,StartUp,MCP");
        if (logLevel >= 3) vertx.eventBus().publish("log", "MCP Router deployed,3,Driver,StartUp,MCP");
      } else {
        // Fatal error - keep console output
        vertx.eventBus().publish("log", "MCPRouterService deployment failed: " + res.cause().getMessage() + ",0,Driver,System,System");
        vertx.eventBus().publish("log", "MCPRouterService deployment failed: " + res.cause().getMessage() + ",0,Driver,StartUp,MCP");
        res.cause().printStackTrace();
        System.exit(1); // Fatal error - cannot continue without router
      }
    });
  }
  
  private void deployMCPServers() {
    if (logLevel >= 1) vertx.eventBus().publish("log", "Deploying MCP Servers...,1,Driver,StartUp,MCP");
    
    // Initialize OracleConnectionManager first, then deploy all servers
    System.out.println("[Driver] Starting OracleConnectionManager initialization...");
    OracleConnectionManager.getInstance().initialize(vertx).onComplete(ar -> {
      if (ar.succeeded()) {
        System.out.println("[Driver] OracleConnectionManager initialization complete");
        if (logLevel >= 1) vertx.eventBus().publish("log", "Oracle Connection Manager initialized,1,Driver,StartUp,Database");
      } else {
        System.out.println("[Driver] OracleConnectionManager initialization failed: " + ar.cause().getMessage());
        vertx.eventBus().publish("log", "Failed to initialize Oracle Connection Manager: " + ar.cause().getMessage() + ",0,Driver,StartUp,Database");
      }
      
      // Deploy all servers AFTER Oracle initialization completes (whether success or failure)
      deployAllMCPServers();
    });
  }
  
  private void deployAllMCPServers() {
    System.out.println("[Driver] Deploying all MCP servers...");
    
    // Import the server classes
    List<Future> deploymentFutures = new ArrayList<>();
    
    int serverCount = 0;

    // Deploy OracleQueryExecutionServer (Worker)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": OracleQueryExecutionServer");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleQueryExecutionServer",
                    new DeploymentOptions().setWorker(true).setWorkerPoolSize(1) // Reduced from 5
            )
    );

    // Deploy OracleQueryAnalysisServer (Worker)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": OracleQueryAnalysisServer");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleQueryAnalysisServer",
                    new DeploymentOptions().setWorker(true).setWorkerPoolSize(1)
            )
    );

    // Deploy OracleSchemaIntelligenceServer (Worker - with extended timeout for schema loading)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": OracleSchemaIntelligenceServer");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleSchemaIntelligenceServer",
                    new DeploymentOptions()
                        .setWorker(true)
                        .setWorkerPoolSize(1)
                        .setMaxWorkerExecuteTime(10)  // 10 minutes for schema loading
                        .setMaxWorkerExecuteTimeUnit(TimeUnit.MINUTES)
            )
    );

    // Deploy OracleSQLGenerationServer (Worker)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": OracleSQLGenerationServer");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleSQLGenerationServer",
                    new DeploymentOptions().setWorker(true).setWorkerPoolSize(1)
            )
    );

    // Deploy OracleSQLValidationServer (Worker)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": OracleSQLValidationServer");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleSQLValidationServer",
                    new DeploymentOptions().setWorker(true).setWorkerPoolSize(1)
            )
    );
    
    // Deploy BusinessMappingServer (Regular)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": BusinessMappingServer");
    deploymentFutures.add(
      vertx.deployVerticle(
        "agents.director.mcp.servers.BusinessMappingServer",
        new DeploymentOptions().setWorker(false)
      )
    );

    // Deploy QueryIntentEvaluationServer (Regular)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": QueryIntentEvaluationServer");
    deploymentFutures.add(
      vertx.deployVerticle(
        "agents.director.mcp.servers.QueryIntentEvaluationServer",
        new DeploymentOptions().setWorker(false)
      )
    );

    // Deploy IntentAnalysisServer (Worker - uses LLM)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": IntentAnalysisServer");
    deploymentFutures.add(
      vertx.deployVerticle(
        "agents.director.mcp.servers.IntentAnalysisServer",
        new DeploymentOptions().setWorker(true).setWorkerPoolSize(1)
      )
    );

    // Deploy SessionSchemaResolverServer (Worker - uses DB and parallel operations)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": SessionSchemaResolverServer");
    deploymentFutures.add(
      vertx.deployVerticle(
        "agents.director.mcp.servers.SessionSchemaResolverServer",
        new DeploymentOptions().setWorker(true).setWorkerPoolSize(8) // More workers for parallel operations
      )
    );
    
    // Wait for all servers to deploy
    CompositeFuture.all(deploymentFutures).onComplete(ar -> {
      if (ar.succeeded()) {
        if (logLevel >= 1) vertx.eventBus().publish("log", "All MCP servers deployed successfully,1,Driver,StartUp,MCP");
        System.out.println("All MCP servers deployed successfully");
        vertx.eventBus().publish("mcp.servers.ready", new JsonObject()
          .put("serverCount", deploymentFutures.size())
          .put("timestamp", System.currentTimeMillis()));
      } else {
        vertx.eventBus().publish("log", "Failed to deploy MCP servers: " + ar.cause().getMessage() + ",0,Driver,StartUp,MCP");
      }
    });
  }
  
  private void deployHosts() {
    if (logLevel >= 1) vertx.eventBus().publish("log", "Deploying Host Applications...,1,Driver,StartUp,Host");
    
    // Deploy core services first
    System.out.println("[Driver] Setting up Logger...");
    setLogger();
    System.out.println("[Driver] Setting up MCP Registry Service...");
    setMCPRegistryService();
    System.out.println("[Driver] Setting up LLM API Service...");
    setLlmAPIService();
    System.out.println("[Driver] LLM API Service setup complete");
    
    // Deploy the UniversalHost (replaces the 3 legacy hosts)
    List<Future> hostFutures = new ArrayList<>();

    // Deploy UniversalHost (new 6-milestone architecture)
    System.out.println("[Driver] Deploying UniversalHost (Simplified 6-Milestone Architecture)...");
    hostFutures.add(
            vertx.deployVerticle(
                    "agents.director.hosts.UniversalHost",
                    new DeploymentOptions()
            )
    );
    
    // Wait for all hosts to deploy
    CompositeFuture.all(hostFutures).onComplete(ar -> {
      if (ar.succeeded()) {
        if (logLevel >= 1) vertx.eventBus().publish("log", "All host applications deployed successfully,1,Driver,StartUp,Host");
        
        // Deploy API endpoints after hosts are ready
        setConversationAPI();
        
        // Signal hosts ready
        vertx.eventBus().publish("hosts.ready", new JsonObject()
          .put("hostCount", hostFutures.size())
          .put("timestamp", System.currentTimeMillis()));
      } else {
        vertx.eventBus().publish("log", "Failed to deploy hosts: " + ar.cause().getMessage() + ",0,Driver,StartUp,Host");
      }
    });
  }
  
  private void setupReadinessListeners() {
    // Listen for MCP router ready event
    vertx.eventBus().consumer("mcp.router.ready", msg -> {
      JsonObject status = (JsonObject) msg.body();
      mcpRouterReady = true;
      if (logLevel >= 1) vertx.eventBus().publish("log", "MCP Router Ready on port: " + status.getInteger("port") + ",1,Driver,StartUp,MCP");
      // Deploy MCP servers after router is ready
      deployMCPServers();
    });
    
    // Listen for MCP servers ready event
    vertx.eventBus().consumer("mcp.servers.ready", msg -> {
      mcpServersReady = true;
      if (logLevel >= 1) vertx.eventBus().publish("log", "All MCP Servers Ready,1,Driver,StartUp,MCP");
      // Deploy hosts after servers are ready
      System.out.println("All MCP Servers Ready, deploy host");
      deployHosts();
    });
    
    // Listen for hosts ready event
    vertx.eventBus().consumer("hosts.ready", msg -> {
      hostsReady = true;
      if (logLevel >= 1) vertx.eventBus().publish("log", "All Host Applications Ready,1,Driver,StartUp,Host");
      checkSystemReady();
    });
  }
  
  private void checkSystemReady() {
    // Check if all critical components are ready
    if (mcpRouterReady && mcpServersReady && hostsReady) {
      // Critical system ready messages - keep console output
      System.out.println("=== MCP-Based Agent System Started ===");
      vertx.eventBus().publish("log", "MCP Router: READY" + ",2,Driver,System,System");
      vertx.eventBus().publish("log", "MCP Servers: READY" + ",2,Driver,System,System");
      vertx.eventBus().publish("log", "Host Applications: READY" + ",2,Driver,System,System");
      
      // Publish the final system ready event
      vertx.eventBus().publish("system.fully.ready", new JsonObject()
        .put("mcpRouter", mcpRouterReady)
        .put("mcpServers", mcpServersReady)
        .put("hosts", hostsReady)
        .put("timestamp", System.currentTimeMillis()));
      
      if (logLevel >= 1) vertx.eventBus().publish("log", "Published system.fully.ready - System is now operational,1,Driver,StartUp,System");
    } else {
      if (logLevel >= 3) vertx.eventBus().publish("log", "Waiting for components - Router: " + mcpRouterReady + 
                       "; Servers: " + mcpServersReady + "; Hosts: " + hostsReady + ",3,Driver,StartUp,System");
    }
  }

  private void setLogger() {
    vertx.deployVerticle(new Logger(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 1) vertx.eventBus().publish("log", "Logger Verticle initialized successfully,1,Driver,StartUp,System");
        if (logLevel >= 3) vertx.eventBus().publish("log", "Logger deployed,3,Driver,StartUp,System");
      } else {
        vertx.eventBus().publish("log", "Logger deployment failed: " + res.cause().getMessage() + ",0,Driver,StartUp,System");
      }
    });
  }
  
  private void setMCPRegistryService() {
    vertx.deployVerticle("agents.director.services.MCPRegistryService", res -> {
      if (res.succeeded()) {
        if (logLevel >= 1) vertx.eventBus().publish("log", "MCP Registry Service initialized successfully,1,Driver,StartUp,System");
        if (logLevel >= 3) vertx.eventBus().publish("log", "MCP Registry deployed,3,Driver,StartUp,System");
      } else {
        vertx.eventBus().publish("log", "MCP Registry deployment failed: " + res.cause().getMessage() + ",0,Driver,StartUp,System");
      }
    });
  }

  
  private void setConversationAPI() {
    // Deploy the streaming conversation API with host routing
    vertx.deployVerticle(new ConversationStreaming(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 1) vertx.eventBus().publish("log", "Conversation API initialized successfully,1,Driver,StartUp,API");
        if (logLevel >= 3) vertx.eventBus().publish("log", "Streaming conversation API deployed,3,Driver,StartUp,API");
      } else {
        vertx.eventBus().publish("log", "Conversation API deployment failed: " + res.cause().getMessage() + ",0,Driver,StartUp,API");
      }
    });
  }
  
  private void setLlmAPIService() {
    // Initialize the LLM API service
    boolean initialized = LlmAPIService.getInstance().setupService(vertx);
    
    if (initialized) {
      if (logLevel >= 1) vertx.eventBus().publish("log", "OpenAI API service initialized successfully,1,Driver,StartUp,System");
      if (logLevel >= 3) vertx.eventBus().publish("log", "LLM API service initialized,3,Driver,StartUp,System");
    } else {
      // Keep warning in console - important configuration issue
      System.out.println("WARNING: OpenAI API service not initialized (missing API key)");
      System.out.println("Set OPENAI_API_KEY environment variable to enable LLM responses");
      vertx.eventBus().publish("log", "OpenAI API service not initialized (missing API key),0,Driver,StartUp,System");
    }
  }
}