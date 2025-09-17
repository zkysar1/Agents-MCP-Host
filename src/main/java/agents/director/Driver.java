package agents.director;

import agents.director.services.MCPRouterService;
import agents.director.services.LlmAPIService;
import agents.director.services.Logger;
import agents.director.services.OracleConnectionManager;
import agents.director.services.KnowledgeGraphBuilder;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import io.github.cdimascio.dotenv.Dotenv;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedList;

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

  // Emergency log buffer - captures logs before Logger is ready
  private static final int EMERGENCY_BUFFER_SIZE = 500;
  private static final List<String> emergencyLogBuffer = Collections.synchronizedList(new LinkedList<>());
  private static final DateTimeFormatter LOG_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
      .withZone(ZoneId.of("UTC"));
  private static boolean loggerReady = false;

  /**
   * Captures log messages to emergency buffer or publishes directly if logger is ready.
   * Thread-safe and maintains a circular buffer of the last EMERGENCY_BUFFER_SIZE entries.
   */
  public static void captureOrPublishLog(String message) {
    if (!loggerReady) {
      synchronized (emergencyLogBuffer) {
        if (emergencyLogBuffer.size() >= EMERGENCY_BUFFER_SIZE) {
          emergencyLogBuffer.removeFirst(); // Remove oldest entry
        }
        emergencyLogBuffer.add(message);
      }
    } else {
      vertx.eventBus().publish("log", message);
    }
  }

  /**
   * Flushes all emergency buffer logs to the logger.
   * Called when logger becomes ready.
   */
  private static void flushEmergencyBuffer() {
    synchronized (emergencyLogBuffer) {
      for (String entry : emergencyLogBuffer) {
        vertx.eventBus().publish("log", entry);
      }
      emergencyLogBuffer.clear();
    }
  }

  public static void main(String[] args) {
    // Capture startup info immediately to emergency buffer
    captureOrPublishLog("=== MCP-Based Agent System Starting ===,1,Driver,System,System");
    captureOrPublishLog("Java version: " + System.getProperty("java.version") + ",2,Driver,System,System");
    captureOrPublishLog("Working directory: " + System.getProperty("user.dir") + ",2,Driver,System,System");
    captureOrPublishLog("Data path: " + DATA_PATH + ",2,Driver,System,System");
    captureOrPublishLog("Agent path: " + zakAgentPath + ",2,Driver,System,System");

    // Deploy Logger FIRST before anything else
    System.out.println("Deploying Logger as first component...");
    vertx.deployVerticle(new Logger(), res -> {
      if (res.succeeded()) {
        // Logger is ready, now we can flush emergency buffer
        loggerReady = true;
        System.out.println("Logger deployed successfully - flushing emergency buffer");
        flushEmergencyBuffer();
        captureOrPublishLog("Logger ready - emergency buffer flushed,2,Logger,System,System");

        // Now continue with rest of initialization
        loadEnvironmentAndStart();
      } else {
        System.err.println("FATAL: Logger deployment failed: " + res.cause().getMessage());
        System.err.println("Cannot continue without logging capability");
        System.exit(1);
      }
    });
  }

  private static void loadEnvironmentAndStart() {
    // Load environment variables from .env.local file
    try {
      Dotenv dotenv = Dotenv.configure()
          .filename(".env.local")
          .systemProperties()  // Load as system properties so they're accessible via System.getProperty()
          .ignoreIfMissing()
          .load();
      System.out.println("Loaded environment configuration from .env.local");
      captureOrPublishLog("Loaded environment configuration from .env.local,3,Driver,StartUp,MCP");
      Driver me = new Driver();
      me.doIt();
    } catch (Exception e) {
      captureOrPublishLog("Could not load .env.local file:" + e.getMessage() + ",3,Driver,StartUp,MCP");
      System.err.println("Warning: Could not load .env.local file: " + e.getMessage());
      // Continue anyway - not fatal
      Driver me = new Driver();
      me.doIt();
    }
  }

  private void doIt() {
    // Log startup information
    System.out.println("Driver initialization starting");
    if (logLevel >= 1) captureOrPublishLog("Driver initialization starting,1,Driver,StartUp,MCP");

    // Set up event listeners for component readiness
    setupReadinessListeners();

    // Start deployment sequence: Router first, then servers, then hosts
    deployMCPRouter();
  }
  
  // New deployment methods following MCP architecture
  private void deployMCPRouter() {
    if (logLevel >= 1) captureOrPublishLog("Deploying MCP Router Service...,1,Driver,StartUp,MCP");
    
    vertx.deployVerticle(new MCPRouterService(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 3) captureOrPublishLog("MCPRouterService deployed,3,Driver,StartUp,MCP");
      } else {
        // Fatal error - keep console output
        captureOrPublishLog("MCPRouterService deployment failed: " + res.cause().getMessage() + ",0,Driver,System,System");
        System.err.println("Fatal error - cannot continue without router");
      }
    });
  }
  
  private void deployMCPServers() {
    if (logLevel >= 1) captureOrPublishLog("Deploying MCP Servers...,1,Driver,StartUp,MCP");

    // Initialize LLM service first (needed by KnowledgeGraphBuilder for dynamic synonyms)
    if (logLevel >= 3) captureOrPublishLog("Setting up LLM API Service...,3,Driver,StartUp,MCP");
    setLlmAPIService();

    // Initialize OracleConnectionManager, then build knowledge graph, then deploy servers
    OracleConnectionManager.getInstance().initialize(vertx).onComplete(ar -> {
      if (ar.succeeded()) {
        if (logLevel >= 3) captureOrPublishLog("Oracle Connection Manager initialized,1,Driver,StartUp,Database");

        // Build knowledge graph after connection is ready
        System.out.println("Building knowledge graph...");
        if (logLevel >= 1) captureOrPublishLog("Building knowledge graph...,1,Driver,StartUp,Database");
        KnowledgeGraphBuilder.getInstance().initialize(vertx).onComplete(graphAr -> {
          if (graphAr.succeeded()) {
            JsonObject metadata = graphAr.result().getJsonObject("metadata");
            System.out.println("Knowledge graph built successfully in " +
                metadata.getLong("buildTime") + "ms with " +
                metadata.getInteger("tableCount") + " tables and " +
                metadata.getInteger("relationshipCount") + " relationships");
            if (logLevel >= 1) captureOrPublishLog(
                "Knowledge graph built - Tables: " + metadata.getInteger("tableCount") +
                ", Relationships: " + metadata.getInteger("relationshipCount") +
                ", Build time: " + metadata.getLong("buildTime") + "ms,1,Driver,StartUp,Graph");
            // Deploy all servers after graph build attempt
            deployAllMCPServers();
          } else {
            System.err.println("Knowledge graph build failed: " + graphAr.cause().getMessage());
            captureOrPublishLog( "Fatal error - Failed to build knowledge graph: " + graphAr.cause().getMessage() + ",0,Driver,StartUp,Graph");
          }

        });
      } else {
        captureOrPublishLog( "Failed to initialize Oracle Connection Manager: " + ar.cause().getMessage() + ",0,Driver,StartUp,Database");
        System.err.println("Fatal error - OracleConnectionManager initialization failed - cannot continue: " + ar.cause().getMessage());
      }
    });
  }
  
  private void deployAllMCPServers() {
    if (logLevel >= 3) captureOrPublishLog( "Deploying all MCP servers...,3,Driver,StartUp,Database");
    
    // Import the server classes
    List<Future<String>> deploymentFutures = new ArrayList<>();
    
    int serverCount = 0;

    // Deploy OracleQueryExecutionServer (Worker)
    if (logLevel >= 3) captureOrPublishLog( "Deploying server " + (++serverCount) + ": OracleQueryExecutionServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleQueryExecutionServer",
                    new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER).setWorkerPoolSize(1) // Reduced from 5
            )
    );

    // Deploy OracleQueryAnalysisServer (Worker)
    if (logLevel >= 3) captureOrPublishLog( "Deploying server " + (++serverCount) + ": OracleQueryAnalysisServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleQueryAnalysisServer",
                    new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER).setWorkerPoolSize(1)
            )
    );

    // Deploy OracleSchemaIntelligenceServer (Worker - with extended timeout for schema loading)
    if (logLevel >= 3) captureOrPublishLog( "Deploying server " + (++serverCount) + ": OracleSchemaIntelligenceServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleSchemaIntelligenceServer",
                    new DeploymentOptions()
                        .setThreadingModel(ThreadingModel.WORKER)
                        .setWorkerPoolSize(1)
                        .setMaxWorkerExecuteTime(5)  // 5 minutes (reduced since graph is pre-built)
                        .setMaxWorkerExecuteTimeUnit(TimeUnit.MINUTES)
            )
    );

    // Deploy OracleSQLGenerationServer (Worker)
    if (logLevel >= 3) captureOrPublishLog( "Deploying server " + (++serverCount) + ": OracleSQLGenerationServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleSQLGenerationServer",
                    new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER).setWorkerPoolSize(1)
            )
    );

    // Deploy OracleSQLValidationServer (Worker)
    if (logLevel >= 3) captureOrPublishLog( "Deploying server " + (++serverCount) + ": OracleSQLValidationServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleSQLValidationServer",
                    new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER).setWorkerPoolSize(1)
            )
    );

    // Deploy QueryIntentEvaluationServer (Regular)
    if (logLevel >= 3) captureOrPublishLog( "Deploying server " + (++serverCount) + ": QueryIntentEvaluationServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
      vertx.deployVerticle(
        "agents.director.mcp.servers.QueryIntentEvaluationServer",
        new DeploymentOptions().setThreadingModel(ThreadingModel.EVENT_LOOP)
      )
    );

    // Deploy IntentAnalysisServer (Worker - uses LLM)
    if (logLevel >= 3) captureOrPublishLog( "Deploying server " + (++serverCount) + ": IntentAnalysisServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
      vertx.deployVerticle(
        "agents.director.mcp.servers.IntentAnalysisServer",
        new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER).setWorkerPoolSize(1)
      )
    );

    // Wait for all servers to deploy
    Future.all(deploymentFutures).onComplete(ar -> {
      if (ar.succeeded()) {
        if (logLevel >= 3) captureOrPublishLog( "All MCP servers deployed successfully,3,Driver,StartUp,MCP");
        System.out.println("All MCP servers deployed successfully");
        vertx.eventBus().publish("mcp.servers.ready", new JsonObject()
          .put("serverCount", deploymentFutures.size())
          .put("timestamp", System.currentTimeMillis()));
      } else {
        captureOrPublishLog( "Failed to deploy MCP servers: " + ar.cause().getMessage() + ",0,Driver,StartUp,MCP");
        System.err.println("Failed to deploy MCP servers: " + ar.cause().getMessage());
      }
    });
  }
  
  private void deployHosts() {
    if (logLevel >= 1) captureOrPublishLog( "Deploying Host Applications...,1,Driver,StartUp,Host");

    // Deploy core services (Logger already deployed at startup)
    if (logLevel >= 3) captureOrPublishLog( "Setting up MCP Registry Service...,3,Driver,StartUp,MCP");
    setMCPRegistryService();
    // Note: LLM API Service is now initialized earlier in deployMCPServers() before Oracle/KnowledgeGraph

    // Deploy UniversalHost (new 6-milestone architecture)
    if (logLevel >= 3) captureOrPublishLog( "Deploying UniversalHost (Simplified 6-Milestone Architecture)...,3,Driver,StartUp,MCP");
    List<Future<String>> hostFutures = new ArrayList<>();
    hostFutures.add(
            vertx.deployVerticle(
                    "agents.director.hosts.UniversalHost",
                    new DeploymentOptions()
            )
    );

    // Wait for all hosts to deploy
    Future.all(hostFutures).onComplete(ar -> {
      if (ar.succeeded()) {
        if (logLevel >= 3) captureOrPublishLog( "All host applications deployed successfully,3,Driver,StartUp,Host");
        
        // Deploy API endpoints after hosts are ready
        setConversationAPI();
        
        // Signal hosts ready
        vertx.eventBus().publish("hosts.ready", new JsonObject()
          .put("hostCount", hostFutures.size())
          .put("timestamp", System.currentTimeMillis()));
      } else {
        captureOrPublishLog( "Failed to deploy hosts: " + ar.cause().getMessage() + ",0,Driver,StartUp,Host");
        System.err.println("Failed to deploy hosts: " + ar.cause().getMessage());
      }
    });
  }
  
  private void setupReadinessListeners() {
    // Listen for MCP router ready event
    vertx.eventBus().consumer("mcp.router.ready", msg -> {
      JsonObject status = (JsonObject) msg.body();
      mcpRouterReady = true;
      if (logLevel >= 1) captureOrPublishLog( "MCP Router Ready on port: " + status.getInteger("port") + ".  Now deploy Servers,1,Driver,StartUp,MCP");
      // Deploy MCP servers after router is ready
      deployMCPServers();
    });
    
    // Listen for MCP servers ready event
    vertx.eventBus().consumer("mcp.servers.ready", msg -> {
      mcpServersReady = true;
      if (logLevel >= 1) captureOrPublishLog( "All MCP Servers Ready.  Now Deploy hosts,1,Driver,StartUp,MCP");
      // Deploy hosts after servers are ready
      deployHosts();
    });
    
    // Listen for hosts ready event
    vertx.eventBus().consumer("hosts.ready", msg -> {
      hostsReady = true;
      if (logLevel >= 1) captureOrPublishLog( "All Host Applications Ready,1,Driver,StartUp,Host");
      checkSystemReady();
    });
  }
  
  private void checkSystemReady() {
    // Check if all critical components are ready
    if (mcpRouterReady && mcpServersReady && hostsReady) {
      // Critical system ready messages - keep console output
      if (logLevel >= 1) captureOrPublishLog( "MCP Router: READY" + ",1,Driver,System,System");
      if (logLevel >= 1) captureOrPublishLog( "MCP Servers: READY" + ",1,Driver,System,System");
      if (logLevel >= 1) captureOrPublishLog( "Host Applications: READY" + ",1,Driver,System,System");
      captureOrPublishLog( "=== MCP-Based Agent System Started ===,1,Driver,System,System");
      
      // Publish the final system ready event
      vertx.eventBus().publish("system.fully.ready", new JsonObject()
        .put("mcpRouter", mcpRouterReady)
        .put("mcpServers", mcpServersReady)
        .put("hosts", hostsReady)
        .put("timestamp", System.currentTimeMillis()));
      
      if (logLevel >= 1) captureOrPublishLog( "Published system.fully.ready - System is now operational,1,Driver,StartUp,System");
    } else {
      captureOrPublishLog( "Error: sent checkSystemReady when not ready: " + mcpRouterReady + "; Servers: " + mcpServersReady + "; Hosts: " + hostsReady + ",0,Driver,StartUp,System");
      System.err.println("Error: sent checkSystemReady when not ready: " + mcpRouterReady + "; Servers: " + mcpServersReady + "; Hosts: " + hostsReady);
    }
  }

  private void setMCPRegistryService() {
    vertx.deployVerticle("agents.director.services.MCPRegistryService", res -> {
      if (res.succeeded()) {
        if (logLevel >= 3) captureOrPublishLog( "MCP Registry deployed,3,Driver,StartUp,System");
      } else {
        captureOrPublishLog( "MCP Registry deployment failed: " + res.cause().getMessage() + ",0,Driver,StartUp,System");
        System.err.println("MCP Registry deployment failed: " + res.cause().getMessage());
      }
    });
  }

  
  private void setConversationAPI() {
    // Deploy the streaming conversation API with host routing
    vertx.deployVerticle(new ConversationStreaming(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 3) captureOrPublishLog( "Streaming conversation API deployed,3,Driver,StartUp,API");
      } else {
        captureOrPublishLog( "Conversation API deployment failed: " + res.cause().getMessage() + ",0,Driver,StartUp,API");
        System.err.println("Conversation API deployment failed: " + res.cause().getMessage());
      }
    });
  }
  
  private void setLlmAPIService() {
    // Initialize the LLM API service
    boolean initialized = LlmAPIService.getInstance().setupService(vertx);
    if (initialized) {
      if (logLevel >= 3) captureOrPublishLog( "LLM API service initialized,3,Driver,StartUp,System");
    } else {
      // Keep warning in console - important configuration issue
      captureOrPublishLog( "LLM API service not initialized (missing configuration),0,Driver,StartUp,System");
      System.err.println("Error: LLM API service not initialized (missing configuration)(maybe check .env.local)");
    }
  }
}