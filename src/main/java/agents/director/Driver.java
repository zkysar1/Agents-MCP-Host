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
    // Load environment variables from .env.local file
    try {
      Dotenv dotenv = Dotenv.configure()
          .filename(".env.local")
          .systemProperties()  // Load as system properties so they're accessible via System.getProperty()
          .ignoreIfMissing()
          .load();
      System.out.println("Loaded environment configuration from .env.local");
      if (logLevel >= 3) vertx.eventBus().publish("log", "Loaded environment configuration from .env.local,3,Driver,StartUp,MCP");
      Driver me = new Driver();
      me.doIt();
    } catch (Exception e) {
      vertx.eventBus().publish("log", "Could not load .env.local file:" + e.getMessage() + ",3,Driver,StartUp,MCP");
      System.err.println("Warning: Could not load .env.local file: " + e.getMessage());
    }
  }

  private void doIt() {
    // Log startup information - Keep as console output (critical startup info)
    System.out.println("Driver Starting");
    if (logLevel >= 1) vertx.eventBus().publish("log", "Driver Starting,1,Driver,StartUp,MCP");
    if (logLevel >= 1) vertx.eventBus().publish("log", "Java version: " + System.getProperty("java.version")+",1,Driver,StartUp,MCP");
    if (logLevel >= 1) vertx.eventBus().publish("log", "Working directory: " + System.getProperty("user.dir")+",1,Driver,StartUp,MCP");
    if (logLevel >= 1) vertx.eventBus().publish("log", "Data path: " + DATA_PATH+",1,Driver,StartUp,MCP");
    if (logLevel >= 1) vertx.eventBus().publish("log", "Agent path: " + zakAgentPath+",1,Driver,StartUp,MCP");
    
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
        if (logLevel >= 3) vertx.eventBus().publish("log", "MCPRouterService deployed,3,Driver,StartUp,MCP");
      } else {
        // Fatal error - keep console output
        vertx.eventBus().publish("log", "MCPRouterService deployment failed: " + res.cause().getMessage() + ",0,Driver,System,System");
        System.err.println("Fatal error - cannot continue without router");
      }
    });
  }
  
  private void deployMCPServers() {
    if (logLevel >= 1) vertx.eventBus().publish("log", "Deploying MCP Servers...,1,Driver,StartUp,MCP");
    
    // Initialize OracleConnectionManager first, then build knowledge graph, then deploy servers
    OracleConnectionManager.getInstance().initialize(vertx).onComplete(ar -> {
      if (ar.succeeded()) {
        if (logLevel >= 3) vertx.eventBus().publish("log", "Oracle Connection Manager initialized,1,Driver,StartUp,Database");

        // Build knowledge graph after connection is ready
        System.out.println("Building knowledge graph...");
        if (logLevel >= 1) vertx.eventBus().publish("log", "Building knowledge graph...,1,Driver,StartUp,Database");
        KnowledgeGraphBuilder.getInstance().initialize(vertx).onComplete(graphAr -> {
          if (graphAr.succeeded()) {
            JsonObject metadata = graphAr.result().getJsonObject("metadata");
            System.out.println("Knowledge graph built successfully in " +
                metadata.getLong("buildTime") + "ms with " +
                metadata.getInteger("tableCount") + " tables and " +
                metadata.getInteger("relationshipCount") + " relationships");
            if (logLevel >= 1) vertx.eventBus().publish("log",
                "Knowledge graph built - Tables: " + metadata.getInteger("tableCount") +
                ", Relationships: " + metadata.getInteger("relationshipCount") +
                ", Build time: " + metadata.getLong("buildTime") + "ms,1,Driver,StartUp,Graph");
            // Deploy all servers after graph build attempt
            deployAllMCPServers();
          } else {
            System.err.println("Knowledge graph build failed: " + graphAr.cause().getMessage());
            vertx.eventBus().publish("log", "Fatal error - Failed to build knowledge graph: " + graphAr.cause().getMessage() + ",0,Driver,StartUp,Graph");
          }

        });
      } else {
        vertx.eventBus().publish("log", "Failed to initialize Oracle Connection Manager: " + ar.cause().getMessage() + ",0,Driver,StartUp,Database");
        System.err.println("Fatal error - OracleConnectionManager initialization failed - cannot continue: " + ar.cause().getMessage());
      }
    });
  }
  
  private void deployAllMCPServers() {
    if (logLevel >= 3) vertx.eventBus().publish("log", "Deploying all MCP servers...,3,Driver,StartUp,Database");
    
    // Import the server classes
    List<Future<String>> deploymentFutures = new ArrayList<>();
    
    int serverCount = 0;

    // Deploy OracleQueryExecutionServer (Worker)
    if (logLevel >= 3) vertx.eventBus().publish("log", "Deploying server " + (++serverCount) + ": OracleQueryExecutionServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleQueryExecutionServer",
                    new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER).setWorkerPoolSize(1) // Reduced from 5
            )
    );

    // Deploy OracleQueryAnalysisServer (Worker)
    if (logLevel >= 3) vertx.eventBus().publish("log", "Deploying server " + (++serverCount) + ": OracleQueryAnalysisServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleQueryAnalysisServer",
                    new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER).setWorkerPoolSize(1)
            )
    );

    // Deploy OracleSchemaIntelligenceServer (Worker - with extended timeout for schema loading)
    if (logLevel >= 3) vertx.eventBus().publish("log", "Deploying server " + (++serverCount) + ": OracleSchemaIntelligenceServer,3,Driver,StartUp,Database");
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
    if (logLevel >= 3) vertx.eventBus().publish("log", "Deploying server " + (++serverCount) + ": OracleSQLGenerationServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleSQLGenerationServer",
                    new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER).setWorkerPoolSize(1)
            )
    );

    // Deploy OracleSQLValidationServer (Worker)
    if (logLevel >= 3) vertx.eventBus().publish("log", "Deploying server " + (++serverCount) + ": OracleSQLValidationServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleSQLValidationServer",
                    new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER).setWorkerPoolSize(1)
            )
    );

    // Deploy QueryIntentEvaluationServer (Regular)
    if (logLevel >= 3) vertx.eventBus().publish("log", "Deploying server " + (++serverCount) + ": QueryIntentEvaluationServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
      vertx.deployVerticle(
        "agents.director.mcp.servers.QueryIntentEvaluationServer",
        new DeploymentOptions().setThreadingModel(ThreadingModel.EVENT_LOOP)
      )
    );

    // Deploy IntentAnalysisServer (Worker - uses LLM)
    if (logLevel >= 3) vertx.eventBus().publish("log", "Deploying server " + (++serverCount) + ": IntentAnalysisServer,3,Driver,StartUp,Database");
    deploymentFutures.add(
      vertx.deployVerticle(
        "agents.director.mcp.servers.IntentAnalysisServer",
        new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER).setWorkerPoolSize(1)
      )
    );

    // Wait for all servers to deploy
    Future.all(deploymentFutures).onComplete(ar -> {
      if (ar.succeeded()) {
        if (logLevel >= 3) vertx.eventBus().publish("log", "All MCP servers deployed successfully,3,Driver,StartUp,MCP");
        System.out.println("All MCP servers deployed successfully");
        vertx.eventBus().publish("mcp.servers.ready", new JsonObject()
          .put("serverCount", deploymentFutures.size())
          .put("timestamp", System.currentTimeMillis()));
      } else {
        vertx.eventBus().publish("log", "Failed to deploy MCP servers: " + ar.cause().getMessage() + ",0,Driver,StartUp,MCP");
        System.err.println("Failed to deploy MCP servers: " + ar.cause().getMessage());
      }
    });
  }
  
  private void deployHosts() {
    if (logLevel >= 1) vertx.eventBus().publish("log", "Deploying Host Applications...,1,Driver,StartUp,Host");
    
    // Deploy core services first
    if (logLevel >= 3) vertx.eventBus().publish("log", "Setting up Logger...,3,Driver,StartUp,MCP");
    setLogger();
    if (logLevel >= 3) vertx.eventBus().publish("log", "Setting up MCP Registry Service...,3,Driver,StartUp,MCP");
    setMCPRegistryService();
    if (logLevel >= 3) vertx.eventBus().publish("log", "Setting up LLM API Service...,3,Driver,StartUp,MCP");
    setLlmAPIService();

    // Deploy UniversalHost (new 6-milestone architecture)
    if (logLevel >= 3) vertx.eventBus().publish("log", "Deploying UniversalHost (Simplified 6-Milestone Architecture)...,3,Driver,StartUp,MCP");
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
        if (logLevel >= 3) vertx.eventBus().publish("log", "All host applications deployed successfully,3,Driver,StartUp,Host");
        
        // Deploy API endpoints after hosts are ready
        setConversationAPI();
        
        // Signal hosts ready
        vertx.eventBus().publish("hosts.ready", new JsonObject()
          .put("hostCount", hostFutures.size())
          .put("timestamp", System.currentTimeMillis()));
      } else {
        vertx.eventBus().publish("log", "Failed to deploy hosts: " + ar.cause().getMessage() + ",0,Driver,StartUp,Host");
        System.err.println("Failed to deploy hosts: " + ar.cause().getMessage());
      }
    });
  }
  
  private void setupReadinessListeners() {
    // Listen for MCP router ready event
    vertx.eventBus().consumer("mcp.router.ready", msg -> {
      JsonObject status = (JsonObject) msg.body();
      mcpRouterReady = true;
      if (logLevel >= 1) vertx.eventBus().publish("log", "MCP Router Ready on port: " + status.getInteger("port") + ".  Now deploy Servers,1,Driver,StartUp,MCP");
      // Deploy MCP servers after router is ready
      deployMCPServers();
    });
    
    // Listen for MCP servers ready event
    vertx.eventBus().consumer("mcp.servers.ready", msg -> {
      mcpServersReady = true;
      if (logLevel >= 1) vertx.eventBus().publish("log", "All MCP Servers Ready.  Now Deploy hosts,1,Driver,StartUp,MCP");
      // Deploy hosts after servers are ready
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
      if (logLevel >= 1) vertx.eventBus().publish("log", "MCP Router: READY" + ",1,Driver,System,System");
      if (logLevel >= 1) vertx.eventBus().publish("log", "MCP Servers: READY" + ",1,Driver,System,System");
      if (logLevel >= 1) vertx.eventBus().publish("log", "Host Applications: READY" + ",1,Driver,System,System");
      vertx.eventBus().publish("log", "=== MCP-Based Agent System Started ===,1,Driver,System,System");
      
      // Publish the final system ready event
      vertx.eventBus().publish("system.fully.ready", new JsonObject()
        .put("mcpRouter", mcpRouterReady)
        .put("mcpServers", mcpServersReady)
        .put("hosts", hostsReady)
        .put("timestamp", System.currentTimeMillis()));
      
      if (logLevel >= 1) vertx.eventBus().publish("log", "Published system.fully.ready - System is now operational,1,Driver,StartUp,System");
    } else {
      vertx.eventBus().publish("log", "Error: sent checkSystemReady when not ready: " + mcpRouterReady + "; Servers: " + mcpServersReady + "; Hosts: " + hostsReady + ",0,Driver,StartUp,System");
      System.err.println("Error: sent checkSystemReady when not ready: " + mcpRouterReady + "; Servers: " + mcpServersReady + "; Hosts: " + hostsReady);
    }
  }

  private void setLogger() {
    vertx.deployVerticle(new Logger(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 3) vertx.eventBus().publish("log", "Logger deployed,3,Driver,StartUp,System");
      } else {
        vertx.eventBus().publish("log", "Logger deployment failed: " + res.cause().getMessage() + ",0,Driver,StartUp,System");
        System.err.println("Logger deployment failed: " + res.cause().getMessage());
      }
    });
  }
  
  private void setMCPRegistryService() {
    vertx.deployVerticle("agents.director.services.MCPRegistryService", res -> {
      if (res.succeeded()) {
        if (logLevel >= 3) vertx.eventBus().publish("log", "MCP Registry deployed,3,Driver,StartUp,System");
      } else {
        vertx.eventBus().publish("log", "MCP Registry deployment failed: " + res.cause().getMessage() + ",0,Driver,StartUp,System");
        System.err.println("MCP Registry deployment failed: " + res.cause().getMessage());
      }
    });
  }

  
  private void setConversationAPI() {
    // Deploy the streaming conversation API with host routing
    vertx.deployVerticle(new ConversationStreaming(), res -> {
      if (res.succeeded()) {
        if (logLevel >= 3) vertx.eventBus().publish("log", "Streaming conversation API deployed,3,Driver,StartUp,API");
      } else {
        vertx.eventBus().publish("log", "Conversation API deployment failed: " + res.cause().getMessage() + ",0,Driver,StartUp,API");
        System.err.println("Conversation API deployment failed: " + res.cause().getMessage());
      }
    });
  }
  
  private void setLlmAPIService() {
    // Initialize the LLM API service
    boolean initialized = LlmAPIService.getInstance().setupService(vertx);
    if (initialized) {
      if (logLevel >= 3) vertx.eventBus().publish("log", "LLM API service initialized,3,Driver,StartUp,System");
    } else {
      // Keep warning in console - important configuration issue
      vertx.eventBus().publish("log", "LLM API service not initialized (missing configuration),0,Driver,StartUp,System");
      System.err.println("Error: LLM API service not initialized (missing configuration)(maybe check .env.local)");
    }
  }
}