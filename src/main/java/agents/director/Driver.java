package agents.director;

import agents.director.services.MCPRouterService;
import agents.director.services.LlmAPIService;
import agents.director.services.Logger;
import agents.director.services.LogUtil;
import agents.director.services.OracleConnectionManager;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.ArrayList;
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
    LogUtil.logInfo(vertx, "Deploying MCP Router Service...", "Driver", "StartUp", "MCP", true);
    
    vertx.deployVerticle(new MCPRouterService(), res -> {
      if (res.succeeded()) {
        LogUtil.logInfo(vertx, "MCPRouterService deployed successfully", "Driver", "StartUp", "MCP", true);
        LogUtil.logDebug(vertx, "MCP Router deployed", "Driver", "StartUp", "MCP");
      } else {
        // Fatal error - keep console output
        vertx.eventBus().publish("log", "MCPRouterService deployment failed: " + res.cause().getMessage() + ",0,Driver,System,System");
        LogUtil.logError(vertx, "MCPRouterService deployment failed", res.cause(), "Driver", "StartUp", "MCP", false);
        res.cause().printStackTrace();
        System.exit(1); // Fatal error - cannot continue without router
      }
    });
  }
  
  private void deployMCPServers() {
    LogUtil.logInfo(vertx, "Deploying MCP Servers...", "Driver", "StartUp", "MCP", true);
    
    // Initialize OracleConnectionManager first, then deploy all servers
    System.out.println("[Driver] Starting OracleConnectionManager initialization...");
    OracleConnectionManager.getInstance().initialize(vertx).onComplete(ar -> {
      if (ar.succeeded()) {
        System.out.println("[Driver] OracleConnectionManager initialization complete");
        LogUtil.logInfo(vertx, "Oracle Connection Manager initialized", "Driver", "StartUp", "Database", true);
      } else {
        System.out.println("[Driver] OracleConnectionManager initialization failed: " + ar.cause().getMessage());
        LogUtil.logError(vertx, "Failed to initialize Oracle Connection Manager", ar.cause(), "Driver", "StartUp", "Database", true);
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

    // Deploy OracleSchemaIntelligenceServer (Worker)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": OracleSchemaIntelligenceServer");
    deploymentFutures.add(
            vertx.deployVerticle(
                    "agents.director.mcp.servers.OracleSchemaIntelligenceServer",
                    new DeploymentOptions().setWorker(true).setWorkerPoolSize(1)
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

    // Deploy StrategyGenerationServer (Worker - uses LLM)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": StrategyGenerationServer");
    deploymentFutures.add(
      vertx.deployVerticle(
        "agents.director.mcp.servers.StrategyGenerationServer",
        new DeploymentOptions().setWorker(true).setWorkerPoolSize(1)
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

    // Deploy StrategyOrchestratorServer (Regular - manages execution)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": StrategyOrchestratorServer");
    deploymentFutures.add(
      vertx.deployVerticle(
        "agents.director.mcp.servers.StrategyOrchestratorServer",
        new DeploymentOptions().setWorker(false)
      )
    );

    // Deploy StrategyLearningServer (Regular - tracks metrics)
    System.out.println("[Driver] Deploying server " + (++serverCount) + ": StrategyLearningServer");
    deploymentFutures.add(
      vertx.deployVerticle(
        "agents.director.mcp.servers.StrategyLearningServer",
        new DeploymentOptions().setWorker(false)
      )
    );
    
    // Wait for all servers to deploy
    CompositeFuture.all(deploymentFutures).onComplete(ar -> {
      if (ar.succeeded()) {
        LogUtil.logInfo(vertx, "All MCP servers deployed successfully", "Driver", "StartUp", "MCP", true);
        System.out.println("All MCP servers deployed successfully");
        vertx.eventBus().publish("mcp.servers.ready", new JsonObject()
          .put("serverCount", deploymentFutures.size())
          .put("timestamp", System.currentTimeMillis()));
      } else {
        LogUtil.logError(vertx, "Failed to deploy MCP servers", ar.cause(), "Driver", "StartUp", "MCP", true);
      }
    });
  }
  
  private void deployHosts() {
    LogUtil.logInfo(vertx, "Deploying Host Applications...", "Driver", "StartUp", "Host", true);
    
    // Deploy core services first
    System.out.println("[Driver] Setting up Logger...");
    //setLogger();
    System.out.println("[Driver] Setting up LLM API Service...");
    setLlmAPIService();
    System.out.println("[Driver] LLM API Service setup complete");
    
    // Deploy the 3 host applications
    List<Future> hostFutures = new ArrayList<>();


    // Deploy OracleDBAnswererHost
    System.out.println("[Driver] Deploying OracleDBAnswererHost...");
    hostFutures.add(
            vertx.deployVerticle(
                    "agents.director.hosts.OracleDBAnswererHost",
                    new DeploymentOptions()
            )
    );

    // Deploy OracleSQLBuilderHost
    System.out.println("[Driver] Deploying OracleSQLBuilderHost...");
    hostFutures.add(
            vertx.deployVerticle(
                    "agents.director.hosts.OracleSQLBuilderHost",
                    new DeploymentOptions()
            )
    );

    // Deploy ToolFreeDirectLLMHost
    System.out.println("[Driver] Deploying ToolFreeDirectLLMHost...");
    hostFutures.add(
      vertx.deployVerticle(
        "agents.director.hosts.ToolFreeDirectLLMHost",
        new DeploymentOptions()
      )
    );
    
    // Wait for all hosts to deploy
    CompositeFuture.all(hostFutures).onComplete(ar -> {
      if (ar.succeeded()) {
        LogUtil.logInfo(vertx, "All host applications deployed successfully", "Driver", "StartUp", "Host", true);
        
        // Deploy API endpoints after hosts are ready
        setConversationAPI();
        
        // Signal hosts ready
        vertx.eventBus().publish("hosts.ready", new JsonObject()
          .put("hostCount", hostFutures.size())
          .put("timestamp", System.currentTimeMillis()));
      } else {
        LogUtil.logError(vertx, "Failed to deploy hosts", ar.cause(), "Driver", "StartUp", "Host", true);
      }
    });
  }
  
  private void setupReadinessListeners() {
    // Listen for MCP router ready event
    vertx.eventBus().consumer("mcp.router.ready", msg -> {
      JsonObject status = (JsonObject) msg.body();
      mcpRouterReady = true;
      LogUtil.logInfo(vertx, "MCP Router Ready on port: " + status.getInteger("port"), "Driver", "StartUp", "MCP", true);
      // Deploy MCP servers after router is ready
      deployMCPServers();
    });
    
    // Listen for MCP servers ready event
    vertx.eventBus().consumer("mcp.servers.ready", msg -> {
      mcpServersReady = true;
      LogUtil.logInfo(vertx, "All MCP Servers Ready", "Driver", "StartUp", "MCP", true);
      // Deploy hosts after servers are ready
      System.out.println("All MCP Servers Ready, deploy host");
      deployHosts();
    });
    
    // Listen for hosts ready event
    vertx.eventBus().consumer("hosts.ready", msg -> {
      hostsReady = true;
      LogUtil.logInfo(vertx, "All Host Applications Ready", "Driver", "StartUp", "Host", true);
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
      
      LogUtil.logInfo(vertx, "Published system.fully.ready - System is now operational", "Driver", "StartUp", "System", true);
    } else {
      LogUtil.logDebug(vertx, "Waiting for components - Router: " + mcpRouterReady + 
                       ", Servers: " + mcpServersReady + ", Hosts: " + hostsReady, "Driver", "StartUp", "System");
    }
  }

  private void setLogger() {
    vertx.deployVerticle(new Logger(), res -> {
      if (res.succeeded()) {
        LogUtil.logInfo(vertx, "Logger Verticle initialized successfully", "Driver", "StartUp", "System", true);
        LogUtil.logDebug(vertx, "Logger deployed", "Driver", "StartUp", "System");
      } else {
        LogUtil.logError(vertx, "Logger deployment failed", res.cause(), "Driver", "StartUp", "System", true);
      }
    });
  }

  
  private void setConversationAPI() {
    // Deploy the streaming conversation API with host routing
    vertx.deployVerticle(new ConversationStreaming(), res -> {
      if (res.succeeded()) {
        LogUtil.logInfo(vertx, "Conversation API initialized successfully", "Driver", "StartUp", "API", true);
        LogUtil.logDebug(vertx, "Streaming conversation API deployed", "Driver", "StartUp", "API");
      } else {
        LogUtil.logError(vertx, "Conversation API deployment failed", res.cause(), "Driver", "StartUp", "API", true);
      }
    });
  }
  
  private void setLlmAPIService() {
    // Initialize the LLM API service
    boolean initialized = LlmAPIService.getInstance().setupService(vertx);
    
    if (initialized) {
      LogUtil.logInfo(vertx, "OpenAI API service initialized successfully", "Driver", "StartUp", "System", true);
      LogUtil.logDebug(vertx, "LLM API service initialized", "Driver", "StartUp", "System");
    } else {
      // Keep warning in console - important configuration issue
      System.out.println("WARNING: OpenAI API service not initialized (missing API key)");
      System.out.println("Set OPENAI_API_KEY environment variable to enable LLM responses");
      LogUtil.logError(vertx, "OpenAI API service not initialized (missing API key)", "Driver", "StartUp", "System", false);
    }
  }
}