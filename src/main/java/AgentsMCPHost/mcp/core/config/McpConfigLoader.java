package AgentsMCPHost.mcp.core.config;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.file.FileSystem;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Loads MCP configuration from file or environment.
 * Supports both JSON file configuration and environment variable overrides.
 */
public class McpConfigLoader {
    
    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/mcp-config.json";
    private static final String CONFIG_ENV_VAR = "MCP_CONFIG_PATH";
    
    private final Vertx vertx;
    private JsonObject cachedConfig;
    
    public McpConfigLoader(Vertx vertx) {
        this.vertx = vertx;
    }
    
    /**
     * Load MCP configuration from file or use defaults
     */
    public Future<JsonObject> loadConfig() {
        if (cachedConfig != null) {
            return Future.succeededFuture(cachedConfig);
        }
        
        Promise<JsonObject> promise = Promise.promise();
        
        // Check for config path from environment
        String configPath = System.getenv(CONFIG_ENV_VAR);
        if (configPath == null || configPath.isEmpty()) {
            configPath = DEFAULT_CONFIG_PATH;
        }
        
        // Make configPath final for lambda
        final String finalConfigPath = configPath;
        
        // Check if config file exists
        Path path = Paths.get(finalConfigPath);
        if (!Files.exists(path)) {
            System.out.println("MCP config file not found, using defaults: " + finalConfigPath);
            // Wrap default config creation in executeBlocking
            vertx.<JsonObject>executeBlocking(() -> {
                return getDefaultConfig();
            }, false)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    cachedConfig = ar.result();
                    promise.complete(cachedConfig);
                } else {
                    promise.fail(ar.cause());
                }
            });
            return promise.future();
        }
        
        // Read config file
        FileSystem fs = vertx.fileSystem();
        fs.readFile(finalConfigPath, ar -> {
            if (ar.succeeded()) {
                // Wrap JSON parsing in executeBlocking
                vertx.<JsonObject>executeBlocking(() -> {
                    try {
                        JsonObject config = new JsonObject(ar.result());
                        return mergeWithEnvironment(config);
                    } catch (Exception e) {
                        System.err.println("Failed to parse MCP config: " + e.getMessage());
                        return getDefaultConfig();
                    }
                }, false)
                .onComplete(parseResult -> {
                    if (parseResult.succeeded()) {
                        cachedConfig = parseResult.result();
                        System.out.println("Loaded MCP configuration from: " + finalConfigPath);
                        promise.complete(cachedConfig);
                    } else {
                        // Fallback to default on parse failure
                        vertx.<JsonObject>executeBlocking(() -> getDefaultConfig(), false)
                        .onComplete(defaultResult -> {
                            cachedConfig = defaultResult.result();
                            promise.complete(cachedConfig);
                        });
                    }
                });
            } else {
                System.err.println("Failed to read MCP config: " + ar.cause().getMessage());
                // Wrap default config creation in executeBlocking
                vertx.<JsonObject>executeBlocking(() -> getDefaultConfig(), false)
                .onComplete(defaultResult -> {
                    if (defaultResult.succeeded()) {
                        cachedConfig = defaultResult.result();
                        promise.complete(cachedConfig);
                    } else {
                        promise.fail(defaultResult.cause());
                    }
                });
            }
        });
        
        return promise.future();
    }
    
    /**
     * Get default configuration if file not found
     */
    private JsonObject getDefaultConfig() {
        return new JsonObject()
            .put("mcpServers", new JsonObject()
                .put("httpServers", new JsonObject()
                    .put("calculator", new JsonObject()
                        .put("enabled", true)
                        .put("port", 8081))
                    .put("weather", new JsonObject()
                        .put("enabled", true)
                        .put("port", 8082))
                    .put("database", new JsonObject()
                        .put("enabled", true)
                        .put("port", 8083))
                    .put("filesystem", new JsonObject()
                        .put("enabled", true)
                        .put("port", 8084)))
                .put("localServers", new JsonObject()))
            .put("clientConfigurations", new JsonObject()
                .put("dual", new JsonObject()
                    .put("enabled", true)
                    .put("connects", new JsonObject()
                        .put("calculator", true)
                        .put("weather", true)))
                .put("single-db", new JsonObject()
                    .put("enabled", true)
                    .put("connects", new JsonObject()
                        .put("database", true)))
                .put("filesystem", new JsonObject()
                    .put("enabled", true)
                    .put("connects", new JsonObject()
                        .put("filesystem", true)))
                .put("local", new JsonObject()
                    .put("enabled", false)))
            .put("toolNaming", new JsonObject()
                .put("usePrefixing", true)
                .put("prefixSeparator", "__")
                .put("supportUnprefixed", true))
            .put("transport", new JsonObject()
                .put("httpTimeout", 30000)
                .put("sseReconnectDelay", 5000)
                .put("stdioBufferSize", 8192));
    }
    
    /**
     * Merge configuration with environment variable overrides
     */
    private JsonObject mergeWithEnvironment(JsonObject config) {
        // Allow environment variables to override specific settings
        
        // Override server ports if specified
        String calcPort = System.getenv("MCP_CALCULATOR_PORT");
        if (calcPort != null) {
            config.getJsonObject("mcpServers")
                .getJsonObject("httpServers")
                .getJsonObject("calculator")
                .put("port", Integer.parseInt(calcPort));
        }
        
        String weatherPort = System.getenv("MCP_WEATHER_PORT");
        if (weatherPort != null) {
            config.getJsonObject("mcpServers")
                .getJsonObject("httpServers")
                .getJsonObject("weather")
                .put("port", Integer.parseInt(weatherPort));
        }
        
        String dbPort = System.getenv("MCP_DATABASE_PORT");
        if (dbPort != null) {
            config.getJsonObject("mcpServers")
                .getJsonObject("httpServers")
                .getJsonObject("database")
                .put("port", Integer.parseInt(dbPort));
        }
        
        String fsPort = System.getenv("MCP_FILESYSTEM_PORT");
        if (fsPort != null) {
            config.getJsonObject("mcpServers")
                .getJsonObject("httpServers")
                .getJsonObject("filesystem")
                .put("port", Integer.parseInt(fsPort));
        }
        
        // Override tool naming settings
        String usePrefixing = System.getenv("MCP_USE_PREFIXING");
        if (usePrefixing != null) {
            config.getJsonObject("toolNaming")
                .put("usePrefixing", Boolean.parseBoolean(usePrefixing));
        }
        
        return config;
    }
    
    /**
     * Get configuration for a specific HTTP server
     */
    public JsonObject getHttpServerConfig(String serverName) {
        if (cachedConfig == null) {
            return null;
        }
        
        return cachedConfig
            .getJsonObject("mcpServers", new JsonObject())
            .getJsonObject("httpServers", new JsonObject())
            .getJsonObject(serverName);
    }
    
    /**
     * Get configuration for local servers
     */
    public JsonObject getLocalServersConfig() {
        if (cachedConfig == null) {
            return new JsonObject();
        }
        
        return cachedConfig
            .getJsonObject("mcpServers", new JsonObject())
            .getJsonObject("localServers", new JsonObject());
    }
    
    /**
     * Check if a server is enabled
     */
    public boolean isServerEnabled(String serverName) {
        JsonObject serverConfig = getHttpServerConfig(serverName);
        if (serverConfig == null) {
            return false;
        }
        return serverConfig.getBoolean("enabled", false);
    }
    
    /**
     * Get port for a server
     */
    public int getServerPort(String serverName, int defaultPort) {
        JsonObject serverConfig = getHttpServerConfig(serverName);
        if (serverConfig == null) {
            return defaultPort;
        }
        return serverConfig.getInteger("port", defaultPort);
    }
    
    /**
     * Get tool naming configuration
     */
    public JsonObject getToolNamingConfig() {
        if (cachedConfig == null) {
            return new JsonObject()
                .put("usePrefixing", true)
                .put("prefixSeparator", "__")
                .put("supportUnprefixed", true);
        }
        
        return cachedConfig.getJsonObject("toolNaming", new JsonObject());
    }
    
    /**
     * Clear cached configuration (useful for testing)
     */
    public void clearCache() {
        cachedConfig = null;
    }
}