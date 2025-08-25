package AgentsMCPHost.api;

import AgentsMCPHost.mcp.servers.oracle.utils.OracleConnectionManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Test endpoints for diagnostics and troubleshooting.
 * Provides manual testing capabilities for various components.
 */
public class Test extends AbstractVerticle {
    
    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        // Simply mark as complete - router setup happens via static method
        startPromise.complete();
    }
    
    /**
     * Configure the router with test endpoints
     * @param parentRouter The parent router to attach to
     */
    public static void setRouter(Router parentRouter) {
        // Oracle connection test endpoint
        parentRouter.get("/host/v1/test/oracle").handler(Test::handleOracleTest);
        parentRouter.post("/host/v1/test/oracle/reconnect").handler(Test::handleOracleReconnect);
    }
    
    /**
     * Test Oracle database connection
     */
    private static void handleOracleTest(RoutingContext ctx) {
        JsonObject result = new JsonObject();
        
        try {
            OracleConnectionManager oracleManager = OracleConnectionManager.getInstance();
            
            // Get connection status
            JsonObject connectionStatus = oracleManager.getConnectionStatus();
            result.put("connectionStatus", connectionStatus);
            
            // Try a test query if connection is healthy
            if (connectionStatus.getBoolean("healthy", false)) {
                result.put("testQuery", performTestQuery(oracleManager));
            } else {
                result.put("testQuery", new JsonObject()
                    .put("success", false)
                    .put("error", "Connection not healthy"));
            }
            
            // Add configuration info
            result.put("configuration", new JsonObject()
                .put("host", "adb.us-ashburn-1.oraclecloud.com")
                .put("port", 1522)
                .put("service", "gd77773c35a7f01_zaksedwtest_high.adb.oraclecloud.com")
                .put("user", "ADMIN")
                .put("passwordSource", System.getenv("ORACLE_TESTING_DATABASE_PASSWORD") != null ? 
                    "environment" : "default-test"));
            
            // Add troubleshooting suggestions
            JsonArray suggestions = new JsonArray();
            if (!connectionStatus.getBoolean("healthy", false)) {
                String lastError = connectionStatus.getString("lastError", "");
                
                if (lastError.contains("UCP-0")) {
                    suggestions.add("Check Oracle JDBC and UCP libraries are in classpath");
                    suggestions.add("Verify network connectivity to Oracle Cloud");
                    suggestions.add("Check firewall allows outbound port 1522");
                    suggestions.add("Ensure TLS is enabled in Oracle Cloud ACL settings");
                } else if (lastError.contains("ORA-12506")) {
                    suggestions.add("Enable TLS authentication in Oracle Cloud Console");
                    suggestions.add("Check Autonomous Database ACL configuration");
                } else if (lastError.contains("ORA-01017")) {
                    suggestions.add("Verify username and password are correct");
                    suggestions.add("Check ORACLE_TESTING_DATABASE_PASSWORD environment variable");
                } else if (lastError.contains("timeout")) {
                    suggestions.add("Check network connectivity and latency");
                    suggestions.add("Verify Oracle Cloud region is accessible");
                }
            }
            result.put("suggestions", suggestions);
            
        } catch (Exception e) {
            result.put("error", "Failed to test Oracle connection: " + e.getMessage());
            result.put("connectionStatus", new JsonObject()
                .put("healthy", false)
                .put("error", e.getMessage()));
        }
        
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .setStatusCode(200)
            .end(result.encodePrettily());
    }
    
    /**
     * Attempt to reconnect to Oracle
     */
    private static void handleOracleReconnect(RoutingContext ctx) {
        JsonObject result = new JsonObject();
        
        try {
            OracleConnectionManager oracleManager = OracleConnectionManager.getInstance();
            
            // Get current status
            JsonObject beforeStatus = oracleManager.getConnectionStatus();
            result.put("beforeStatus", beforeStatus);
            
            // Attempt reconnection
            oracleManager.attemptReconnection()
                .onSuccess(v -> {
                    JsonObject afterStatus = oracleManager.getConnectionStatus();
                    result.put("afterStatus", afterStatus);
                    result.put("success", true);
                    result.put("message", "Reconnection attempt completed");
                    
                    ctx.response()
                        .putHeader("Content-Type", "application/json")
                        .setStatusCode(200)
                        .end(result.encodePrettily());
                })
                .onFailure(err -> {
                    JsonObject afterStatus = oracleManager.getConnectionStatus();
                    result.put("afterStatus", afterStatus);
                    result.put("success", false);
                    result.put("error", err.getMessage());
                    
                    ctx.response()
                        .putHeader("Content-Type", "application/json")
                        .setStatusCode(200)
                        .end(result.encodePrettily());
                });
                
        } catch (Exception e) {
            result.put("error", "Failed to attempt reconnection: " + e.getMessage());
            result.put("success", false);
            
            ctx.response()
                .putHeader("Content-Type", "application/json")
                .setStatusCode(500)
                .end(result.encodePrettily());
        }
    }
    
    /**
     * Perform a simple test query
     */
    private static JsonObject performTestQuery(OracleConnectionManager oracleManager) {
        JsonObject result = new JsonObject();
        
        try {
            long startTime = System.currentTimeMillis();
            
            // Use executeQuery method with a simple query
            oracleManager.executeQuery("SELECT 'Connection Test' as status, SYSDATE as current_time FROM DUAL")
                .onSuccess(rows -> {
                    long elapsed = System.currentTimeMillis() - startTime;
                    
                    result.put("success", true);
                    result.put("queryTime", elapsed + "ms");
                    result.put("rowCount", rows.size());
                    
                    if (!rows.isEmpty()) {
                        result.put("result", rows.getJsonObject(0));
                    }
                })
                .onFailure(err -> {
                    result.put("success", false);
                    result.put("error", err.getMessage());
                });
                
            // Wait briefly for async result (not ideal but ok for test endpoint)
            Thread.sleep(100);
            
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", "Test query failed: " + e.getMessage());
        }
        
        return result;
    }
}