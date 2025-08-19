package AgentsMCPHost.mcp;

import AgentsMCPHost.mcp.utils.OracleConnectionManager;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Test Oracle database connectivity
 * Run this to verify the Oracle connection is working before proceeding
 */
public class OracleConnectionTest {
    
    public static void main(String[] args) {
        System.out.println("=== Oracle Database Connection Test ===");
        System.out.println();
        
        // Use hardcoded password for consistency
        String password = "ARmy0320-- milk";
        System.out.println("✓ Using configured Oracle password");
        System.out.println();
        
        // Create Vert.x instance
        Vertx vertx = Vertx.vertx();
        
        // Get connection manager instance
        OracleConnectionManager connManager = OracleConnectionManager.getInstance();
        
        // Initialize connection pool
        System.out.println("Initializing Oracle connection pool...");
        System.out.println("Host: adb.us-ashburn-1.oraclecloud.com");
        System.out.println("Port: 1522 (TLS)");
        System.out.println("Service: gd77773c35a7f01_zaksedwtest_high.adb.oraclecloud.com");
        System.out.println("User: ADMIN");
        System.out.println();
        
        connManager.initialize(vertx)
            .onSuccess(v -> {
                System.out.println("✓ Connection pool initialized successfully!");
                System.out.println();
                
                // Test connection
                System.out.println("Testing database connection...");
                connManager.testConnection()
                    .onSuccess(isValid -> {
                        if (isValid) {
                            System.out.println("✓ Database connection is valid!");
                            System.out.println();
                            
                            // Get pool statistics
                            JsonObject stats = connManager.getPoolStatistics();
                            System.out.println("Connection Pool Statistics:");
                            System.out.println("  Available connections: " + stats.getInteger("availableConnections"));
                            System.out.println("  Borrowed connections: " + stats.getInteger("borrowedConnections"));
                            System.out.println("  Total connections: " + stats.getInteger("totalConnections"));
                            System.out.println("  Min pool size: " + stats.getInteger("minPoolSize"));
                            System.out.println("  Max pool size: " + stats.getInteger("maxPoolSize"));
                            System.out.println();
                            
                            // List tables
                            System.out.println("Listing database tables...");
                            connManager.listTables()
                                .onSuccess(tables -> {
                                    if (tables.isEmpty()) {
                                        System.out.println("No tables found in schema. Database needs to be initialized.");
                                        System.out.println("Run the schema.sql script to create tables.");
                                    } else {
                                        System.out.println("Found " + tables.size() + " tables:");
                                        for (int i = 0; i < tables.size(); i++) {
                                            JsonObject table = tables.getJsonObject(i);
                                            System.out.println("  - " + table.getString("name"));
                                        }
                                    }
                                    System.out.println();
                                    
                                    // Test a simple query
                                    System.out.println("Testing simple query...");
                                    connManager.executeQuery("SELECT 1 AS test_value FROM dual")
                                        .onSuccess(results -> {
                                            System.out.println("✓ Query executed successfully!");
                                            System.out.println("Result: " + results.encodePrettily());
                                            System.out.println();
                                            
                                            // Check for enumeration tables
                                            System.out.println("Checking for enumeration tables...");
                                            connManager.executeQuery(
                                                "SELECT table_name FROM user_tables WHERE table_name LIKE '%_ENUM' ORDER BY table_name"
                                            ).onSuccess(enumTables -> {
                                                if (enumTables.isEmpty()) {
                                                    System.out.println("No enumeration tables found.");
                                                } else {
                                                    System.out.println("Found " + enumTables.size() + " enumeration tables:");
                                                    for (int i = 0; i < enumTables.size(); i++) {
                                                        JsonObject row = enumTables.getJsonObject(i);
                                                        System.out.println("  - " + row.getString("TABLE_NAME"));
                                                    }
                                                }
                                                System.out.println();
                                                
                                                // Final summary
                                                System.out.println("=== Connection Test Summary ===");
                                                System.out.println("✓ Oracle JDBC driver loaded");
                                                System.out.println("✓ Connection pool initialized");
                                                System.out.println("✓ Database connection valid");
                                                System.out.println("✓ Query execution working");
                                                
                                                if (tables.isEmpty()) {
                                                    System.out.println();
                                                    System.out.println("Next steps:");
                                                    System.out.println("1. Run schema.sql to create tables");
                                                    System.out.println("2. Run sample-data.sql to populate test data");
                                                    System.out.println("3. Start implementing Oracle MCP servers");
                                                }
                                                
                                                // Shutdown
                                                connManager.shutdown()
                                                    .onSuccess(v2 -> {
                                                        System.out.println();
                                                        System.out.println("Connection pool closed.");
                                                        vertx.close();
                                                        System.exit(0);
                                                    })
                                                    .onFailure(err -> {
                                                        System.err.println("Failed to shutdown: " + err.getMessage());
                                                        System.exit(1);
                                                    });
                                            }).onFailure(err -> {
                                                System.err.println("Failed to check enum tables: " + err.getMessage());
                                                vertx.close();
                                                System.exit(1);
                                            });
                                        })
                                        .onFailure(err -> {
                                            System.err.println("Query failed: " + err.getMessage());
                                            vertx.close();
                                            System.exit(1);
                                        });
                                })
                                .onFailure(err -> {
                                    System.err.println("Failed to list tables: " + err.getMessage());
                                    vertx.close();
                                    System.exit(1);
                                });
                        } else {
                            System.err.println("✗ Database connection is not valid!");
                            vertx.close();
                            System.exit(1);
                        }
                    })
                    .onFailure(err -> {
                        System.err.println("Connection test failed: " + err.getMessage());
                        vertx.close();
                        System.exit(1);
                    });
            })
            .onFailure(err -> {
                System.err.println("✗ Failed to initialize connection pool!");
                System.err.println("Error: " + err.getMessage());
                System.err.println();
                System.err.println("Possible causes:");
                System.err.println("1. Incorrect password configured (check hardcoded value)");
                System.err.println("2. Network connectivity issues");
                System.err.println("3. Oracle Cloud database not accessible");
                System.err.println("4. TLS/SSL configuration issues");
                System.err.println();
                System.err.println("Stack trace:");
                err.printStackTrace();
                vertx.close();
                System.exit(1);
            });
    }
}