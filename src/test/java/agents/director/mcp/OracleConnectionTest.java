package agents.director.mcp;

import agents.director.services.OracleConnectionManager;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Test Oracle database connectivity
 * Run this to verify the Oracle connection is working before proceeding
 */
public class OracleConnectionTest {
    
    /**
     * Read configuration from system properties (loaded from .env.local by dotenv)
     * with a fallback to OS environment variables. Mirrors the lookup used by
     * OracleConnectionManager and the DEFAULT_SCHEMA pattern elsewhere in the codebase.
     */
    private static String env(String key) {
        String value = System.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            value = System.getenv(key);
        }
        return (value == null || value.trim().isEmpty()) ? null : value.trim();
    }

    public static void main(String[] args) {
        System.out.println("=== Oracle Database Connection Test ===");
        System.out.println();

        // Read DB connection details from the same source as production code
        // (System.getProperty from dotenv, falling back to System.getenv).
        // Never hardcode credentials in source.
        String host = env("DB_HOST");
        String port = env("DB_PORT");
        String service = env("DB_SERVICE");
        String user = env("DB_USER");
        String password = env("DB_PASSWORD");

        // This smoke test needs a live Oracle DB and configured credentials.
        // If any are missing, skip cleanly rather than attempting a connection with nulls.
        if (host == null || port == null || service == null || user == null || password == null) {
            System.out.println("Skipped: set DB_* env vars (DB_HOST, DB_PORT, DB_SERVICE, "
                + "DB_USER, DB_PASSWORD) in .env.local or the environment to run this smoke test.");
            System.exit(0);
        }
        System.out.println("[OK] Using configured Oracle credentials from environment");
        System.out.println();

        // Create Vert.x instance
        Vertx vertx = Vertx.vertx();

        // Get connection manager instance
        OracleConnectionManager connManager = OracleConnectionManager.getInstance();

        // Initialize connection pool
        System.out.println("Initializing Oracle connection pool...");
        System.out.println("Host: " + host);
        System.out.println("Port: " + port);
        System.out.println("Service: " + service);
        System.out.println("User: " + user);
        System.out.println();
        
        connManager.initialize(vertx)
            .onSuccess(v -> {
                System.out.println("[OK] Connection pool initialized successfully!");
                System.out.println();
                
                // Test connection
                System.out.println("Testing database connection...");
                connManager.testConnection()
                    .onSuccess(isValid -> {
                        if (isValid) {
                            System.out.println("[OK] Database connection is valid!");
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
                                            System.out.println("[OK] Query executed successfully!");
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
                                                System.out.println("[OK] Oracle JDBC driver loaded");
                                                System.out.println("[OK] Connection pool initialized");
                                                System.out.println("[OK] Database connection valid");
                                                System.out.println("[OK] Query execution working");
                                                
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
                System.err.println("1. Incorrect password configured (check DB_PASSWORD env var)");
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