package agents.director.mcp;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

/**
 * Integration tests for MCP functionality.
 */
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class McpIntegrationTest {
    
    private static WebClient webClient;
    private static final String BASE_URL = "http://localhost:8080";
    
    @BeforeAll
    static void setup(Vertx vertx) {
        webClient = WebClient.create(vertx);
        System.out.println("MCP Integration Test Suite Starting...");
    }
    
    @Test
    @Order(1)
    @DisplayName("Test Calculator Tool")
    void testCalculatorTool(Vertx vertx, VertxTestContext testContext) throws Throwable {
        JsonObject request = new JsonObject()
            .put("use_mcp", true)
            .put("messages", new JsonArray()
                .add(new JsonObject()
                    .put("role", "user")
                    .put("content", "Calculate 25 multiplied by 4")));
        
        webClient.post(8080, "localhost", "/host/v1/conversations")
            .sendJsonObject(request, ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result().bodyAsJsonObject();
                    System.out.println("Calculator response: " + response.encodePrettily());
                    
                    // Verify response contains calculation result
                    testContext.verify(() -> {
                        Assertions.assertNotNull(response.getJsonArray("choices"));
                        JsonObject message = response.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message");
                        String content = message.getString("content");
                        Assertions.assertTrue(content.contains("100"), 
                            "Response should contain the result 100");
                    });
                    
                    testContext.completeNow();
                } else {
                    testContext.failNow(ar.cause());
                }
            });
        
        testContext.awaitCompletion(10, TimeUnit.SECONDS);
    }
    
    @Test
    @Order(2)
    @DisplayName("Test Weather Tool")
    void testWeatherTool(Vertx vertx, VertxTestContext testContext) throws Throwable {
        JsonObject request = new JsonObject()
            .put("use_mcp", true)
            .put("messages", new JsonArray()
                .add(new JsonObject()
                    .put("role", "user")
                    .put("content", "Get weather alerts for California")));
        
        webClient.post(8080, "localhost", "/host/v1/conversations")
            .sendJsonObject(request, ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result().bodyAsJsonObject();
                    System.out.println("Weather response: " + response.encodePrettily());
                    
                    testContext.verify(() -> {
                        Assertions.assertNotNull(response.getJsonArray("choices"));
                        JsonObject message = response.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message");
                        String content = message.getString("content");
                        Assertions.assertTrue(
                            content.contains("alert") || content.contains("weather") || content.contains("CA"),
                            "Response should contain weather information");
                    });
                    
                    testContext.completeNow();
                } else {
                    testContext.failNow(ar.cause());
                }
            });
        
        testContext.awaitCompletion(10, TimeUnit.SECONDS);
    }
    
    @Test
    @Order(3)
    @DisplayName("Test File System Tool")
    void testFileSystemTool(Vertx vertx, VertxTestContext testContext) throws Throwable {
        JsonObject request = new JsonObject()
            .put("use_mcp", true)
            .put("messages", new JsonArray()
                .add(new JsonObject()
                    .put("role", "user")
                    .put("content", "Create a directory called test-mcp-dir")));
        
        webClient.post(8080, "localhost", "/host/v1/conversations")
            .sendJsonObject(request, ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result().bodyAsJsonObject();
                    System.out.println("FileSystem response: " + response.encodePrettily());
                    
                    testContext.verify(() -> {
                        Assertions.assertNotNull(response.getJsonArray("choices"));
                        JsonObject message = response.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message");
                        String content = message.getString("content");
                        Assertions.assertTrue(
                            content.contains("directory") || content.contains("created") || content.contains("test-mcp-dir"),
                            "Response should confirm directory creation");
                    });
                    
                    testContext.completeNow();
                } else {
                    testContext.failNow(ar.cause());
                }
            });
        
        testContext.awaitCompletion(10, TimeUnit.SECONDS);
    }
    
    @Test
    @Order(4)
    @DisplayName("Test Database Tool")
    void testDatabaseTool(Vertx vertx, VertxTestContext testContext) throws Throwable {
        JsonObject request = new JsonObject()
            .put("use_mcp", true)
            .put("messages", new JsonArray()
                .add(new JsonObject()
                    .put("role", "user")
                    .put("content", "List all tables in the database")));
        
        webClient.post(8080, "localhost", "/host/v1/conversations")
            .sendJsonObject(request, ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result().bodyAsJsonObject();
                    System.out.println("Database response: " + response.encodePrettily());
                    
                    testContext.verify(() -> {
                        Assertions.assertNotNull(response.getJsonArray("choices"));
                        JsonObject message = response.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message");
                        String content = message.getString("content");
                        Assertions.assertTrue(
                            content.contains("users") || content.contains("products") || content.contains("table"),
                            "Response should contain table information");
                    });
                    
                    testContext.completeNow();
                } else {
                    testContext.failNow(ar.cause());
                }
            });
        
        testContext.awaitCompletion(10, TimeUnit.SECONDS);
    }
    
    @Test
    @Order(5)
    @DisplayName("Test Multi-Tool Workflow")
    void testMultiToolWorkflow(Vertx vertx, VertxTestContext testContext) throws Throwable {
        JsonObject request = new JsonObject()
            .put("use_mcp", true)
            .put("messages", new JsonArray()
                .add(new JsonObject()
                    .put("role", "user")
                    .put("content", "Calculate 50 plus 30 and save the result to a file called sum.txt")));
        
        webClient.post(8080, "localhost", "/host/v1/conversations")
            .sendJsonObject(request, ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result().bodyAsJsonObject();
                    System.out.println("Multi-tool response: " + response.encodePrettily());
                    
                    testContext.verify(() -> {
                        Assertions.assertNotNull(response.getJsonArray("choices"));
                        JsonObject message = response.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message");
                        String content = message.getString("content");
                        Assertions.assertTrue(
                            content.contains("80") || content.contains("saved") || content.contains("sum.txt"),
                            "Response should indicate calculation and file saving");
                    });
                    
                    testContext.completeNow();
                } else {
                    testContext.failNow(ar.cause());
                }
            });
        
        testContext.awaitCompletion(10, TimeUnit.SECONDS);
    }
    
    @Test
    @Order(6)
    @DisplayName("Test MCP Auto-Detection")
    void testMcpAutoDetection(Vertx vertx, VertxTestContext testContext) throws Throwable {
        // Don't explicitly set use_mcp, let it auto-detect
        JsonObject request = new JsonObject()
            .put("messages", new JsonArray()
                .add(new JsonObject()
                    .put("role", "user")
                    .put("content", "Calculate the square root of 144"))); // Should trigger MCP
        
        webClient.post(8080, "localhost", "/host/v1/conversations")
            .sendJsonObject(request, ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result().bodyAsJsonObject();
                    System.out.println("Auto-detection response: " + response.encodePrettily());
                    
                    testContext.verify(() -> {
                        Assertions.assertNotNull(response.getJsonArray("choices"));
                        // Should get a response whether MCP was used or not
                        JsonObject message = response.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message");
                        Assertions.assertNotNull(message.getString("content"));
                    });
                    
                    testContext.completeNow();
                } else {
                    testContext.failNow(ar.cause());
                }
            });
        
        testContext.awaitCompletion(10, TimeUnit.SECONDS);
    }
    
    @Test
    @Order(7)
    @DisplayName("Test Standard LLM (No MCP)")
    void testStandardLlm(Vertx vertx, VertxTestContext testContext) throws Throwable {
        JsonObject request = new JsonObject()
            .put("use_mcp", false) // Explicitly disable MCP
            .put("messages", new JsonArray()
                .add(new JsonObject()
                    .put("role", "user")
                    .put("content", "What is the meaning of life?")));
        
        webClient.post(8080, "localhost", "/host/v1/conversations")
            .sendJsonObject(request, ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result().bodyAsJsonObject();
                    System.out.println("Standard LLM response: " + response.encodePrettily());
                    
                    testContext.verify(() -> {
                        Assertions.assertNotNull(response.getJsonArray("choices"));
                        JsonObject message = response.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message");
                        Assertions.assertNotNull(message.getString("content"));
                        // This should be a philosophical answer, not a tool result
                    });
                    
                    testContext.completeNow();
                } else {
                    testContext.failNow(ar.cause());
                }
            });
        
        testContext.awaitCompletion(10, TimeUnit.SECONDS);
    }
    
    @AfterAll
    static void cleanup() {
        if (webClient != null) {
            webClient.close();
        }
        System.out.println("MCP Integration Test Suite Complete");
    }
}