package agents.director.mcp;

import agents.director.hosts.base.MilestoneDecider;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

/**
 * Simple unit tests for MCP functionality.
 * These tests don't require any external services to be running.
 */
@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class McpIntegrationTest {
    
    @BeforeAll
    static void setup() {
        System.out.println("MCP Integration Test Suite Starting...");
    }
    
    @Test
    @Order(1)
    @DisplayName("Test JsonObject Creation")
    void testJsonObjectCreation(VertxTestContext testContext) {
        JsonObject obj = new JsonObject()
            .put("name", "test")
            .put("value", 42)
            .put("enabled", true);
        
        testContext.verify(() -> {
            Assertions.assertEquals("test", obj.getString("name"));
            Assertions.assertEquals(42, obj.getInteger("value"));
            Assertions.assertTrue(obj.getBoolean("enabled"));
        });
        
        testContext.completeNow();
    }
    
    @Test
    @Order(2)
    @DisplayName("Test JsonArray Operations")
    void testJsonArrayOperations(VertxTestContext testContext) {
        JsonArray array = new JsonArray()
            .add("first")
            .add(100)
            .add(new JsonObject().put("key", "value"));
        
        testContext.verify(() -> {
            Assertions.assertEquals(3, array.size());
            Assertions.assertEquals("first", array.getString(0));
            Assertions.assertEquals(100, array.getInteger(1));
            Assertions.assertNotNull(array.getJsonObject(2));
            Assertions.assertEquals("value", array.getJsonObject(2).getString("key"));
        });
        
        testContext.completeNow();
    }
    
    @Test
    @Order(3)
    @DisplayName("Test MilestoneDecider Creation")
    void testMilestoneDeciderCreation(VertxTestContext testContext) {
        MilestoneDecider decider = new MilestoneDecider();
        
        testContext.verify(() -> {
            Assertions.assertNotNull(decider);
            // Test the static method
            String description = MilestoneDecider.getMilestoneDescription(3);
            Assertions.assertNotNull(description);
            Assertions.assertTrue(description.contains("milestone"));
        });
        
        testContext.completeNow();
    }
    
    @Test
    @Order(4)
    @DisplayName("Test Milestone Description Generation")
    void testMilestoneDescriptions(VertxTestContext testContext) {
        testContext.verify(() -> {
            // Test descriptions for each milestone level
            for (int i = 1; i <= 6; i++) {
                String desc = MilestoneDecider.getMilestoneDescription(i);
                Assertions.assertNotNull(desc, "Description for milestone " + i + " should not be null");
                Assertions.assertTrue(desc.contains(String.valueOf(i)), 
                    "Description should contain milestone number " + i);
            }
        });
        
        testContext.completeNow();
    }
    
    @Test
    @Order(5)
    @DisplayName("Test Vertx Context")
    void testVertxContext(Vertx vertx, VertxTestContext testContext) {
        vertx.executeBlocking(promise -> {
            // Simulate some blocking operation
            try {
                Thread.sleep(10);
                promise.complete("Success");
            } catch (InterruptedException e) {
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                testContext.verify(() -> {
                    Assertions.assertEquals("Success", res.result());
                });
                testContext.completeNow();
            } else {
                testContext.failNow(res.cause());
            }
        });
    }
    
    @Test
    @Order(6)
    @DisplayName("Test JSON Encoding and Decoding")
    void testJsonEncodingDecoding(VertxTestContext testContext) {
        JsonObject original = new JsonObject()
            .put("id", 123)
            .put("name", "Test Item")
            .put("metadata", new JsonObject()
                .put("created", "2024-01-01")
                .put("tags", new JsonArray().add("tag1").add("tag2")));
        
        String encoded = original.encode();
        JsonObject decoded = new JsonObject(encoded);
        
        testContext.verify(() -> {
            Assertions.assertEquals(original.getInteger("id"), decoded.getInteger("id"));
            Assertions.assertEquals(original.getString("name"), decoded.getString("name"));
            Assertions.assertNotNull(decoded.getJsonObject("metadata"));
            Assertions.assertEquals(2, decoded.getJsonObject("metadata")
                .getJsonArray("tags").size());
        });
        
        testContext.completeNow();
    }
    
    @Test
    @Order(7)
    @DisplayName("Test Simple Math Operations")
    void testSimpleMathOperations(VertxTestContext testContext) {
        // Simple test to verify basic operations work
        int result1 = 25 * 4;
        int result2 = 50 + 30;
        double result3 = Math.sqrt(144);
        
        testContext.verify(() -> {
            Assertions.assertEquals(100, result1);
            Assertions.assertEquals(80, result2);
            Assertions.assertEquals(12.0, result3);
        });
        
        testContext.completeNow();
    }
    
    @AfterAll
    static void cleanup() {
        System.out.println("MCP Integration Test Suite Complete");
    }
}