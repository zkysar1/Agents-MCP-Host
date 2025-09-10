package agents.director.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for AgentConfigurationRegistry
 */
class AgentConfigurationRegistryTest {
    
    @TempDir
    Path tempDir;
    
    private AgentConfigurationRegistry registry;
    
    @BeforeEach
    void setUp() {
        registry = AgentConfigurationRegistry.builder()
            .withAgentsDirectory(tempDir.toString())
            .withHotReload(false) // Disable for testing
            .build();
    }
    
    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.shutdown();
        }
    }
    
    @Test
    void testInitializeEmptyRegistry() throws IOException {
        registry.initialize();
        
        assertTrue(registry.getRegisteredAgentTypes().isEmpty());
        assertEquals(0, registry.getStatistics().getRegisteredAgents());
    }
    
    @Test
    void testDynamicRegistration() throws IOException {
        registry.initialize();
        
        // Create a test configuration
        AgentConfiguration config = new AgentConfiguration("test-agent", "Test Agent", "A test agent");
        config.enableManager(ManagerType.INTENT_ANALYSIS);
        config.setAllowToolUse(true);
        config.setMaxComplexity(5);
        
        // Register the configuration
        boolean success = registry.registerConfiguration(config);
        assertTrue(success);
        
        // Verify registration
        assertTrue(registry.isRegistered("test-agent"));
        assertEquals(1, registry.getRegisteredAgentTypes().size());
        
        // Retrieve the configuration
        AgentConfiguration retrieved = registry.getConfiguration("test-agent");
        assertNotNull(retrieved);
        assertEquals("Test Agent", retrieved.getDisplayName());
        assertTrue(retrieved.isManagerEnabled(ManagerType.INTENT_ANALYSIS));
    }
    
    @Test
    void testUnregistration() throws IOException {
        registry.initialize();
        
        // Register a configuration
        AgentConfiguration config = new AgentConfiguration("temp-agent", "Temp Agent", "Temporary agent");
        registry.registerConfiguration(config);
        assertTrue(registry.isRegistered("temp-agent"));
        
        // Unregister it
        boolean success = registry.unregisterConfiguration("temp-agent");
        assertTrue(success);
        assertFalse(registry.isRegistered("temp-agent"));
        assertNull(registry.getConfiguration("temp-agent"));
    }
    
    @Test
    void testConfigurationValidation() throws IOException {
        registry.initialize();
        
        // Try to register an invalid configuration
        AgentConfiguration invalidConfig = new AgentConfiguration();
        // Missing required fields: agentType, displayName, description
        
        boolean success = registry.registerConfiguration(invalidConfig);
        assertFalse(success);
    }
    
    @Test
    void testFiltering() throws IOException {
        registry.initialize();
        
        // Register multiple configurations with different managers
        AgentConfiguration config1 = new AgentConfiguration("agent1", "Agent 1", "First agent");
        config1.enableManager(ManagerType.INTENT_ANALYSIS);
        config1.enableManager(ManagerType.ORACLE_EXECUTION);
        
        AgentConfiguration config2 = new AgentConfiguration("agent2", "Agent 2", "Second agent");
        config2.enableManager(ManagerType.INTENT_ANALYSIS);
        config2.enableManager(ManagerType.SCHEMA_INTELLIGENCE);
        
        AgentConfiguration config3 = new AgentConfiguration("agent3", "Agent 3", "Third agent");
        config3.enableManager(ManagerType.ORACLE_EXECUTION);
        config3.enableManager(ManagerType.SCHEMA_INTELLIGENCE);
        
        registry.registerConfiguration(config1);
        registry.registerConfiguration(config2);
        registry.registerConfiguration(config3);
        
        // Filter by required managers
        Set<ManagerType> requiredManagers = Set.of(ManagerType.INTENT_ANALYSIS);
        Map<String, AgentConfiguration> filtered = registry.getConfigurationsWithManagers(requiredManagers);
        
        assertEquals(2, filtered.size());
        assertTrue(filtered.containsKey("agent1"));
        assertTrue(filtered.containsKey("agent2"));
        assertFalse(filtered.containsKey("agent3"));
    }
    
    @Test
    void testLoadConfigurationFromJson() throws IOException {
        // Create a temporary JSON file
        String jsonContent = """
            {
              "agentType": "json-test-agent",
              "displayName": "JSON Test Agent",
              "description": "Agent loaded from JSON",
              "backstory": "I am a test agent loaded from JSON",
              "enabledManagers": ["IntentAnalysisManager", "MCPClientManager"],
              "allowExecution": false,
              "allowToolUse": true,
              "requireValidation": true,
              "maxComplexity": 7,
              "defaultStrategy": "test-strategy",
              "allowedStrategies": ["test-strategy", "backup-strategy"]
            }
            """;
        
        Path configFile = tempDir.resolve("json-test-agent.json");
        Files.writeString(configFile, jsonContent);
        
        // Initialize registry and load configurations
        registry.initialize();
        
        // Verify the configuration was loaded
        assertTrue(registry.isRegistered("json-test-agent"));
        
        AgentConfiguration loaded = registry.getConfiguration("json-test-agent");
        assertNotNull(loaded);
        assertEquals("JSON Test Agent", loaded.getDisplayName());
        assertEquals("Agent loaded from JSON", loaded.getDescription());
        assertEquals("I am a test agent loaded from JSON", loaded.getBackstory());
        assertFalse(loaded.isAllowExecution());
        assertTrue(loaded.isAllowToolUse());
        assertEquals(7, loaded.getMaxComplexity());
        assertEquals("test-strategy", loaded.getDefaultStrategy());
        assertTrue(loaded.isManagerEnabled(ManagerType.INTENT_ANALYSIS));
        assertTrue(loaded.isManagerEnabled(ManagerType.MCP_CLIENT));
        assertEquals(2, loaded.getAllowedStrategies().size());
    }
    
    @Test
    void testRegistryStatistics() throws IOException {
        registry.initialize();
        
        // Register some configurations
        AgentConfiguration config1 = new AgentConfiguration("stats1", "Stats 1", "First stats agent");
        AgentConfiguration config2 = new AgentConfiguration("stats2", "Stats 2", "Second stats agent");
        
        registry.registerConfiguration(config1);
        registry.registerConfiguration(config2);
        
        // Check statistics
        AgentConfigurationRegistry.RegistryStatistics stats = registry.getStatistics();
        assertEquals(2, stats.getRegisteredAgents());
        assertEquals(2, stats.getCachedConfigurations());
        assertFalse(stats.isHotReloadEnabled()); // Disabled for testing
    }
}