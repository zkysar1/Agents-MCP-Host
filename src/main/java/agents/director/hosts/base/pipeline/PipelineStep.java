package agents.director.hosts.base.pipeline;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.Set;
import java.util.HashSet;
import java.util.function.Function;

/**
 * Represents an individual step in the manager pipeline.
 * Each step encapsulates the execution logic, dependencies, and metadata
 * needed to perform a specific operation in the pipeline.
 */
public class PipelineStep {
    
    private final String id;
    private final String name;
    private final String description;
    private final String managerType;
    private final String toolName;
    private final String serverName;
    private final Set<String> dependencies;
    private final boolean optional;
    private final int priority;
    private final long timeoutMs;
    private final int maxRetries;
    private final JsonObject configuration;
    
    // Execution function that takes context and returns Future<JsonObject>
    private final Function<ExecutionContext, Future<JsonObject>> executor;
    
    // Validation function to check if step should be executed
    private final Function<ExecutionContext, Boolean> condition;
    
    // Function to build arguments for the step based on context
    private final Function<ExecutionContext, JsonObject> argumentBuilder;
    
    private PipelineStep(Builder builder) {
        this.id = builder.id;
        this.name = builder.name;
        this.description = builder.description;
        this.managerType = builder.managerType;
        this.toolName = builder.toolName;
        this.serverName = builder.serverName;
        this.dependencies = new HashSet<>(builder.dependencies);
        this.optional = builder.optional;
        this.priority = builder.priority;
        this.timeoutMs = builder.timeoutMs;
        this.maxRetries = builder.maxRetries;
        this.configuration = builder.configuration.copy();
        this.executor = builder.executor;
        this.condition = builder.condition;
        this.argumentBuilder = builder.argumentBuilder;
    }
    
    // Getters
    public String getId() { return id; }
    public String getName() { return name; }
    public String getDescription() { return description; }
    public String getManagerType() { return managerType; }
    public String getToolName() { return toolName; }
    public String getServerName() { return serverName; }
    public Set<String> getDependencies() { return new HashSet<>(dependencies); }
    public boolean isOptional() { return optional; }
    public int getPriority() { return priority; }
    public long getTimeoutMs() { return timeoutMs; }
    public int getMaxRetries() { return maxRetries; }
    public JsonObject getConfiguration() { return configuration.copy(); }
    
    /**
     * Check if this step should be executed based on the current context
     */
    public boolean shouldExecute(ExecutionContext context) {
        if (condition != null) {
            try {
                return condition.apply(context);
            } catch (Exception e) {
                // If condition evaluation fails, execute if required, skip if optional
                return !optional;
            }
        }
        return true; // Default to execute
    }
    
    /**
     * Check if all dependencies are satisfied
     */
    public boolean areDependenciesSatisfied(ExecutionContext context) {
        for (String dependency : dependencies) {
            if (!context.hasStepResult(dependency)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Build arguments for this step based on the execution context
     */
    public JsonObject buildArguments(ExecutionContext context) {
        if (argumentBuilder != null) {
            try {
                JsonObject args = argumentBuilder.apply(context);
                return args != null ? args : new JsonObject();
            } catch (Exception e) {
                // If argument building fails, return minimal arguments
                return createMinimalArguments(context);
            }
        }
        return createMinimalArguments(context);
    }
    
    /**
     * Execute this step
     */
    public Future<JsonObject> execute(ExecutionContext context) {
        if (executor != null) {
            return executor.apply(context);
        }
        // Default implementation - should not happen if properly built
        return Future.failedFuture("No executor defined for step: " + id);
    }
    
    /**
     * Get step metadata as JsonObject
     */
    public JsonObject toJsonObject() {
        return new JsonObject()
            .put("id", id)
            .put("name", name)
            .put("description", description)
            .put("managerType", managerType)
            .put("toolName", toolName)
            .put("serverName", serverName)
            .put("dependencies", new JsonArray(dependencies.stream().toList()))
            .put("optional", optional)
            .put("priority", priority)
            .put("timeoutMs", timeoutMs)
            .put("maxRetries", maxRetries)
            .put("configuration", configuration);
    }
    
    private JsonObject createMinimalArguments(ExecutionContext context) {
        JsonObject args = new JsonObject();
        
        // Add query if available
        if (context.getOriginalQuery() != null) {
            args.put("query", context.getOriginalQuery());
        }
        
        // Add basic context
        args.put("context", new JsonObject()
            .put("conversationId", context.getConversationId())
            .put("sessionId", context.getSessionId()));
        
        return args;
    }
    
    @Override
    public String toString() {
        return String.format("PipelineStep[id=%s, manager=%s, tool=%s, optional=%b, deps=%d]",
            id, managerType, toolName, optional, dependencies.size());
    }
    
    /**
     * Builder class for PipelineStep
     */
    public static class Builder {
        private String id;
        private String name;
        private String description;
        private String managerType;
        private String toolName;
        private String serverName;
        private Set<String> dependencies = new HashSet<>();
        private boolean optional = false;
        private int priority = 5;
        private long timeoutMs = 30000; // 30 seconds default
        private int maxRetries = 1;
        private JsonObject configuration = new JsonObject();
        private Function<ExecutionContext, Future<JsonObject>> executor;
        private Function<ExecutionContext, Boolean> condition;
        private Function<ExecutionContext, JsonObject> argumentBuilder;
        
        public Builder(String id) {
            this.id = id;
            this.name = id; // Default name to id
        }
        
        public Builder name(String name) {
            this.name = name;
            return this;
        }
        
        public Builder description(String description) {
            this.description = description;
            return this;
        }
        
        public Builder managerType(String managerType) {
            this.managerType = managerType;
            return this;
        }
        
        public Builder toolName(String toolName) {
            this.toolName = toolName;
            return this;
        }
        
        public Builder serverName(String serverName) {
            this.serverName = serverName;
            return this;
        }
        
        public Builder addDependency(String dependency) {
            this.dependencies.add(dependency);
            return this;
        }
        
        public Builder dependencies(String... dependencies) {
            for (String dep : dependencies) {
                this.dependencies.add(dep);
            }
            return this;
        }
        
        public Builder optional(boolean optional) {
            this.optional = optional;
            return this;
        }
        
        public Builder optional() {
            return optional(true);
        }
        
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }
        
        public Builder timeout(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }
        
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }
        
        public Builder configuration(JsonObject configuration) {
            this.configuration = configuration != null ? configuration.copy() : new JsonObject();
            return this;
        }
        
        public Builder executor(Function<ExecutionContext, Future<JsonObject>> executor) {
            this.executor = executor;
            return this;
        }
        
        public Builder condition(Function<ExecutionContext, Boolean> condition) {
            this.condition = condition;
            return this;
        }
        
        public Builder argumentBuilder(Function<ExecutionContext, JsonObject> argumentBuilder) {
            this.argumentBuilder = argumentBuilder;
            return this;
        }
        
        public PipelineStep build() {
            if (id == null || id.trim().isEmpty()) {
                throw new IllegalArgumentException("Step id cannot be null or empty");
            }
            if (executor == null) {
                throw new IllegalArgumentException("Executor cannot be null for step: " + id);
            }
            return new PipelineStep(this);
        }
    }
    
    // Static factory methods for common step types
    
    /**
     * Create a step that uses a manager to call an MCP tool
     */
    public static Builder managerTool(String id, String managerType, String toolName) {
        return new Builder(id)
            .managerType(managerType)
            .toolName(toolName);
    }
    
    /**
     * Create a step for schema intelligence operations
     */
    public static Builder schemaStep(String id, String toolName) {
        return managerTool(id, "SchemaIntelligenceManager", toolName)
            .serverName("OracleSchemaIntelligence");
    }
    
    /**
     * Create a step for intent analysis operations
     */
    public static Builder intentStep(String id, String toolName) {
        return managerTool(id, "IntentAnalysisManager", toolName)
            .serverName("IntentAnalysis");
    }
    
    /**
     * Create a step for SQL pipeline operations
     */
    public static Builder sqlStep(String id, String toolName, String serverName) {
        return managerTool(id, "SQLPipelineManager", toolName)
            .serverName(serverName);
    }
    
    /**
     * Create a step for execution operations
     */
    public static Builder executionStep(String id, String toolName) {
        return managerTool(id, "OracleExecutionManager", toolName)
            .serverName("OracleQueryExecution");
    }
    
    /**
     * Create a step for strategy orchestration operations
     */
    public static Builder strategyStep(String id, String toolName) {
        return managerTool(id, "StrategyOrchestrationManager", toolName)
            .serverName("StrategyOrchestrator");
    }
}