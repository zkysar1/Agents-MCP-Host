package agents.director.hosts.base.pipeline;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Configuration class for the manager pipeline system.
 * Defines pipeline execution policies, step configurations, and fallback strategies.
 */
public class PipelineConfiguration {
    
    private final String pipelineId;
    private final String name;
    private final String description;
    private final List<PipelineStep> steps;
    private final ExecutionPolicy executionPolicy;
    private final JsonObject globalConfiguration;
    
    // Error handling configuration
    private final boolean continueOnOptionalFailure;
    private final boolean enableFallback;
    private final List<String> fallbackStrategies;
    
    // Performance configuration  
    private final long globalTimeoutMs;
    private final int maxConcurrentSteps;
    private final boolean enableStreaming;
    private final long streamingIntervalMs;
    
    // Adaptation configuration
    private final boolean enableAdaptation;
    private final int adaptationCheckInterval; // Check every N steps
    private final double adaptationThreshold; // Confidence threshold for adaptation
    
    private PipelineConfiguration(Builder builder) {
        this.pipelineId = builder.pipelineId;
        this.name = builder.name;
        this.description = builder.description;
        this.steps = Collections.unmodifiableList(new ArrayList<>(builder.steps));
        this.executionPolicy = builder.executionPolicy;
        this.globalConfiguration = builder.globalConfiguration.copy();
        this.continueOnOptionalFailure = builder.continueOnOptionalFailure;
        this.enableFallback = builder.enableFallback;
        this.fallbackStrategies = Collections.unmodifiableList(new ArrayList<>(builder.fallbackStrategies));
        this.globalTimeoutMs = builder.globalTimeoutMs;
        this.maxConcurrentSteps = builder.maxConcurrentSteps;
        this.enableStreaming = builder.enableStreaming;
        this.streamingIntervalMs = builder.streamingIntervalMs;
        this.enableAdaptation = builder.enableAdaptation;
        this.adaptationCheckInterval = builder.adaptationCheckInterval;
        this.adaptationThreshold = builder.adaptationThreshold;
    }
    
    // Getters
    public String getPipelineId() { return pipelineId; }
    public String getName() { return name; }
    public String getDescription() { return description; }
    public List<PipelineStep> getSteps() { return steps; }
    public ExecutionPolicy getExecutionPolicy() { return executionPolicy; }
    public JsonObject getGlobalConfiguration() { return globalConfiguration.copy(); }
    public boolean isContinueOnOptionalFailure() { return continueOnOptionalFailure; }
    public boolean isEnableFallback() { return enableFallback; }
    public List<String> getFallbackStrategies() { return fallbackStrategies; }
    public long getGlobalTimeoutMs() { return globalTimeoutMs; }
    public int getMaxConcurrentSteps() { return maxConcurrentSteps; }
    public boolean isEnableStreaming() { return enableStreaming; }
    public long getStreamingIntervalMs() { return streamingIntervalMs; }
    public boolean isEnableAdaptation() { return enableAdaptation; }
    public int getAdaptationCheckInterval() { return adaptationCheckInterval; }
    public double getAdaptationThreshold() { return adaptationThreshold; }
    
    /**
     * Get steps that can be executed (dependencies satisfied)
     */
    public List<PipelineStep> getExecutableSteps(ExecutionContext context) {
        return steps.stream()
            .filter(step -> step.shouldExecute(context))
            .filter(step -> step.areDependenciesSatisfied(context))
            .filter(step -> !context.hasStepResult(step.getId()))
            .sorted(Comparator.comparingInt(PipelineStep::getPriority).reversed())
            .collect(Collectors.toList());
    }
    
    /**
     * Get the next step to execute based on execution policy
     */
    public Optional<PipelineStep> getNextStep(ExecutionContext context) {
        List<PipelineStep> executable = getExecutableSteps(context);
        
        if (executable.isEmpty()) {
            return Optional.empty();
        }
        
        switch (executionPolicy) {
            case SEQUENTIAL:
                // Find the first unexecuted step in order
                for (PipelineStep step : steps) {
                    if (!context.hasStepResult(step.getId()) && 
                        step.shouldExecute(context) && 
                        step.areDependenciesSatisfied(context)) {
                        return Optional.of(step);
                    }
                }
                return Optional.empty();
                
            case PRIORITY_BASED:
                // Return highest priority executable step
                return executable.stream()
                    .max(Comparator.comparingInt(PipelineStep::getPriority));
                
            case DEPENDENCY_BASED:
                // Return step with all dependencies satisfied and highest priority
                return executable.stream()
                    .filter(step -> step.areDependenciesSatisfied(context))
                    .max(Comparator.comparingInt(PipelineStep::getPriority));
                
            default:
                return Optional.of(executable.get(0));
        }
    }
    
    /**
     * Check if pipeline execution is complete
     */
    public boolean isComplete(ExecutionContext context) {
        // Check if all required steps are complete
        return steps.stream()
            .filter(step -> !step.isOptional())
            .filter(step -> step.shouldExecute(context))
            .allMatch(step -> context.hasStepResult(step.getId()));
    }
    
    /**
     * Get pipeline progress (0.0 to 1.0)
     */
    public double getProgress(ExecutionContext context) {
        long totalSteps = steps.stream()
            .filter(step -> step.shouldExecute(context))
            .count();
        
        if (totalSteps == 0) {
            return 1.0;
        }
        
        long completedSteps = steps.stream()
            .filter(step -> step.shouldExecute(context))
            .filter(step -> context.hasStepResult(step.getId()))
            .count();
        
        return (double) completedSteps / totalSteps;
    }
    
    /**
     * Get remaining steps
     */
    public List<PipelineStep> getRemainingSteps(ExecutionContext context) {
        return steps.stream()
            .filter(step -> step.shouldExecute(context))
            .filter(step -> !context.hasStepResult(step.getId()))
            .collect(Collectors.toList());
    }
    
    /**
     * Validate configuration
     */
    public List<String> validate() {
        List<String> errors = new ArrayList<>();
        
        if (pipelineId == null || pipelineId.trim().isEmpty()) {
            errors.add("Pipeline ID cannot be null or empty");
        }
        
        if (steps.isEmpty()) {
            errors.add("Pipeline must have at least one step");
        }
        
        // Check for circular dependencies
        if (hasCircularDependencies()) {
            errors.add("Pipeline has circular dependencies");
        }
        
        // Check for unresolved dependencies
        Set<String> stepIds = steps.stream().map(PipelineStep::getId).collect(Collectors.toSet());
        for (PipelineStep step : steps) {
            for (String dependency : step.getDependencies()) {
                if (!stepIds.contains(dependency)) {
                    errors.add("Step " + step.getId() + " has unresolved dependency: " + dependency);
                }
            }
        }
        
        return errors;
    }
    
    private boolean hasCircularDependencies() {
        Map<String, Set<String>> graph = new HashMap<>();
        
        // Build dependency graph
        for (PipelineStep step : steps) {
            graph.put(step.getId(), step.getDependencies());
        }
        
        // Check for cycles using DFS
        Set<String> visited = new HashSet<>();
        Set<String> recursionStack = new HashSet<>();
        
        for (String stepId : graph.keySet()) {
            if (hasCycleDFS(graph, stepId, visited, recursionStack)) {
                return true;
            }
        }
        
        return false;
    }
    
    private boolean hasCycleDFS(Map<String, Set<String>> graph, String node, 
                               Set<String> visited, Set<String> recursionStack) {
        visited.add(node);
        recursionStack.add(node);
        
        Set<String> dependencies = graph.get(node);
        if (dependencies != null) {
            for (String dependency : dependencies) {
                if (!visited.contains(dependency)) {
                    if (hasCycleDFS(graph, dependency, visited, recursionStack)) {
                        return true;
                    }
                } else if (recursionStack.contains(dependency)) {
                    return true;
                }
            }
        }
        
        recursionStack.remove(node);
        return false;
    }
    
    /**
     * Convert to JSON representation
     */
    public JsonObject toJson() {
        JsonArray stepsJson = new JsonArray();
        steps.forEach(step -> stepsJson.add(step.toJsonObject()));
        
        return new JsonObject()
            .put("pipelineId", pipelineId)
            .put("name", name)
            .put("description", description)
            .put("steps", stepsJson)
            .put("executionPolicy", executionPolicy.toString())
            .put("globalConfiguration", globalConfiguration)
            .put("continueOnOptionalFailure", continueOnOptionalFailure)
            .put("enableFallback", enableFallback)
            .put("fallbackStrategies", new JsonArray(fallbackStrategies))
            .put("globalTimeoutMs", globalTimeoutMs)
            .put("maxConcurrentSteps", maxConcurrentSteps)
            .put("enableStreaming", enableStreaming)
            .put("streamingIntervalMs", streamingIntervalMs)
            .put("enableAdaptation", enableAdaptation)
            .put("adaptationCheckInterval", adaptationCheckInterval)
            .put("adaptationThreshold", adaptationThreshold);
    }
    
    /**
     * Execution policies for the pipeline
     */
    public enum ExecutionPolicy {
        SEQUENTIAL,      // Execute steps in order
        PRIORITY_BASED,  // Execute highest priority steps first
        DEPENDENCY_BASED // Execute based on dependency satisfaction
    }
    
    /**
     * Builder for PipelineConfiguration
     */
    public static class Builder {
        private String pipelineId;
        private String name;
        private String description;
        private List<PipelineStep> steps = new ArrayList<>();
        private ExecutionPolicy executionPolicy = ExecutionPolicy.SEQUENTIAL;
        private JsonObject globalConfiguration = new JsonObject();
        private boolean continueOnOptionalFailure = true;
        private boolean enableFallback = true;
        private List<String> fallbackStrategies = new ArrayList<>();
        private long globalTimeoutMs = 300000; // 5 minutes
        private int maxConcurrentSteps = 1;
        private boolean enableStreaming = true;
        private long streamingIntervalMs = 1000;
        private boolean enableAdaptation = false;
        private int adaptationCheckInterval = 3;
        private double adaptationThreshold = 0.7;
        
        public Builder(String pipelineId) {
            this.pipelineId = pipelineId;
            this.name = pipelineId; // Default name
        }
        
        public Builder name(String name) {
            this.name = name;
            return this;
        }
        
        public Builder description(String description) {
            this.description = description;
            return this;
        }
        
        public Builder addStep(PipelineStep step) {
            this.steps.add(step);
            return this;
        }
        
        public Builder steps(PipelineStep... steps) {
            Collections.addAll(this.steps, steps);
            return this;
        }
        
        public Builder steps(List<PipelineStep> steps) {
            this.steps.addAll(steps);
            return this;
        }
        
        public Builder executionPolicy(ExecutionPolicy policy) {
            this.executionPolicy = policy;
            return this;
        }
        
        public Builder globalConfiguration(JsonObject configuration) {
            this.globalConfiguration = configuration != null ? configuration.copy() : new JsonObject();
            return this;
        }
        
        public Builder continueOnOptionalFailure(boolean continueOnOptionalFailure) {
            this.continueOnOptionalFailure = continueOnOptionalFailure;
            return this;
        }
        
        public Builder enableFallback(boolean enableFallback) {
            this.enableFallback = enableFallback;
            return this;
        }
        
        public Builder addFallbackStrategy(String strategy) {
            this.fallbackStrategies.add(strategy);
            return this;
        }
        
        public Builder globalTimeout(long timeoutMs) {
            this.globalTimeoutMs = timeoutMs;
            return this;
        }
        
        public Builder maxConcurrentSteps(int maxConcurrentSteps) {
            this.maxConcurrentSteps = maxConcurrentSteps;
            return this;
        }
        
        public Builder enableStreaming(boolean enableStreaming) {
            this.enableStreaming = enableStreaming;
            return this;
        }
        
        public Builder streamingInterval(long intervalMs) {
            this.streamingIntervalMs = intervalMs;
            return this;
        }
        
        public Builder enableAdaptation(boolean enableAdaptation) {
            this.enableAdaptation = enableAdaptation;
            return this;
        }
        
        public Builder adaptationCheckInterval(int interval) {
            this.adaptationCheckInterval = interval;
            return this;
        }
        
        public Builder adaptationThreshold(double threshold) {
            this.adaptationThreshold = threshold;
            return this;
        }
        
        public PipelineConfiguration build() {
            PipelineConfiguration config = new PipelineConfiguration(this);
            List<String> errors = config.validate();
            if (!errors.isEmpty()) {
                throw new IllegalArgumentException("Pipeline configuration invalid: " + 
                    String.join(", ", errors));
            }
            return config;
        }
    }
    
    // Static factory methods for common configurations
    
    /**
     * Create a standard Oracle database query pipeline
     */
    public static PipelineConfiguration createOracleQueryPipeline() {
        return new Builder("oracle_query_pipeline")
            .name("Oracle Database Query Pipeline")
            .description("Complete pipeline for Oracle database query processing")
            .executionPolicy(ExecutionPolicy.SEQUENTIAL)
            .continueOnOptionalFailure(true)
            .enableFallback(true)
            .build();
    }
    
    /**
     * Create a schema exploration pipeline
     */
    public static PipelineConfiguration createSchemaExplorationPipeline() {
        return new Builder("schema_exploration_pipeline")
            .name("Schema Exploration Pipeline")
            .description("Pipeline for exploring database schema")
            .executionPolicy(ExecutionPolicy.DEPENDENCY_BASED)
            .globalTimeout(120000) // 2 minutes
            .build();
    }
    
    /**
     * Create an adaptive pipeline that can modify itself during execution
     */
    public static PipelineConfiguration createAdaptivePipeline() {
        return new Builder("adaptive_pipeline")
            .name("Adaptive Query Pipeline")
            .description("Pipeline that adapts based on execution feedback")
            .executionPolicy(ExecutionPolicy.DEPENDENCY_BASED)
            .enableAdaptation(true)
            .adaptationCheckInterval(2)
            .adaptationThreshold(0.8)
            .build();
    }
}