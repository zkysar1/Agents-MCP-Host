/**
 * Manager Pipeline System for Sequential Execution with Dependency Management
 * 
 * <p>This package provides a sophisticated pipeline system for coordinating the sequential 
 * execution of managers in the correct dependency order. The system supports partial 
 * execution, streaming events, error handling with fallbacks, and shared context between steps.
 * 
 * <h2>Core Components</h2>
 * 
 * <h3>ExecutionContext</h3>
 * <p>Maintains shared state between pipeline steps, including:
 * <ul>
 *   <li>Step results and timing information</li>
 *   <li>Error tracking and recovery state</li>
 *   <li>Conversation history and user context</li>
 *   <li>Performance metrics and confidence calculation</li>
 * </ul>
 * 
 * <h3>PipelineStep</h3>
 * <p>Represents individual operations in the pipeline with:
 * <ul>
 *   <li>Dependency management and execution conditions</li>
 *   <li>Retry logic and error handling policies</li>
 *   <li>Custom argument builders for context-aware execution</li>
 *   <li>Optional/required step classification</li>
 * </ul>
 * 
 * <h3>PipelineConfiguration</h3>
 * <p>Defines pipeline execution policies and global settings:
 * <ul>
 *   <li>Execution policies (Sequential, Priority-based, Dependency-based)</li>
 *   <li>Error handling and fallback strategies</li>
 *   <li>Streaming and adaptation configuration</li>
 *   <li>Performance and timeout settings</li>
 * </ul>
 * 
 * <h3>ManagerPipeline</h3>
 * <p>Coordinates the execution of registered managers:
 * <ul>
 *   <li>Routes step execution to appropriate managers</li>
 *   <li>Handles streaming events and progress reporting</li>
 *   <li>Manages fallback strategies and error recovery</li>
 *   <li>Supports pipeline adaptation during execution</li>
 * </ul>
 * 
 * <h3>PipelineFactory</h3>
 * <p>Provides pre-built configurations for common use cases:
 * <ul>
 *   <li>Complete Oracle query processing pipeline</li>
 *   <li>Schema exploration and discovery pipeline</li>
 *   <li>SQL-only generation pipeline</li>
 *   <li>Adaptive pipeline with runtime modification</li>
 * </ul>
 * 
 * <h2>Pipeline Execution Flow</h2>
 * 
 * <p>The typical execution flow follows this pattern:
 * 
 * <pre>{@code
 * 1. Schema Resolution → Business Term Mapping
 * 2. Query Intent Analysis → Complexity Analysis
 * 3. Query Analysis → SQL Generation → SQL Optimization
 * 4. SQL Validation → Query Execution
 * 5. Result Formatting → Response Composition
 * }</pre>
 * 
 * <h2>Usage Examples</h2>
 * 
 * <h3>Basic Pipeline Setup</h3>
 * <pre>{@code
 * // Create pipeline configuration
 * PipelineConfiguration config = PipelineFactory.createCompleteOracleQueryPipeline();
 * 
 * // Create and configure pipeline
 * ManagerPipeline pipeline = new ManagerPipeline(vertx, config)
 *     .registerManager("SchemaIntelligenceManager", schemaManager)
 *     .registerManager("SQLPipelineManager", sqlManager)
 *     .registerManager("OracleExecutionManager", executionManager);
 * 
 * // Create execution context
 * ExecutionContext context = new ExecutionContext(
 *     conversationId, sessionId, userQuery, conversationHistory);
 * 
 * // Execute pipeline
 * Future<JsonObject> result = pipeline.execute(context);
 * }</pre>
 * 
 * <h3>Custom Pipeline Configuration</h3>
 * <pre>{@code
 * PipelineConfiguration config = new PipelineConfiguration.Builder("custom_pipeline")
 *     .name("Custom Oracle Pipeline")
 *     .executionPolicy(ExecutionPolicy.DEPENDENCY_BASED)
 *     .enableAdaptation(true)
 *     .addStep(PipelineStep.intentStep("intent", "analyze_intent")
 *         .priority(10)
 *         .build())
 *     .addStep(PipelineStep.sqlStep("sql_gen", "generate_sql", "OracleSQLGeneration")
 *         .addDependency("intent")
 *         .priority(5)
 *         .build())
 *     .build();
 * }</pre>
 * 
 * <h3>Strategy-Based Pipeline Creation</h3>
 * <pre>{@code
 * // Create pipeline based on strategy picker decision
 * PipelineConfiguration config = PipelineFactory.createStrategyBasedPipeline(
 *     "sql_only", strategyConfig);
 * 
 * // Pipeline will automatically configure steps based on strategy
 * ManagerPipeline pipeline = new ManagerPipeline(vertx, config);
 * }</pre>
 * 
 * <h2>Error Handling and Fallbacks</h2>
 * 
 * <p>The pipeline system provides multiple levels of error handling:
 * 
 * <ul>
 *   <li><strong>Step-level retries:</strong> Automatic retry with exponential backoff</li>
 *   <li><strong>Optional step skipping:</strong> Continue execution when optional steps fail</li>
 *   <li><strong>Fallback strategies:</strong> Alternative execution paths when primary pipeline fails</li>
 *   <li><strong>Graceful degradation:</strong> Partial results when complete execution is impossible</li>
 * </ul>
 * 
 * <h2>Streaming and Progress Reporting</h2>
 * 
 * <p>The pipeline emits streaming events for real-time progress reporting:
 * 
 * <ul>
 *   <li><strong>pipeline_started:</strong> Pipeline execution begins</li>
 *   <li><strong>step_started/completed/failed:</strong> Individual step lifecycle</li>
 *   <li><strong>pipeline_adapting:</strong> Runtime adaptation in progress</li>
 *   <li><strong>fallback_started:</strong> Fallback strategy activation</li>
 *   <li><strong>pipeline_completed:</strong> Final execution results</li>
 * </ul>
 * 
 * <h2>Performance Considerations</h2>
 * 
 * <p>The pipeline system is designed for optimal performance:
 * 
 * <ul>
 *   <li>Concurrent step execution where dependencies allow</li>
 *   <li>Lazy evaluation of step conditions and arguments</li>
 *   <li>Efficient context sharing with copy-on-write semantics</li>
 *   <li>Configurable timeouts and resource limits</li>
 * </ul>
 * 
 * <h2>Integration with Existing Systems</h2>
 * 
 * <p>The pipeline system integrates seamlessly with existing manager infrastructure:
 * 
 * <ul>
 *   <li>Compatible with all existing MCPClientManager implementations</li>
 *   <li>Supports current tool routing and argument building patterns</li>
 *   <li>Maintains compatibility with streaming and interrupt mechanisms</li>
 *   <li>Preserves existing logging and metrics collection</li>
 * </ul>
 * 
 * @since 1.0
 * @version 1.0
 * @author AI Assistant
 */
package agents.director.hosts.base.pipeline;