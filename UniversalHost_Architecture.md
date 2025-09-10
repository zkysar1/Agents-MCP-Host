# UniversalHost Architecture

## Overview

The UniversalHost represents a major architectural evolution of the Agents-MCP-Host system, replacing three specialized host classes with a single, configuration-driven host that can serve as any agent type.

## Architecture Components

### 1. Intent Engine (`hosts.base.intelligence.IntentEngine`)

The Intent Engine forms the first stage of the Universal Host pipeline, responsible for understanding user queries at a deep level.

**Key Responsibilities:**
- Analyze query intent and extract primary/secondary intents
- Determine query complexity and resource requirements  
- Assess conversation context and user profile
- Provide confidence scoring for intent predictions
- Support adaptive interaction styles

**Key Methods:**
- `analyzeIntent()` - Core intent analysis with comprehensive results
- `classifyIntent()` - Quick intent classification for simple routing
- `recommendAgent()` - Suggest best agent type for the query
- `analyzeContext()` - Understand conversation continuity and patterns
- `suggestInteractionStyle()` - Adaptive interaction recommendations

### 2. Strategy Picker (`hosts.base.intelligence.StrategyPicker`)

The Strategy Picker forms the second stage, selecting optimal execution strategies based on intent analysis results.

**Key Responsibilities:**
- Select optimal execution strategies based on intent and agent configuration
- Generate dynamic execution plans with appropriate tools
- Adapt strategies in real-time based on execution feedback
- Learn from execution patterns to improve future selections
- Manage strategy complexity and resource constraints
- Provide fallback strategies when primary approaches fail

**Key Methods:**
- `selectStrategy()` - Core strategy selection based on intent analysis
- `generateAgentStrategy()` - Agent-specific strategy generation
- `generateDynamicStrategy()` - Real-time strategy generation
- `adaptStrategy()` - Real-time strategy adaptation during execution
- `learnFromExecution()` - Learning from execution patterns
- `getFallbackStrategies()` - Generate fallback options

### 3. UniversalHost (`hosts.UniversalHost`)

The UniversalHost orchestrates the entire pipeline and manages agent-specific behavior through configuration.

**Key Features:**
- **Agent-agnostic design** - Supports any agent configuration
- **Dynamic manager loading** - Initializes only required managers
- **Intent-driven strategy selection** - Uses Intent Engine → Strategy Picker flow
- **Configuration-driven behavior** - All behavior controlled by agent JSON configs
- **Streaming support** - Full streaming with interrupt handling
- **Performance monitoring** - Tracks and learns from execution patterns

## Pipeline Flow

### Universal Processing Pipeline

1. **Intent Analysis Stage**
   - Intent Engine analyzes the user query
   - Extracts primary/secondary intents
   - Determines complexity and requirements
   - Assesses conversation context

2. **Strategy Selection Stage**  
   - Strategy Picker evaluates intent analysis
   - Considers available managers and agent constraints
   - Selects or generates optimal execution strategy
   - Plans step-by-step execution approach

3. **Strategy Execution Stage**
   - Executes strategy steps using configured managers
   - Supports real-time adaptation based on intermediate results
   - Handles interrupts and error recovery
   - Tracks performance metrics

4. **Learning & Response Stage**
   - Records execution patterns for future learning
   - Composes agent-appropriate response
   - Updates conversation context
   - Provides confidence scoring

## Agent Configuration

### Agent Configuration Structure

```json
{
  "agentType": "database-analyst",
  "displayName": "Database Analyst",
  "description": "Specialized agent for database analysis",
  "systemPrompt": "You are a database analyst...",
  "enabledManagers": [
    "OracleExecutionManager",
    "SQLPipelineManager",
    "SchemaIntelligenceManager",
    "IntentAnalysisManager",
    "StrategyOrchestrationManager"
  ],
  "managerConfigs": {
    "OracleExecutionManager": {
      "maxQueryExecutionTime": 300000,
      "autoCommit": false
    }
  },
  "allowExecution": true,
  "allowToolUse": true,
  "dynamicStrategies": true,
  "maxComplexity": 8
}
```

### Key Configuration Options

- **enabledManagers**: List of managers to initialize for this agent
- **managerConfigs**: Manager-specific configuration overrides
- **allowExecution**: Whether agent can execute database queries
- **allowToolUse**: Whether agent can use MCP tools
- **dynamicStrategies**: Enable dynamic strategy generation vs static
- **maxComplexity**: Maximum query complexity this agent can handle
- **allowedStrategies**: List of allowed strategy types

## Migration from Legacy Hosts

### Replaced Host Classes

1. **OracleDBAnswererHost** → UniversalHost with `database-analyst` config
2. **OracleSQLBuilderHost** → UniversalHost with `sql-expert` config  
3. **ToolFreeDirectLLMHost** → UniversalHost with `general-assistant` config

### Migration Benefits

- **Unified Architecture**: Single host class handles all agent types
- **Configuration-driven**: Easy to create new agent types via JSON
- **Intelligence Layer**: Intent Engine + Strategy Picker provide smarter processing
- **Dynamic Adaptation**: Real-time strategy adaptation based on execution
- **Learning Capability**: Continuous improvement from execution patterns
- **Simplified Maintenance**: One codebase instead of three specialized hosts

## Event Bus Integration

### Endpoints

- `host.universal.process` - Universal processing endpoint
- `host.{agentType}.process` - Agent-specific endpoint for backward compatibility
- `host.universal.status` - Status and capabilities reporting
- `host.universal.config` - Agent configuration retrieval
- `host.universal.clear` - Clear conversation context

### Streaming Events

All streaming events are published with agent-specific context:
- `streaming.{conversationId}.progress` - Processing progress updates
- `streaming.{conversationId}.tool.start` - Tool execution start
- `streaming.{conversationId}.tool.complete` - Tool execution complete  
- `streaming.{conversationId}.error` - Error events
- `streaming.{conversationId}.final` - Final response
- `streaming.{conversationId}.interrupt` - Interrupt handling

## Manager Integration

### Dynamic Manager Loading

The UniversalHost dynamically loads only the managers specified in the agent configuration:

```java
// Managers are loaded based on configuration
JsonArray enabledManagers = agentConfig.getJsonArray("enabledManagers");
for (String managerName : enabledManagers) {
    MCPClientManager manager = createManager(managerName, baseUrl);
    managers.put(managerName, manager);
}
```

### Manager Types

- **OracleExecutionManager** - Database query execution
- **SQLPipelineManager** - SQL generation and validation
- **SchemaIntelligenceManager** - Schema analysis and mapping
- **IntentAnalysisManager** - Legacy intent analysis (still used by some strategies)
- **StrategyOrchestrationManager** - Legacy strategy orchestration

## Intelligence Components

### Intent Engine Features

- **Deep Intent Analysis**: Multi-level intent extraction
- **Context Awareness**: Conversation history and user profile integration
- **Confidence Scoring**: Reliability metrics for intent predictions
- **Agent Recommendation**: Suggests optimal agent type for queries
- **Adaptive Interaction**: Personalized interaction styles

### Strategy Picker Features

- **Dynamic Strategy Generation**: Real-time strategy creation
- **Static Strategy Selection**: Pre-defined strategy templates
- **Real-time Adaptation**: Strategy modification during execution
- **Performance Learning**: Continuous improvement from execution history
- **Fallback Mechanisms**: Robust error recovery

## Performance & Monitoring

### Metrics Tracking

- Execution duration per stage
- Strategy effectiveness scores
- Intent prediction accuracy
- Manager utilization patterns
- User satisfaction indicators

### Learning Loop

1. **Execution Recording**: Capture strategy execution details
2. **Pattern Analysis**: Identify successful patterns and failure modes
3. **Strategy Optimization**: Improve strategy selection algorithms
4. **Adaptation Rules**: Refine real-time adaptation triggers

## Testing & Validation

### Compilation Verification

The architecture successfully compiles with the existing codebase:

```bash
./gradlew compileJava
# BUILD SUCCESSFUL
```

### Functional Testing

Key test scenarios to validate:

1. **Agent Configuration Loading**: Test loading different agent configurations
2. **Manager Initialization**: Verify only configured managers are loaded
3. **Intent Engine Pipeline**: Test intent analysis with various query types
4. **Strategy Picker Pipeline**: Test strategy selection and adaptation
5. **Streaming Support**: Verify streaming events and interrupt handling
6. **Error Handling**: Test graceful degradation and fallback mechanisms

## Future Enhancements

### Planned Improvements

1. **Machine Learning Integration**: Enhanced intent prediction and strategy selection
2. **A/B Testing Framework**: Compare strategy effectiveness
3. **Advanced Analytics**: Deeper performance analysis and optimization
4. **Multi-Agent Coordination**: Cross-agent collaboration capabilities
5. **External System Integration**: Broader tool and service integration

### Extension Points

- **Custom Intelligence Components**: Add domain-specific analysis engines
- **Strategy Plugins**: Extensible strategy generation algorithms  
- **Manager Plugins**: Custom manager types for specialized functionality
- **Learning Algorithms**: Pluggable learning and adaptation algorithms

## Conclusion

The UniversalHost architecture represents a significant advancement in intelligent agent design, providing:

- **Unified, maintainable codebase** replacing three specialized hosts
- **Intelligent query processing** through Intent Engine and Strategy Picker
- **Dynamic adaptation** with real-time strategy modification
- **Configuration-driven flexibility** for creating diverse agent types
- **Continuous learning** for improved performance over time

This architecture provides a solid foundation for building sophisticated, adaptive agents that can handle complex database tasks while continuously improving their performance.