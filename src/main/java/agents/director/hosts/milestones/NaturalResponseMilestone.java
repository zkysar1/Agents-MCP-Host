package agents.director.hosts.milestones;

import agents.director.hosts.base.MilestoneContext;
import agents.director.hosts.base.MilestoneManager;
import agents.director.services.LlmAPIService;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;

/**
 * Milestone 6: Natural Language Response
 * 
 * Generates a natural language response to the user's original question
 * based on all the data gathered and results obtained.
 * Uses LLM service to create human-friendly responses.
 * 
 * Output shared with user: Natural language answer to their question
 */
public class NaturalResponseMilestone extends MilestoneManager {
    
    private LlmAPIService llmService;
    
    public NaturalResponseMilestone(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl, 6, "NaturalResponseMilestone", 
              "Generate natural language response to user's question");
    }
    
    @Override
    public Future<Void> initialize() {
        Promise<Void> promise = Promise.promise();
        
        // Initialize LLM service
        llmService = LlmAPIService.getInstance();
        
        if (llmService.isInitialized()) {
            log("Natural response milestone initialized successfully", 2);
            promise.complete();
        } else {
            log("LLM service not available, will use template-based responses", 1);
            promise.complete(); // Still complete, we'll use fallback
        }
        
        return promise.future();
    }
    
    @Override
    public Future<MilestoneContext> execute(MilestoneContext context) {
        Promise<MilestoneContext> promise = Promise.promise();
        
        log("Starting natural language response generation", 3);
        
        // Publish progress event at start
        if (context.isStreaming() && context.getSessionId() != null) {
            publishProgressEvent(context.getConversationId(),
                "Step 6: Response Generation",
                "Creating natural language response...",
                new JsonObject()
                    .put("phase", "response_generation")
                    .put("row_count", context.getRowCount()));
        }
        
        // Check if we have results to work with
        if (context.getRowCount() == 0 && context.getQueryResults() == null) {
            // No data to generate response from
            String simpleResponse = "I was unable to retrieve data for your query: " + context.getQuery();
            context.setNaturalResponse(simpleResponse);
            context.completeMilestone(6);
            promise.complete(context);
            return promise.future();
        }
        
        // Generate natural language response
        if (llmService != null && llmService.isInitialized()) {
            generateLLMResponse(context)
                .onSuccess(response -> {
                    context.setNaturalResponse(response);
                    context.setResponseMetadata(new JsonObject()
                        .put("method", "llm")
                        .put("success", true));
                    context.completeMilestone(6);
                    
                    // Publish streaming event if applicable
                    if (context.isStreaming() && context.getSessionId() != null) {
                        publishStreamingEvent(context.getConversationId(), "milestone.response_complete",
                            getShareableResult(context));
                    }
                    
                    log("Natural language response generated successfully", 2);
                    promise.complete(context);
                })
                .onFailure(err -> {
                    log("LLM response generation failed, using template: " + err.getMessage(), 1);
                    
                    // Fallback to template-based response
                    String templateResponse = generateTemplateResponse(context);
                    context.setNaturalResponse(templateResponse);
                    context.setResponseMetadata(new JsonObject()
                        .put("method", "template")
                        .put("fallback", true));
                    context.completeMilestone(6);
                    promise.complete(context);
                });
        } else {
            // Use template-based response
            String templateResponse = generateTemplateResponse(context);
            context.setNaturalResponse(templateResponse);
            context.setResponseMetadata(new JsonObject()
                .put("method", "template")
                .put("no_llm", true));
            context.completeMilestone(6);
            promise.complete(context);
        }
        
        return promise.future();
    }
    
    @Override
    public JsonObject getShareableResult(MilestoneContext context) {
        return new JsonObject()
            .put("milestone", 6)
            .put("milestone_name", "Natural Language Response")
            .put("response", context.getNaturalResponse())
            .put("query", context.getQuery())
            .put("data_points", context.getRowCount())
            .put("message", context.getNaturalResponse());
    }
    
    /**
     * Generate response using LLM
     */
    private Future<String> generateLLMResponse(MilestoneContext context) {
        Promise<String> promise = Promise.promise();
        
        // Build comprehensive prompt
        String systemPrompt = "You are a helpful data analyst assistant. " +
            "Generate a clear, natural language response to the user's question based on the query results. " +
            "Be concise but informative. If the data shows specific numbers or trends, mention them. " +
            "Backstory: " + context.getBackstory() + "\n" +
            "Guidance: " + context.getGuidance();
        
        // Build user prompt with all context
        StringBuilder userPrompt = new StringBuilder();
        userPrompt.append("Original Question: ").append(context.getQuery()).append("\n\n");
        userPrompt.append("Intent Understood: ").append(context.getIntent()).append("\n\n");
        userPrompt.append("SQL Executed: ").append(context.getGeneratedSql()).append("\n\n");
        userPrompt.append("Results Summary:\n");
        userPrompt.append("- Total Rows: ").append(context.getRowCount()).append("\n");
        
        // Add sample data if available
        if (context.getQueryResults() != null && !context.getQueryResults().isEmpty()) {
            userPrompt.append("\nSample Data (first 5 rows):\n");
            int sampleSize = Math.min(5, context.getQueryResults().size());
            for (int i = 0; i < sampleSize; i++) {
                userPrompt.append(context.getQueryResults().getJsonObject(i).encodePrettily()).append("\n");
            }
        }
        
        userPrompt.append("\nPlease provide a natural language answer to the original question.");
        
        // Call LLM
        java.util.List<String> messages = Arrays.asList(
            new JsonObject().put("role", "system").put("content", systemPrompt).encode(),
            new JsonObject().put("role", "user").put("content", userPrompt.toString()).encode()
        );
        
        // Get current Vert.x context
        io.vertx.core.Context vertxContext = Vertx.currentContext();
        
        llmService.chatCompletion(messages, 0.3, 500)
            .whenComplete((result, error) -> {
                Runnable handler = () -> {
                    if (error != null) {
                        promise.fail(error);
                        return;
                    }
                    
                    try {
                        String response = result.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message")
                            .getString("content");
                        
                        promise.complete(response);
                    } catch (Exception e) {
                        promise.fail(e);
                    }
                };
                
                if (vertxContext != null) {
                    vertxContext.runOnContext(v -> handler.run());
                } else {
                    handler.run();
                }
            });
        
        return promise.future();
    }
    
    /**
     * Generate template-based response when LLM is not available
     */
    private String generateTemplateResponse(MilestoneContext context) {
        StringBuilder response = new StringBuilder();
        
        // Start with acknowledgment
        response.append("Based on your query: \"").append(context.getQuery()).append("\"\n\n");
        
        // Add intent understanding
        response.append("I understood that you wanted to: ").append(context.getIntent()).append("\n\n");
        
        // Add results summary
        if (context.getRowCount() > 0) {
            response.append("Here's what I found:\n");
            
            // Describe the results
            if (context.getRowCount() == 1) {
                response.append("• The query returned 1 result\n");
            } else {
                response.append("• The query returned ").append(context.getRowCount()).append(" results\n");
            }
            
            // Add execution details
            if (context.getExecutionTime() > 0) {
                response.append("• Query executed in ").append(context.getExecutionTime()).append("ms\n");
            }
            
            // Add table information
            if (!context.getRelevantTables().isEmpty()) {
                response.append("• Data was retrieved from: ").append(String.join(", ", context.getRelevantTables())).append("\n");
            }
            
            // Add sample data description if available
            if (context.getQueryResults() != null && !context.getQueryResults().isEmpty()) {
                response.append("\n");
                JsonObject firstRow = context.getQueryResults().getJsonObject(0);
                if (firstRow != null) {
                    // Try to provide meaningful summary based on data
                    for (String field : firstRow.fieldNames()) {
                        Object value = firstRow.getValue(field);
                        if (value != null && isNumericField(field)) {
                            response.append("• ").append(field).append(": ").append(value).append("\n");
                            break; // Just show one key metric
                        }
                    }
                }
            }
            
            // Add SQL for transparency
            response.append("\nThe SQL query used was:\n```sql\n")
                   .append(context.getGeneratedSql())
                   .append("\n```");
        } else {
            response.append("No data was found matching your criteria.\n\n");
            
            if (context.getGeneratedSql() != null) {
                response.append("The SQL query attempted was:\n```sql\n")
                       .append(context.getGeneratedSql())
                       .append("\n```");
            }
        }
        
        return response.toString();
    }
    
    /**
     * Check if a field name suggests it contains numeric data
     */
    private boolean isNumericField(String fieldName) {
        String lower = fieldName.toLowerCase();
        return lower.contains("count") || lower.contains("sum") || lower.contains("total") ||
               lower.contains("amount") || lower.contains("quantity") || lower.contains("average") ||
               lower.contains("avg") || lower.contains("max") || lower.contains("min");
    }
}