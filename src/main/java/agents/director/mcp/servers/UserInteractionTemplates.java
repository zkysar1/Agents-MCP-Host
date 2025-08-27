package agents.director.mcp.servers;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * UserInteractionTemplates - Clear, simple templates for user interactions.
 * Keeps cognitive load low with straightforward questions and options.
 * 
 * All templates are designed to:
 * - Be clear and concise
 * - Provide specific options when possible
 * - Show confidence levels
 * - Allow escape routes
 */
public class UserInteractionTemplates {
    
    /**
     * Confirm table selection
     */
    public String confirmTables(List<String> tables, SchemaMatcher.MatchResult matches) {
        StringBuilder message = new StringBuilder();
        
        message.append("I found the following potentially relevant tables:\n\n");
        
        // List tables with row counts if available
        for (String table : tables) {
            // Find the table match to get row count
            SchemaMatcher.TableMatch match = matches.tableMatches.stream()
                .filter(m -> m.tableName.equals(table))
                .findFirst()
                .orElse(null);
            
            if (match != null && match.rowCount > 0) {
                message.append("• ").append(table)
                       .append(" (").append(match.rowCount).append(" rows)")
                       .append(" - match score: ").append(String.format("%.1f%%", match.score * 100))
                       .append("\n");
            } else {
                message.append("• ").append(table).append("\n");
            }
        }
        
        message.append("\n");
        message.append("Are these the right tables to search? ");
        message.append("(yes/no/specify others)");
        
        return message.toString();
    }
    
    /**
     * Clarify intent when confidence is medium
     */
    public String clarifyIntent(QueryTokenExtractor.QueryTokens tokens, 
                               SchemaMatcher.MatchResult matches) {
        StringBuilder message = new StringBuilder();
        
        message.append("I'm trying to understand your query. ");
        message.append("Let me confirm what you're looking for:\n\n");
        
        // Show what we understood
        if (!tokens.actionWords.isEmpty()) {
            message.append("Action: ").append(String.join(", ", tokens.actionWords)).append("\n");
        }
        
        if (!tokens.potentialEntities.isEmpty()) {
            message.append("Looking for: ").append(String.join(", ", tokens.potentialEntities)).append("\n");
        }
        
        if (!tokens.quotedValues.isEmpty()) {
            message.append("Specific values: ").append(String.join(", ", tokens.quotedValues)).append("\n");
        }
        
        if (!tokens.numericValues.isEmpty()) {
            message.append("Numbers: ").append(String.join(", ", tokens.numericValues)).append("\n");
        }
        
        message.append("\n");
        
        // Show what we matched
        if (!matches.tableMatches.isEmpty()) {
            message.append("Possible tables: ");
            message.append(matches.tableMatches.stream()
                .map(m -> m.tableName)
                .distinct()
                .limit(3)
                .collect(Collectors.joining(", ")));
            message.append("\n");
        }
        
        if (!matches.columnMatches.isEmpty()) {
            message.append("Possible columns: ");
            message.append(matches.columnMatches.stream()
                .map(m -> m.columnName)
                .distinct()
                .limit(3)
                .collect(Collectors.joining(", ")));
            message.append("\n");
        }
        
        message.append("\n");
        message.append("Is this correct? (yes/no/clarify)");
        
        return message.toString();
    }
    
    /**
     * Ask user to rephrase when confidence is low
     */
    public String askForRephrase(String originalQuery) {
        StringBuilder message = new StringBuilder();
        
        message.append("I'm having trouble understanding your query:\n");
        message.append("\"").append(originalQuery).append("\"\n\n");
        
        message.append("Could you rephrase it or be more specific?\n");
        message.append("For example:\n");
        message.append("• \"Show all records from [table name]\"\n");
        message.append("• \"Find [items] where [condition]\"\n");
        message.append("• \"List [column] from [table]\"\n");
        
        return message.toString();
    }
    
    /**
     * Confirm SQL before execution
     */
    public String confirmSQL(String sql) {
        StringBuilder message = new StringBuilder();
        
        message.append("I've prepared the following SQL query:\n\n");
        message.append("```sql\n");
        message.append(sql);
        message.append("\n```\n\n");
        
        message.append("This query will:\n");
        
        // Parse SQL to explain it (simple version)
        String upperSQL = sql.toUpperCase();
        
        if (upperSQL.contains("SELECT *")) {
            message.append("• Retrieve all columns\n");
        } else if (upperSQL.contains("SELECT")) {
            message.append("• Retrieve specific columns\n");
        }
        
        if (upperSQL.contains("FROM")) {
            String fromClause = sql.substring(sql.toUpperCase().indexOf("FROM"));
            message.append("• Search in the specified tables\n");
        }
        
        if (upperSQL.contains("WHERE")) {
            message.append("• Apply filtering conditions\n");
        }
        
        if (upperSQL.contains("JOIN")) {
            message.append("• Combine data from multiple tables\n");
        }
        
        if (upperSQL.contains("FETCH FIRST") || upperSQL.contains("LIMIT")) {
            message.append("• Limit the number of results\n");
        }
        
        message.append("\n");
        message.append("Should I execute this query? (yes/no/modify)");
        
        return message.toString();
    }
    
    /**
     * Disambiguate when multiple options exist
     */
    public String disambiguate(String token, List<String> options) {
        StringBuilder message = new StringBuilder();
        
        message.append("The term '").append(token).append("' could refer to:\n\n");
        
        for (int i = 0; i < options.size(); i++) {
            message.append((i + 1)).append(". ").append(options.get(i)).append("\n");
        }
        
        message.append("\n");
        message.append("Which one did you mean? (enter number or specify)");
        
        return message.toString();
    }
    
    /**
     * Show progress during long operations
     */
    public String showProgress(String stage, String details) {
        StringBuilder message = new StringBuilder();
        
        message.append("Working on: ").append(stage).append("\n");
        if (details != null && !details.isEmpty()) {
            message.append("Details: ").append(details).append("\n");
        }
        
        return message.toString();
    }
    
    /**
     * Format error messages user-friendly
     */
    public String formatError(String error, String suggestion) {
        StringBuilder message = new StringBuilder();
        
        message.append("I encountered an issue:\n");
        message.append(error).append("\n\n");
        
        if (suggestion != null && !suggestion.isEmpty()) {
            message.append("Suggestion: ").append(suggestion).append("\n");
        } else {
            message.append("Please try rephrasing your query or being more specific.\n");
        }
        
        return message.toString();
    }
    
    /**
     * Success message with results summary
     */
    public String formatSuccess(int rowCount, long executionTime) {
        StringBuilder message = new StringBuilder();
        
        if (rowCount == 0) {
            message.append("Query executed successfully, but no results were found.\n");
            message.append("You might want to adjust your search criteria.\n");
        } else if (rowCount == 1) {
            message.append("Found 1 result.\n");
        } else {
            message.append("Found ").append(rowCount).append(" results.\n");
        }
        
        if (executionTime > 0) {
            message.append("Execution time: ").append(executionTime).append("ms\n");
        }
        
        return message.toString();
    }
    
    /**
     * Suggest alternatives when no direct match
     */
    public String suggestAlternatives(Set<String> unmatchedTokens, List<String> availableTables) {
        StringBuilder message = new StringBuilder();
        
        message.append("I couldn't find exact matches for: ");
        message.append(String.join(", ", unmatchedTokens)).append("\n\n");
        
        if (!availableTables.isEmpty()) {
            message.append("Available tables include:\n");
            int count = 0;
            for (String table : availableTables) {
                if (count++ >= 10) {
                    message.append("... and ").append(availableTables.size() - 10).append(" more\n");
                    break;
                }
                message.append("• ").append(table).append("\n");
            }
        }
        
        message.append("\n");
        message.append("Would you like to:\n");
        message.append("1. Search in a specific table\n");
        message.append("2. Rephrase your query\n");
        message.append("3. See all available tables\n");
        
        return message.toString();
    }
}