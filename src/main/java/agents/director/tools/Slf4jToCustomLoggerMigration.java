package agents.director.tools;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import java.util.stream.Stream;

/**
 * Migration tool to convert SLF4J logging to custom event-bus based logging.
 * This tool processes Java files and converts logger statements to vertx.eventBus().publish() calls.
 */
public class Slf4jToCustomLoggerMigration {
    
    private static final Pattern SLF4J_IMPORT_PATTERN = Pattern.compile(
        "^\\s*import\\s+org\\.slf4j\\.(Logger|LoggerFactory);\\s*$", 
        Pattern.MULTILINE
    );
    
    private static final Pattern LOGGER_DECLARATION_PATTERN = Pattern.compile(
        "private\\s+static\\s+final\\s+Logger\\s+\\w+\\s*=\\s*LoggerFactory\\.getLogger\\([^)]+\\);?"
    );
    
    private static final Pattern LOGGER_CALL_PATTERN = Pattern.compile(
        "(logger)\\.(debug|info|warn|error)\\s*\\("
    );
    
    private static final Pattern PARAMETERIZED_LOG_PATTERN = Pattern.compile(
        "(logger)\\.(debug|info|warn|error)\\s*\\(\\s*\"([^\"]+)\"\\s*,\\s*(.+?)\\s*\\)",
        Pattern.DOTALL
    );
    
    private static final Pattern SIMPLE_LOG_PATTERN = Pattern.compile(
        "(logger)\\.(debug|info|warn|error)\\s*\\(\\s*\"([^\"]+)\"\\s*\\)"
    );
    
    // Map SLF4J levels to custom logger levels
    private static final Map<String, String> LEVEL_MAPPING = Map.of(
        "debug", "3",
        "info", "2",
        "warn", "1",
        "error", "0"
    );
    
    // Files to process
    private static final List<String> FILES_TO_MIGRATE = Arrays.asList(
        "src/main/java/agents/director/hosts/OracleDBAnswererHost.java",
        "src/main/java/agents/director/mcp/servers/QueryIntentEvaluationServer.java",
        "src/main/java/agents/director/mcp/servers/OracleSchemaIntelligenceServer.java",
        "src/main/java/agents/director/mcp/servers/OracleSQLValidationServer.java",
        "src/main/java/agents/director/mcp/servers/OracleSQLGenerationServer.java",
        "src/main/java/agents/director/mcp/servers/OracleQueryExecutionServer.java",
        "src/main/java/agents/director/mcp/servers/OracleQueryAnalysisServer.java",
        "src/main/java/agents/director/mcp/servers/BusinessMappingServer.java",
        "src/main/java/agents/director/apis/ConversationStreaming.java",
        "src/main/java/agents/director/services/MCPRouterService.java",
        "src/main/java/agents/director/mcp/client/UniversalMCPClient.java",
        "src/main/java/agents/director/hosts/OracleSQLBuilderHost.java",
        "src/main/java/agents/director/hosts/ToolFreeDirectLLMHost.java",
        "src/main/java/agents/director/mcp/base/MCPServerBase.java"
    );
    
    private final Path projectRoot;
    
    public Slf4jToCustomLoggerMigration(String projectRootPath) {
        this.projectRoot = Paths.get(projectRootPath);
    }
    
    /**
     * Main entry point for the migration
     */
    public void migrate() {
        System.out.println("Starting SLF4J to Custom Logger migration...\n");
        
        for (String filePath : FILES_TO_MIGRATE) {
            Path fullPath = projectRoot.resolve(filePath);
            try {
                if (Files.exists(fullPath)) {
                    System.out.println("Processing: " + filePath);
                    migrateFile(fullPath);
                    System.out.println("✓ Completed: " + filePath + "\n");
                } else {
                    System.out.println("✗ File not found: " + filePath + "\n");
                }
            } catch (IOException e) {
                System.err.println("✗ Error processing " + filePath + ": " + e.getMessage() + "\n");
                e.printStackTrace();
            }
        }
        
        System.out.println("Migration complete!");
    }
    
    /**
     * Migrate a single file from SLF4J to custom logger
     */
    private void migrateFile(Path filePath) throws IOException {
        String content = Files.readString(filePath);
        String originalContent = content;
        
        // Extract class name from file
        String className = extractClassName(filePath);
        
        // Check if file needs vertx reference
        boolean needsVertxReference = !content.contains("vertx") || !hasVertxAccessInClass(content);
        
        // Remove SLF4J imports
        content = removeSLF4JImports(content);
        
        // Remove logger declaration
        content = removeLoggerDeclaration(content);
        
        // Convert all logger calls
        content = convertLoggerCalls(content, className, needsVertxReference);
        
        // Add vertx parameter if needed
        if (needsVertxReference && content.contains("vertx.eventBus().publish(\"log\"")) {
            content = addVertxParameter(content);
        }
        
        // Only write if content changed
        if (!content.equals(originalContent)) {
            Files.writeString(filePath, content);
            System.out.println("  - File updated successfully");
        } else {
            System.out.println("  - No changes needed");
        }
    }
    
    /**
     * Extract class name from file path
     */
    private String extractClassName(Path filePath) {
        String fileName = filePath.getFileName().toString();
        return fileName.replace(".java", "");
    }
    
    /**
     * Check if class already has access to vertx
     */
    private boolean hasVertxAccessInClass(String content) {
        // Check for various ways vertx might be available
        return content.contains("private Vertx vertx") || 
               content.contains("protected Vertx vertx") ||
               content.contains("final Vertx vertx") ||
               content.contains("extends AbstractVerticle") ||
               content.contains("this.vertx");
    }
    
    /**
     * Remove SLF4J imports
     */
    private String removeSLF4JImports(String content) {
        return SLF4J_IMPORT_PATTERN.matcher(content).replaceAll("");
    }
    
    /**
     * Remove logger declaration
     */
    private String removeLoggerDeclaration(String content) {
        return LOGGER_DECLARATION_PATTERN.matcher(content).replaceAll("");
    }
    
    /**
     * Convert all logger calls to event bus publishes
     */
    private String convertLoggerCalls(String content, String className, boolean needsVertxRef) {
        String vertxRef = needsVertxRef ? "vertx" : "vertx";
        
        // Process line by line for better control
        String[] lines = content.split("\n");
        StringBuilder result = new StringBuilder();
        
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            String convertedLine = convertLoggerLine(line, className, vertxRef, getCategory(className));
            result.append(convertedLine).append("\n");
        }
        
        // Remove trailing newline
        if (result.length() > 0) {
            result.setLength(result.length() - 1);
        }
        
        return result.toString();
    }
    
    /**
     * Convert a single line containing logger calls
     */
    private String convertLoggerLine(String line, String className, String vertxRef, String category) {
        // Handle parameterized logging first (with placeholders)
        Matcher paramMatcher = Pattern.compile(
            "(logger)\\.(debug|info|warn|error)\\s*\\(\\s*\"([^\"]+)\"\\s*,\\s*(.+?)\\s*\\);?"
        ).matcher(line);
        
        if (paramMatcher.find()) {
            String level = LEVEL_MAPPING.get(paramMatcher.group(2));
            String message = paramMatcher.group(3);
            String args = paramMatcher.group(4).trim();
            
            // Handle the arguments - could be multiple
            if (args.endsWith(");")) {
                args = args.substring(0, args.length() - 2);
            }
            if (args.endsWith(")")) {
                args = args.substring(0, args.length() - 1);
            }
            
            // Convert {} placeholders to concatenation
            String convertedMessage = convertParameterizedMessage(message, args);
            
            String indent = getIndentation(line);
            return indent + vertxRef + ".eventBus().publish(\"log\", " + convertedMessage + 
                   " + \"," + level + "," + className + "," + category + ",System\");";
        }
        
        // Handle simple logging (no parameters)
        Matcher simpleMatcher = Pattern.compile(
            "(logger)\\.(debug|info|warn|error)\\s*\\(\\s*\"([^\"]+)\"\\s*\\);?"
        ).matcher(line);
        
        if (simpleMatcher.find()) {
            String level = LEVEL_MAPPING.get(simpleMatcher.group(2));
            String message = simpleMatcher.group(3);
            
            String indent = getIndentation(line);
            return indent + vertxRef + ".eventBus().publish(\"log\", \"" + message + 
                   "," + level + "," + className + "," + category + ",System\");";
        }
        
        // Handle error with exception
        Matcher errorMatcher = Pattern.compile(
            "(logger)\\.(error)\\s*\\(\\s*\"([^\"]+)\"\\s*,\\s*(\\w+)\\s*\\);?"
        ).matcher(line);
        
        if (errorMatcher.find()) {
            String message = errorMatcher.group(3);
            String exception = errorMatcher.group(4);
            
            String indent = getIndentation(line);
            return indent + vertxRef + ".eventBus().publish(\"log\", \"" + message + 
                   ": \" + " + exception + ".getMessage() + \",0," + className + "," + category + ",System\");";
        }
        
        return line;
    }
    
    /**
     * Convert parameterized message with {} to string concatenation
     */
    private String convertParameterizedMessage(String message, String args) {
        String[] argArray = splitArguments(args);
        int argIndex = 0;
        StringBuilder result = new StringBuilder("\"");
        
        int lastPos = 0;
        int pos = message.indexOf("{}");
        
        while (pos != -1 && argIndex < argArray.length) {
            result.append(message.substring(lastPos, pos));
            result.append("\" + ");
            // Ensure method calls are properly closed
            String arg = argArray[argIndex].trim();
            if (arg.contains("(") && !arg.contains(")")) {
                arg = arg + ")";
            }
            result.append(arg);
            result.append(" + \"");
            
            argIndex++;
            lastPos = pos + 2;
            pos = message.indexOf("{}", lastPos);
        }
        
        // Append remainder
        result.append(message.substring(lastPos));
        result.append("\"");
        
        return result.toString();
    }
    
    /**
     * Split arguments considering nested parentheses and commas
     */
    private String[] splitArguments(String args) {
        List<String> result = new ArrayList<>();
        int parenDepth = 0;
        int lastStart = 0;
        
        for (int i = 0; i < args.length(); i++) {
            char c = args.charAt(i);
            if (c == '(' || c == '{' || c == '[') {
                parenDepth++;
            } else if (c == ')' || c == '}' || c == ']') {
                parenDepth--;
            } else if (c == ',' && parenDepth == 0) {
                result.add(args.substring(lastStart, i));
                lastStart = i + 1;
            }
        }
        
        // Add the last argument
        if (lastStart < args.length()) {
            result.add(args.substring(lastStart));
        }
        
        return result.toArray(new String[0]);
    }
    
    /**
     * Get line indentation
     */
    private String getIndentation(String line) {
        int i = 0;
        while (i < line.length() && Character.isWhitespace(line.charAt(i))) {
            i++;
        }
        return line.substring(0, i);
    }
    
    /**
     * Determine category based on class name
     */
    private String getCategory(String className) {
        if (className.contains("Server")) return "MCP";
        if (className.contains("Host")) return "Host";
        if (className.contains("Client")) return "MCP";
        if (className.contains("Service")) return "Service";
        if (className.contains("API") || className.contains("Conversation")) return "API";
        return "System";
    }
    
    /**
     * Add vertx parameter to methods if needed
     */
    private String addVertxParameter(String content) {
        // This is complex and would require proper AST parsing
        // For now, we'll assume classes have access to vertx through inheritance or injection
        return content;
    }
    
    /**
     * Main method to run the migration
     */
    public static void main(String[] args) {
        String projectRoot = args.length > 0 ? args[0] : "/mnt/c/Users/Zachary/OneDrive/Zak/SmartNPCs/MCPThink/Agents-MCP-Host";
        
        Slf4jToCustomLoggerMigration migration = new Slf4jToCustomLoggerMigration(projectRoot);
        migration.migrate();
    }
}