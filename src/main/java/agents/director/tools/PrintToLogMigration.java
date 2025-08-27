package agents.director.tools;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;

/**
 * Tool to migrate System.out.println statements to custom event bus logging
 * while keeping critical console outputs.
 */
public class PrintToLogMigration {
    
    // Patterns to identify different types of prints
    private static final Pattern PRINT_PATTERN = Pattern.compile(
        "(\\s*)System\\.out\\.println\\s*\\((.+?)\\);", 
        Pattern.DOTALL
    );
    
    private static final Pattern ERR_PRINT_PATTERN = Pattern.compile(
        "(\\s*)System\\.err\\.println\\s*\\((.+?)\\);", 
        Pattern.DOTALL
    );
    
    // Categories of prints to keep as console output
    private static final List<String> KEEP_CONSOLE_PATTERNS = Arrays.asList(
        "===.*System.*===",           // System startup/shutdown banners
        "Java version:",              // System info
        "Working directory:",         // System info
        "Data path:",                 // System info
        "Agent path:",                // System info
        ".*: READY$",                 // Readiness messages
        "All.*Ready",                 // Component readiness
        "WARNING:",                   // Warnings
        "\\[WARNING\\]",             // Warning prefix
        "OPENAI_API_KEY",            // API key warnings
        "Set OPENAI_API_KEY",        // API key warnings
        "Using default test password" // Security warnings
    );
    
    // Patterns that indicate debug level (3)
    private static final List<String> DEBUG_PATTERNS = Arrays.asList(
        "\\[DEBUG\\]",
        "Tool count updated",
        "Discovered tool:",
        "registered tool:",
        "Calling tool.*with args",
        "deployed successfully",
        "deployed client"
    );
    
    // Patterns that indicate info level (2)
    private static final List<String> INFO_PATTERNS = Arrays.asList(
        "started successfully",
        "initialized successfully",
        "Ready",
        "completed successfully",
        "Loaded.*configuration",
        "Processing.*request",
        "Selected strategy:",
        "Executing.*tool",
        "orchestration.*completed"
    );
    
    // Patterns that indicate warning level (1)
    private static final List<String> WARNING_PATTERNS = Arrays.asList(
        "Optional step.*failed",
        "fallback",
        "retry",
        "Warning"
    );
    
    private final Path projectRoot;
    private int totalPrints = 0;
    private int keptConsole = 0;
    private int convertedToLog = 0;
    
    public PrintToLogMigration(String projectRoot) {
        this.projectRoot = Paths.get(projectRoot);
    }
    
    public void migrate() {
        System.out.println("Starting System.out.println to Log migration...\n");
        
        try {
            Files.walk(projectRoot.resolve("src/main/java"))
                .filter(path -> path.toString().endsWith(".java"))
                .forEach(this::processFile);
                
            System.out.println("\nMigration complete!");
            System.out.println("Total prints found: " + totalPrints);
            System.out.println("Kept as console: " + keptConsole);
            System.out.println("Converted to logs: " + convertedToLog);
            
        } catch (IOException e) {
            System.err.println("Error during migration: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void processFile(Path filePath) {
        try {
            String content = Files.readString(filePath);
            String originalContent = content;
            
            // Extract class name
            String className = filePath.getFileName().toString().replace(".java", "");
            
            // Process System.out.println
            content = processPrintStatements(content, className, false);
            
            // Process System.err.println
            content = processErrorPrintStatements(content, className);
            
            // Only write if changed
            if (!content.equals(originalContent)) {
                Files.writeString(filePath, content);
                System.out.println("Processed: " + filePath.getFileName());
            }
            
        } catch (IOException e) {
            System.err.println("Error processing " + filePath + ": " + e.getMessage());
        }
    }
    
    private String processPrintStatements(String content, String className, boolean isError) {
        StringBuffer result = new StringBuffer();
        Matcher matcher = PRINT_PATTERN.matcher(content);
        
        while (matcher.find()) {
            String indent = matcher.group(1);
            String printContent = matcher.group(2);
            totalPrints++;
            
            // Check if this should stay as console output
            if (shouldKeepAsConsole(printContent)) {
                keptConsole++;
                matcher.appendReplacement(result, matcher.group(0)); // Keep original
            } else {
                // Convert to event bus log
                String logStatement = convertToLog(indent, printContent, className, isError);
                matcher.appendReplacement(result, logStatement);
                convertedToLog++;
            }
        }
        
        matcher.appendTail(result);
        return result.toString();
    }
    
    private String processErrorPrintStatements(String content, String className) {
        StringBuffer result = new StringBuffer();
        Matcher matcher = ERR_PRINT_PATTERN.matcher(content);
        
        while (matcher.find()) {
            String indent = matcher.group(1);
            String printContent = matcher.group(2);
            totalPrints++;
            
            // Most System.err.println should stay for critical errors
            if (printContent.contains("CRITICAL") || printContent.contains("Failed to deploy")) {
                keptConsole++;
                matcher.appendReplacement(result, matcher.group(0)); // Keep original
            } else {
                // Convert to error log (level 0)
                String logStatement = convertToLog(indent, printContent, className, true);
                matcher.appendReplacement(result, logStatement);
                convertedToLog++;
            }
        }
        
        matcher.appendTail(result);
        return result.toString();
    }
    
    private boolean shouldKeepAsConsole(String content) {
        for (String pattern : KEEP_CONSOLE_PATTERNS) {
            if (content.matches(".*" + pattern + ".*")) {
                return true;
            }
        }
        return false;
    }
    
    private String convertToLog(String indent, String content, String className, boolean isError) {
        // Determine log level
        String level = determineLogLevel(content, isError);
        
        // Determine category
        String category = determineCategory(className);
        
        // Clean the content - remove enclosing quotes if it's a simple string
        String cleanContent = cleanPrintContent(content);
        
        // Build the log statement
        return String.format("%svertx.eventBus().publish(\"log\", %s + \",%s,%s,%s,System\");",
            indent, cleanContent, level, className, category);
    }
    
    private String determineLogLevel(String content, boolean isError) {
        if (isError) return "0"; // Error
        
        // Check for debug patterns
        for (String pattern : DEBUG_PATTERNS) {
            if (content.toLowerCase().contains(pattern.toLowerCase())) {
                return "3";
            }
        }
        
        // Check for warning patterns
        for (String pattern : WARNING_PATTERNS) {
            if (content.toLowerCase().contains(pattern.toLowerCase())) {
                return "1";
            }
        }
        
        // Check for info patterns
        for (String pattern : INFO_PATTERNS) {
            if (content.toLowerCase().contains(pattern.toLowerCase())) {
                return "2";
            }
        }
        
        // Default to info
        return "2";
    }
    
    private String determineCategory(String className) {
        if (className.contains("Server")) return "MCP";
        if (className.contains("Client")) return "MCP";
        if (className.contains("Host")) return "Host";
        if (className.contains("Service")) return "Service";
        if (className.equals("Driver")) return "System";
        if (className.contains("API") || className.contains("Conversation")) return "API";
        if (className.contains("Connection") || className.contains("Oracle")) return "Database";
        return "System";
    }
    
    private String cleanPrintContent(String content) {
        // Handle string concatenation
        if (content.contains("+")) {
            return content; // Keep as-is for concatenated strings
        }
        
        // Handle simple strings
        if (content.startsWith("\"") && content.endsWith("\"")) {
            // Remove quotes and keep the content
            String inner = content.substring(1, content.length() - 1);
            return "\"" + inner + "\"";
        }
        
        return content;
    }
    
    public static void main(String[] args) {
        String projectRoot = args.length > 0 ? args[0] : 
            "/mnt/c/Users/Zachary/OneDrive/Zak/SmartNPCs/MCPThink/Agents-MCP-Host";
            
        PrintToLogMigration migration = new PrintToLogMigration(projectRoot);
        migration.migrate();
    }
}