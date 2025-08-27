package agents.director.services;

import io.vertx.core.Vertx;
import agents.director.Driver;

/**
 * Utility class for structured logging with console output control.
 * Provides methods to log messages with appropriate console output based on priority.
 */
public class LogUtil {
    
    // Log levels matching the Driver.logLevel
    public static final int ERROR = 0;
    public static final int INFO = 1;
    public static final int DETAIL = 2;
    public static final int DEBUG = 3;
    public static final int DATA = 4;
    
    /**
     * Log a critical message that should always appear in console and logs
     */
    public static void logCritical(Vertx vertx, String message, String component, String operation, String category) {
        // Always show critical messages in console
        vertx.eventBus().publish("log", message + ",2,LogUtil,System,System");
        
        // Also send to event bus for CSV logging
        if (vertx != null) {
            vertx.eventBus().publish("log", formatLogMessage(message, ERROR, component, operation, category));
        }
    }
    
    /**
     * Log an error that should appear in console and logs
     */
    public static void logError(Vertx vertx, String message, String component, String operation, String category, boolean showInConsole) {
        if (showInConsole) {
            vertx.eventBus().publish("log", message + ",0,LogUtil,System,System");
        }
        
        if (vertx != null) {
            vertx.eventBus().publish("log", formatLogMessage(message, ERROR, component, operation, category));
        }
    }
    
    /**
     * Log an error with exception details
     */
    public static void logError(Vertx vertx, String message, Throwable throwable, String component, String operation, String category, boolean showInConsole) {
        String fullMessage = message + ": " + throwable.getMessage();
        
        if (showInConsole) {
            vertx.eventBus().publish("log", fullMessage + ",0,LogUtil,System,System");
            if (Driver.logLevel >= DEBUG) {
                throwable.printStackTrace();
            }
        }
        
        if (vertx != null) {
            // Log main error
            vertx.eventBus().publish("log", formatLogMessage(fullMessage, ERROR, component, operation, category));
            
            // Log stack trace at debug level
            if (Driver.logLevel >= DEBUG) {
                StringBuilder stackTrace = new StringBuilder();
                for (StackTraceElement element : throwable.getStackTrace()) {
                    stackTrace.append("  at ").append(element.toString()).append("\n");
                }
                vertx.eventBus().publish("log", formatLogMessage("Stack trace: " + stackTrace.toString(), DEBUG, component, operation, category));
            }
        }
    }
    
    /**
     * Log an info message (startup, status, etc)
     */
    public static void logInfo(Vertx vertx, String message, String component, String operation, String category, boolean showInConsole) {
        if (showInConsole && Driver.logLevel >= INFO) {
            vertx.eventBus().publish("log", message + ",2,LogUtil,System,System");
        }
        
        if (vertx != null) {
            vertx.eventBus().publish("log", formatLogMessage(message, INFO, component, operation, category));
        }
    }
    
    /**
     * Log a detailed message
     */
    public static void logDetail(Vertx vertx, String message, String component, String operation, String category) {
        if (Driver.logLevel >= DETAIL) {
            if (vertx != null) {
                vertx.eventBus().publish("log", formatLogMessage(message, DETAIL, component, operation, category));
            }
        }
    }
    
    /**
     * Log a debug message (never shows in console, only in logs)
     */
    public static void logDebug(Vertx vertx, String message, String component, String operation, String category) {
        if (Driver.logLevel >= DEBUG) {
            if (vertx != null) {
                vertx.eventBus().publish("log", formatLogMessage(message, DEBUG, component, operation, category));
            }
        }
    }
    
    /**
     * Log data/verbose message
     */
    public static void logData(Vertx vertx, String message, String component, String operation, String category) {
        if (Driver.logLevel >= DATA) {
            if (vertx != null) {
                vertx.eventBus().publish("log", formatLogMessage(message, DATA, component, operation, category));
            }
        }
    }
    
    /**
     * Format log message for event bus
     */
    private static String formatLogMessage(String message, int level, String component, String operation, String category) {
        // Remove any commas from the message to avoid CSV issues
        String cleanMessage = message.replace(",", ";");
        return cleanMessage + "," + level + "," + component + "," + operation + "," + category;
    }
}