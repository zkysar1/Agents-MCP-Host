package AgentsMCPHost.mcp.utils;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.text.DecimalFormat;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * ResultFormatter - Formats SQL query results for business users.
 * 
 * Features:
 * - Converts technical data to business-friendly format
 * - Handles currency and percentage formatting
 * - Generates summary statistics
 * - Creates pivot tables
 * - Produces natural language summaries
 * - Formats for different output types (table, chart, narrative)
 */
public class ResultFormatter {
    
    // Thread-local formatters to avoid thread safety issues
    private static final ThreadLocal<DecimalFormat> CURRENCY_FORMAT = 
        ThreadLocal.withInitial(() -> new DecimalFormat("$#,##0.00"));
    private static final ThreadLocal<DecimalFormat> PERCENT_FORMAT = 
        ThreadLocal.withInitial(() -> new DecimalFormat("#0.0%"));
    private static final ThreadLocal<DecimalFormat> NUMBER_FORMAT = 
        ThreadLocal.withInitial(() -> new DecimalFormat("#,##0"));
    // Thread-safe date formatters
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("MMM dd, yyyy");
    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("MMM dd, yyyy HH:mm");
    
    private final EnumerationMapper enumMapper;
    
    // Column type patterns
    private static final Map<String, ColumnType> COLUMN_TYPE_PATTERNS = new HashMap<>();
    static {
        COLUMN_TYPE_PATTERNS.put(".*_ID$", ColumnType.ID);
        COLUMN_TYPE_PATTERNS.put(".*PRICE.*", ColumnType.CURRENCY);
        COLUMN_TYPE_PATTERNS.put(".*AMOUNT.*", ColumnType.CURRENCY);
        COLUMN_TYPE_PATTERNS.put(".*COST.*", ColumnType.CURRENCY);
        COLUMN_TYPE_PATTERNS.put(".*REVENUE.*", ColumnType.CURRENCY);
        COLUMN_TYPE_PATTERNS.put(".*PERCENT.*", ColumnType.PERCENTAGE);
        COLUMN_TYPE_PATTERNS.put(".*RATE.*", ColumnType.PERCENTAGE);
        COLUMN_TYPE_PATTERNS.put(".*DATE.*", ColumnType.DATE);
        COLUMN_TYPE_PATTERNS.put(".*TIME.*", ColumnType.DATETIME);
        COLUMN_TYPE_PATTERNS.put(".*COUNT.*", ColumnType.INTEGER);
        COLUMN_TYPE_PATTERNS.put(".*QTY.*", ColumnType.INTEGER);
        COLUMN_TYPE_PATTERNS.put(".*QUANTITY.*", ColumnType.INTEGER);
    }
    
    public ResultFormatter() {
        this.enumMapper = EnumerationMapper.getInstance();
    }
    
    /**
     * Format query results for business presentation
     */
    public FormattedResult format(JsonArray results, FormatOptions options) {
        FormattedResult formatted = new FormattedResult();
        
        if (results == null || results.isEmpty()) {
            formatted.summary = "No results found.";
            formatted.formattedData = new JsonArray();
            return formatted;
        }
        
        // Detect column types
        Map<String, ColumnType> columnTypes = detectColumnTypes(results);
        
        // Format data based on output type
        switch (options.outputType) {
            case TABLE:
                formatted.formattedData = formatAsTable(results, columnTypes);
                formatted.headers = extractHeaders(results);
                break;
                
            case CHART:
                formatted.formattedData = formatForChart(results, columnTypes, options);
                formatted.chartConfig = generateChartConfig(results, options);
                break;
                
            case NARRATIVE:
                formatted.narrative = generateNarrative(results, columnTypes);
                formatted.formattedData = results; // Keep original for reference
                break;
                
            case SUMMARY:
            default:
                formatted.formattedData = formatWithSummary(results, columnTypes);
                break;
        }
        
        // Generate summary statistics
        formatted.statistics = generateStatistics(results, columnTypes);
        
        // Generate business summary
        formatted.summary = generateBusinessSummary(results, formatted.statistics);
        
        return formatted;
    }
    
    /**
     * Detect column types based on names and values
     */
    private Map<String, ColumnType> detectColumnTypes(JsonArray results) {
        Map<String, ColumnType> types = new HashMap<>();
        
        if (results.isEmpty()) return types;
        
        JsonObject firstRow = results.getJsonObject(0);
        
        for (String column : firstRow.fieldNames()) {
            ColumnType type = ColumnType.TEXT; // Default
            
            // Check column name patterns
            for (Map.Entry<String, ColumnType> pattern : COLUMN_TYPE_PATTERNS.entrySet()) {
                if (column.toUpperCase().matches(pattern.getKey())) {
                    type = pattern.getValue();
                    break;
                }
            }
            
            // If still text, check value type
            if (type == ColumnType.TEXT) {
                Object value = firstRow.getValue(column);
                if (value instanceof Number) {
                    if (value instanceof Integer || value instanceof Long) {
                        type = ColumnType.INTEGER;
                    } else {
                        type = ColumnType.DECIMAL;
                    }
                } else if (value instanceof Boolean) {
                    type = ColumnType.BOOLEAN;
                }
            }
            
            types.put(column, type);
        }
        
        return types;
    }
    
    /**
     * Format results as a table
     */
    private JsonArray formatAsTable(JsonArray results, Map<String, ColumnType> columnTypes) {
        JsonArray formatted = new JsonArray();
        
        for (int i = 0; i < results.size(); i++) {
            JsonObject row = results.getJsonObject(i);
            JsonObject formattedRow = new JsonObject();
            
            for (String column : row.fieldNames()) {
                Object value = row.getValue(column);
                ColumnType type = columnTypes.getOrDefault(column, ColumnType.TEXT);
                formattedRow.put(column, formatValue(value, type));
            }
            
            formatted.add(formattedRow);
        }
        
        return formatted;
    }
    
    /**
     * Format results for chart visualization
     */
    private JsonArray formatForChart(JsonArray results, Map<String, ColumnType> columnTypes, 
                                    FormatOptions options) {
        JsonArray chartData = new JsonArray();
        
        // Determine chart axes
        String xAxis = options.chartXAxis;
        String yAxis = options.chartYAxis;
        
        if (xAxis == null || yAxis == null) {
            // Auto-detect axes
            for (String column : columnTypes.keySet()) {
                if (xAxis == null && columnTypes.get(column) == ColumnType.TEXT) {
                    xAxis = column;
                } else if (yAxis == null && 
                          (columnTypes.get(column) == ColumnType.INTEGER ||
                           columnTypes.get(column) == ColumnType.DECIMAL ||
                           columnTypes.get(column) == ColumnType.CURRENCY)) {
                    yAxis = column;
                }
            }
        }
        
        // Build chart data
        for (int i = 0; i < results.size(); i++) {
            JsonObject row = results.getJsonObject(i);
            JsonObject chartPoint = new JsonObject()
                .put("x", row.getValue(xAxis))
                .put("y", row.getValue(yAxis))
                .put("label", formatValue(row.getValue(xAxis), ColumnType.TEXT));
            
            chartData.add(chartPoint);
        }
        
        return chartData;
    }
    
    /**
     * Generate chart configuration
     */
    private JsonObject generateChartConfig(JsonArray results, FormatOptions options) {
        return new JsonObject()
            .put("type", options.chartType != null ? options.chartType : "bar")
            .put("title", options.title != null ? options.title : "Query Results")
            .put("xLabel", options.chartXAxis)
            .put("yLabel", options.chartYAxis);
    }
    
    /**
     * Generate a narrative description of results
     */
    private String generateNarrative(JsonArray results, Map<String, ColumnType> columnTypes) {
        StringBuilder narrative = new StringBuilder();
        
        int rowCount = results.size();
        narrative.append("The query returned ").append(rowCount)
                .append(" ").append(rowCount == 1 ? "result" : "results").append(". ");
        
        if (rowCount > 0) {
            // Describe the first few results
            int describedCount = Math.min(3, rowCount);
            narrative.append("Here are the top ").append(describedCount).append(" entries: ");
            
            for (int i = 0; i < describedCount; i++) {
                JsonObject row = results.getJsonObject(i);
                narrative.append("\n").append(i + 1).append(". ");
                
                // Build description from key fields
                List<String> keyFields = new ArrayList<>();
                for (String column : row.fieldNames()) {
                    ColumnType type = columnTypes.get(column);
                    if (type != ColumnType.ID) { // Skip ID fields in narrative
                        String value = formatValue(row.getValue(column), type);
                        keyFields.add(column.replace("_", " ") + ": " + value);
                    }
                }
                narrative.append(String.join(", ", keyFields));
            }
            
            if (rowCount > describedCount) {
                narrative.append("\n... and ").append(rowCount - describedCount).append(" more.");
            }
        }
        
        return narrative.toString();
    }
    
    /**
     * Format results with summary information
     */
    private JsonArray formatWithSummary(JsonArray results, Map<String, ColumnType> columnTypes) {
        // Format the data
        JsonArray formatted = formatAsTable(results, columnTypes);
        
        // Add summary row if numeric columns exist
        boolean hasNumeric = columnTypes.values().stream()
            .anyMatch(type -> type == ColumnType.INTEGER || 
                            type == ColumnType.DECIMAL || 
                            type == ColumnType.CURRENCY);
        
        if (hasNumeric && results.size() > 1) {
            JsonObject summaryRow = new JsonObject();
            summaryRow.put("_summary", true);
            
            for (String column : columnTypes.keySet()) {
                ColumnType type = columnTypes.get(column);
                if (type == ColumnType.INTEGER || type == ColumnType.DECIMAL || 
                    type == ColumnType.CURRENCY) {
                    double sum = 0;
                    for (int i = 0; i < results.size(); i++) {
                        Object value = results.getJsonObject(i).getValue(column);
                        if (value instanceof Number) {
                            sum += ((Number) value).doubleValue();
                        }
                    }
                    summaryRow.put(column, formatValue(sum, type));
                } else {
                    summaryRow.put(column, "Total");
                }
            }
            
            formatted.add(summaryRow);
        }
        
        return formatted;
    }
    
    /**
     * Generate statistics for numeric columns
     */
    private JsonObject generateStatistics(JsonArray results, Map<String, ColumnType> columnTypes) {
        JsonObject stats = new JsonObject();
        
        for (String column : columnTypes.keySet()) {
            ColumnType type = columnTypes.get(column);
            if (type == ColumnType.INTEGER || type == ColumnType.DECIMAL || 
                type == ColumnType.CURRENCY || type == ColumnType.PERCENTAGE) {
                
                List<Double> values = new ArrayList<>();
                for (int i = 0; i < results.size(); i++) {
                    Object value = results.getJsonObject(i).getValue(column);
                    if (value instanceof Number) {
                        values.add(((Number) value).doubleValue());
                    }
                }
                
                if (!values.isEmpty()) {
                    JsonObject columnStats = new JsonObject();
                    columnStats.put("count", values.size());
                    columnStats.put("sum", values.stream().mapToDouble(Double::doubleValue).sum());
                    columnStats.put("avg", values.stream().mapToDouble(Double::doubleValue).average().orElse(0));
                    columnStats.put("min", Collections.min(values));
                    columnStats.put("max", Collections.max(values));
                    
                    stats.put(column, columnStats);
                }
            }
        }
        
        stats.put("totalRows", results.size());
        return stats;
    }
    
    /**
     * Generate a business-friendly summary
     */
    private String generateBusinessSummary(JsonArray results, JsonObject statistics) {
        StringBuilder summary = new StringBuilder();
        
        int totalRows = statistics.getInteger("totalRows", 0);
        summary.append("Found ").append(NUMBER_FORMAT.get().format(totalRows))
               .append(" ").append(totalRows == 1 ? "record" : "records").append(". ");
        
        // Add key statistics
        for (String column : statistics.fieldNames()) {
            if (!"totalRows".equals(column)) {
                JsonObject columnStats = statistics.getJsonObject(column);
                if (columnStats != null) {
                    double avg = columnStats.getDouble("avg", 0.0);
                    double max = columnStats.getDouble("max", 0.0);
                    
                    String colName = column.toLowerCase().replace("_", " ");
                    
                    if (column.toUpperCase().contains("AMOUNT") || 
                        column.toUpperCase().contains("PRICE") ||
                        column.toUpperCase().contains("REVENUE")) {
                        summary.append("Average ").append(colName).append(": ")
                               .append(CURRENCY_FORMAT.get().format(avg)).append(". ");
                        summary.append("Highest: ").append(CURRENCY_FORMAT.get().format(max)).append(". ");
                    }
                }
            }
        }
        
        return summary.toString();
    }
    
    /**
     * Format a single value based on its type
     */
    private String formatValue(Object value, ColumnType type) {
        if (value == null) {
            return "";
        }
        
        switch (type) {
            case CURRENCY:
                if (value instanceof Number) {
                    return CURRENCY_FORMAT.get().format(((Number) value).doubleValue());
                }
                break;
                
            case PERCENTAGE:
                if (value instanceof Number) {
                    return PERCENT_FORMAT.get().format(((Number) value).doubleValue());
                }
                break;
                
            case INTEGER:
                if (value instanceof Number) {
                    return NUMBER_FORMAT.get().format(((Number) value).longValue());
                }
                break;
                
            case DECIMAL:
                if (value instanceof Number) {
                    return String.format("%.2f", ((Number) value).doubleValue());
                }
                break;
                
            case DATE:
                if (value instanceof Date) {
                    LocalDate localDate = ((Date) value).toInstant()
                        .atZone(ZoneId.systemDefault())
                        .toLocalDate();
                    return DATE_FORMAT.format(localDate);
                }
                break;
                
            case DATETIME:
                if (value instanceof Date) {
                    LocalDateTime localDateTime = ((Date) value).toInstant()
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
                    return DATETIME_FORMAT.format(localDateTime);
                }
                break;
                
            case BOOLEAN:
                return value.toString().equalsIgnoreCase("true") ? "Yes" : "No";
                
            case ID:
                // Don't format IDs
                return value.toString();
                
            case TEXT:
            default:
                return value.toString();
        }
        
        return value.toString();
    }
    
    /**
     * Extract headers from results
     */
    private List<String> extractHeaders(JsonArray results) {
        if (results.isEmpty()) {
            return new ArrayList<>();
        }
        
        JsonObject firstRow = results.getJsonObject(0);
        List<String> headers = new ArrayList<>();
        
        for (String column : firstRow.fieldNames()) {
            // Convert column name to readable format
            String header = column.toLowerCase()
                .replace("_", " ")
                .replace("id", "ID");
            
            // Capitalize first letter of each word
            String[] words = header.split(" ");
            for (int i = 0; i < words.length; i++) {
                if (words[i].length() > 0 && !words[i].equals("ID")) {
                    words[i] = words[i].substring(0, 1).toUpperCase() + 
                              words[i].substring(1);
                }
            }
            
            headers.add(String.join(" ", words));
        }
        
        return headers;
    }
    
    /**
     * Create a pivot table from results
     */
    public JsonObject createPivotTable(JsonArray results, String rowField, String columnField, 
                                       String valueField, AggregationType aggregation) {
        JsonObject pivot = new JsonObject();
        Map<String, Map<String, List<Double>>> pivotData = new HashMap<>();
        
        // Collect data
        for (int i = 0; i < results.size(); i++) {
            JsonObject row = results.getJsonObject(i);
            String rowKey = String.valueOf(row.getValue(rowField));
            String colKey = String.valueOf(row.getValue(columnField));
            Object value = row.getValue(valueField);
            
            if (value instanceof Number) {
                pivotData.computeIfAbsent(rowKey, k -> new HashMap<>())
                        .computeIfAbsent(colKey, k -> new ArrayList<>())
                        .add(((Number) value).doubleValue());
            }
        }
        
        // Aggregate data
        JsonArray pivotRows = new JsonArray();
        for (Map.Entry<String, Map<String, List<Double>>> rowEntry : pivotData.entrySet()) {
            JsonObject pivotRow = new JsonObject();
            pivotRow.put(rowField, rowEntry.getKey());
            
            for (Map.Entry<String, List<Double>> colEntry : rowEntry.getValue().entrySet()) {
                double aggregatedValue = aggregate(colEntry.getValue(), aggregation);
                pivotRow.put(colEntry.getKey(), aggregatedValue);
            }
            
            pivotRows.add(pivotRow);
        }
        
        pivot.put("rows", pivotRows);
        pivot.put("rowField", rowField);
        pivot.put("columnField", columnField);
        pivot.put("valueField", valueField);
        pivot.put("aggregation", aggregation.toString());
        
        return pivot;
    }
    
    /**
     * Aggregate values based on type
     */
    private double aggregate(List<Double> values, AggregationType type) {
        if (values.isEmpty()) return 0;
        
        switch (type) {
            case SUM:
                return values.stream().mapToDouble(Double::doubleValue).sum();
            case AVG:
                return values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            case MIN:
                return Collections.min(values);
            case MAX:
                return Collections.max(values);
            case COUNT:
                return values.size();
            default:
                return values.get(0);
        }
    }
    
    /**
     * Column types
     */
    private enum ColumnType {
        TEXT, INTEGER, DECIMAL, CURRENCY, PERCENTAGE, DATE, DATETIME, BOOLEAN, ID
    }
    
    /**
     * Output types
     */
    public enum OutputType {
        TABLE, CHART, NARRATIVE, SUMMARY
    }
    
    /**
     * Aggregation types
     */
    public enum AggregationType {
        SUM, AVG, MIN, MAX, COUNT
    }
    
    /**
     * Format options
     */
    public static class FormatOptions {
        public OutputType outputType = OutputType.SUMMARY;
        public String title;
        public String chartType; // bar, line, pie
        public String chartXAxis;
        public String chartYAxis;
        public boolean includeStatistics = true;
        public boolean translateEnums = true;
        
        public static FormatOptions fromJson(JsonObject json) {
            FormatOptions options = new FormatOptions();
            if (json != null) {
                String type = json.getString("outputType", "SUMMARY");
                options.outputType = OutputType.valueOf(type.toUpperCase());
                options.title = json.getString("title");
                options.chartType = json.getString("chartType");
                options.chartXAxis = json.getString("chartXAxis");
                options.chartYAxis = json.getString("chartYAxis");
                options.includeStatistics = json.getBoolean("includeStatistics", true);
                options.translateEnums = json.getBoolean("translateEnums", true);
            }
            return options;
        }
    }
    
    /**
     * Formatted result
     */
    public static class FormattedResult {
        public JsonArray formattedData;
        public List<String> headers;
        public String summary;
        public String narrative;
        public JsonObject statistics;
        public JsonObject chartConfig;
        
        public JsonObject toJson() {
            JsonObject json = new JsonObject();
            if (formattedData != null) json.put("data", formattedData);
            if (headers != null) json.put("headers", new JsonArray(headers));
            if (summary != null) json.put("summary", summary);
            if (narrative != null) json.put("narrative", narrative);
            if (statistics != null) json.put("statistics", statistics);
            if (chartConfig != null) json.put("chartConfig", chartConfig);
            return json;
        }
    }
}