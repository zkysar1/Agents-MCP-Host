package agents.director.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static agents.director.Driver.logLevel;

/**
 * Service for interacting with LLM Chat Completions API.
 * This is a singleton service (not a verticle) that can be used by any verticle.
 * Supports any OpenAI-compatible API endpoint.
 */
public class LlmAPIService {
  private static LlmAPIService instance;
  private WebClient webClient;
  private Vertx vertx;

  // LLM configuration loaded from environment variables
  private String LLM_API_KEY;
  private String LLM_API_URL;
  private String LLM_CHAT_COMPLETIONS_PATH;
  private String LLM_MODEL_NAME;
  private int LLM_REQUEST_TIMEOUT_MS;
  
  private LlmAPIService() {
    // Private constructor for singleton
  }
  
  /**
   * Get the singleton instance
   * @return The LlmAPIService instance
   */
  public static synchronized LlmAPIService getInstance() {
    if (instance == null) {
      instance = new LlmAPIService();
    }
    return instance;
  }
  
  /**
   * Get required environment variable or throw exception
   * Checks both System.getProperty() (from dotenv) and System.getenv() (from OS)
   */
  private String getRequiredEnv(String key) {
    // First try system properties (loaded by dotenv)
    String value = System.getProperty(key);

    // If not found, try environment variables
    if (value == null || value.trim().isEmpty()) {
      value = System.getenv(key);
    }

    // If still not found, throw error
    if (value == null || value.trim().isEmpty()) {
      throw new RuntimeException(
        "Required configuration '" + key + "' is not set. " +
        "Please ensure .env.local file contains all required LLM configuration."
      );
    }
    return value.trim();
  }

  /**
   * Initialize the service with Vertx instance
   * @param vertx The Vertx instance
   * @return true if initialization successful, false if configuration missing
   */
  public boolean setupService(Vertx vertx) {
    this.vertx = vertx;

    try {
      // Load all required configuration from environment
      this.LLM_API_KEY = getRequiredEnv("LLM_API_KEY");
      this.LLM_API_URL = getRequiredEnv("LLM_API_URL");
      this.LLM_CHAT_COMPLETIONS_PATH = getRequiredEnv("LLM_CHAT_COMPLETIONS_PATH");
      this.LLM_MODEL_NAME = getRequiredEnv("LLM_MODEL_NAME");

      // Parse timeout as integer
      String timeoutStr = getRequiredEnv("LLM_REQUEST_TIMEOUT_MS");
      try {
        this.LLM_REQUEST_TIMEOUT_MS = Integer.parseInt(timeoutStr);
      } catch (NumberFormatException e) {
        throw new RuntimeException(
          "Invalid LLM_REQUEST_TIMEOUT_MS value: '" + timeoutStr + "'. Must be a valid number in milliseconds."
        );
      }

      // Log loaded configuration (without API key)
      vertx.eventBus().publish("log", "LLM Service Configuration loaded:,1,LlmAPIService,Configuration,Info");
      vertx.eventBus().publish("log", "  API URL: " + LLM_API_URL + ",2,LlmAPIService,Configuration,Info");
      vertx.eventBus().publish("log", "  Endpoint: " + LLM_CHAT_COMPLETIONS_PATH + ",2,LlmAPIService,Configuration,Info");
      vertx.eventBus().publish("log", "  Model: " + LLM_MODEL_NAME + ",2,LlmAPIService,Configuration,Info");
      vertx.eventBus().publish("log", "  Timeout: " + LLM_REQUEST_TIMEOUT_MS + "ms,2,LlmAPIService,Configuration,Info");
      vertx.eventBus().publish("log", "  API Key: [CONFIGURED],2,LlmAPIService,Configuration,Info");

    } catch (RuntimeException e) {
      vertx.eventBus().publish("log", "ERROR: " + e.getMessage() + ",0,LlmAPIService,Configuration,Error");
      vertx.eventBus().publish("log", "LLM service disabled due to missing configuration,0,LlmAPIService,Configuration,Error");
      return false;
    }
    
    // Create WebClient with options
    WebClientOptions options = new WebClientOptions()
      .setUserAgent("ZAK-Agent/1.0")
      .setConnectTimeout(5000)
      .setSsl(true)
      .setTrustAll(false);
    
    this.webClient = WebClient.create(vertx, options);
    
    vertx.eventBus().publish("log", "LlmAPIService initialized with LLM API at " + LLM_API_URL + ",2,LlmAPIService,Service,System");
    vertx.eventBus().publish("log",
      "LlmAPIService initialized successfully,1,LlmAPIService,StartUp,System");
    
    return true;
  }
  
  /**
   * Check if the service is properly initialized
   * @return true if service is ready to use
   */
  public boolean isInitialized() {
    return webClient != null && LLM_API_KEY != null;
  }
  
  /**
   * Make a chat completion request to the LLM API
   * @param messages The messages array for the conversation
   * @return Future containing the LLM response
   */
  public Future<JsonObject> chatCompletion(JsonArray messages) {
    return chatCompletion(messages, null);
  }
  
  /**
   * Make a chat completion request to the LLM API with optional streaming
   * @param messages The messages array for the conversation
   * @param streamId Optional stream ID for publishing events
   * @return Future containing the LLM response
   */
  public Future<JsonObject> chatCompletion(JsonArray messages, String streamId) {
    Promise<JsonObject> promise = Promise.<JsonObject>promise();
    
    if (!isInitialized()) {
      promise.fail("LlmAPIService not properly initialized - check LLM configuration in .env.local");
      return promise.future();
    }
    
    // Build the request body
    JsonObject requestBody = new JsonObject()
      .put("model", LLM_MODEL_NAME)  // Standard field name for OpenAI-compatible APIs
      .put("messages", messages)
      .put("temperature", 0.7)
      .put("max_tokens", 2000);
    
    if (logLevel >= 3) {
      vertx.eventBus().publish("log", "[DEBUG] Sending request to LLM API: " + requestBody.encodePrettily() + ",2,LlmAPIService,Service,System");
    }
    
    // Publish LLM request event if streaming
    if (streamId != null) {
      StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
      publisher.publishLLMRequest(messages);
    }
    
    // Create the HTTP request
    HttpRequest<Buffer> request = webClient
      .post(443, LLM_API_URL, LLM_CHAT_COMPLETIONS_PATH)
      .timeout(LLM_REQUEST_TIMEOUT_MS)
      .putHeader("Authorization", "Bearer " + LLM_API_KEY)
      .putHeader("Content-Type", "application/json");
    
    // Log the API call
    vertx.eventBus().publish("log", "Calling LLM API at " + LLM_API_URL + LLM_CHAT_COMPLETIONS_PATH + ",2,LlmAPIService,API,Request");
    
    // Send the request
    request.sendJsonObject(requestBody, ar -> {
      if (ar.succeeded()) {
        HttpResponse<Buffer> response = ar.result();
        
        if (logLevel >= 3) {
          vertx.eventBus().publish("log", "[DEBUG] LLM API response status: " + response.statusCode() + ",2,LlmAPIService,Service,System");
        }
        
        if (response.statusCode() == 200) {
          try {
            JsonObject responseBody = response.bodyAsJsonObject();
            
            if (logLevel >= 4) {
              vertx.eventBus().publish("log", "[DEBUG] LLM API response body: " + responseBody.encodePrettily() + ",2,LlmAPIService,Service,System");
            }
            
            // Log token usage
            JsonObject usage = responseBody.getJsonObject("usage", new JsonObject());
            vertx.eventBus().publish("log",
              "LLM API call successful - Tokens: " + usage.getInteger("total_tokens", 0) +
              " (prompt: " + usage.getInteger("prompt_tokens", 0) +
              ", completion: " + usage.getInteger("completion_tokens", 0) + "),2,LlmAPIService,API,Success");
            
            // Publish LLM response event if streaming
            if (streamId != null) {
              StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
              // usage already declared above
              JsonObject metadata = new JsonObject()
                .put("model", responseBody.getString("model"))
                .put("totalTokens", usage.getInteger("total_tokens", 0))
                .put("promptTokens", usage.getInteger("prompt_tokens", 0))
                .put("completionTokens", usage.getInteger("completion_tokens", 0));
              
              String content = responseBody.getJsonArray("choices")
                .getJsonObject(0)
                .getJsonObject("message")
                .getString("content");
              
              publisher.publishLLMResponse(content, metadata);
            }
            
            promise.complete(responseBody);
          } catch (Exception e) {
            promise.fail("Failed to parse LLM API response: " + e.getMessage());
          }
        } else if (response.statusCode() == 429) {
          // Rate limit exceeded
          JsonObject errorBody = response.bodyAsJsonObject();
          String errorMessage = errorBody.getJsonObject("error", new JsonObject())
            .getString("message", "Rate limit exceeded");
          
          vertx.eventBus().publish("log",
            "LLM API rate limit exceeded,0,LlmAPIService,API,RateLimit");
          
          promise.fail("Rate limit: " + errorMessage);
        } else if (response.statusCode() == 401) {
          // Invalid API key
          vertx.eventBus().publish("log",
            "LLM API authentication failed - invalid API key,0,LlmAPIService,API,Auth");

          promise.fail("Invalid LLM API key");
        } else {
          // Other errors
          String errorBody = "";
          try {
            JsonObject error = response.bodyAsJsonObject();
            errorBody = error.getJsonObject("error", new JsonObject())
              .getString("message", "Unknown error");
          } catch (Exception e) {
            errorBody = response.bodyAsString();
          }
          
          vertx.eventBus().publish("log",
            "LLM API error " + response.statusCode() + ": " + errorBody +
            ",0,LlmAPIService,API,Error");

          promise.fail("LLM API error (" + response.statusCode() + "): " + errorBody);
        }
      } else {
        // Network or timeout error
        String errorMessage = ar.cause().getMessage();
        
        if (errorMessage.contains("timeout")) {
          vertx.eventBus().publish("log",
            "LLM API request timeout,0,LlmAPIService,API,Timeout");
          promise.fail("Request timeout - LLM API took too long to respond");
        } else {
          vertx.eventBus().publish("log",
            "LLM API connection failed: " + errorMessage + ",0,LlmAPIService,API,Network");
          promise.fail("Failed to connect to LLM API: " + errorMessage);
        }
      }
    });
    
    return promise.future();
  }
  
  /**
   * Make a chat completion request to the LLM API with custom parameters
   * @param messages The messages as a List of encoded JSON strings
   * @param temperature The temperature parameter (0.0 - 1.0)
   * @param maxTokens Maximum tokens in response
   * @return CompletableFuture containing the LLM response
   */
  public CompletableFuture<JsonObject> chatCompletion(List<String> messages, double temperature, int maxTokens) {
    // Convert List<String> to JsonArray
    JsonArray messageArray = new JsonArray();
    for (String msgStr : messages) {
      try {
        messageArray.add(new JsonObject(msgStr));
      } catch (Exception e) {
        // If parsing fails, assume it's a simple content string
        messageArray.add(new JsonObject().put("role", "user").put("content", msgStr));
      }
    }
    
    // Build the request body
    JsonObject requestBody = new JsonObject()
      .put("model", LLM_MODEL_NAME)  // Standard field name for OpenAI-compatible APIs
      .put("messages", messageArray)
      .put("temperature", temperature)
      .put("max_tokens", maxTokens);
    
    // Use CompletableFuture to adapt Vert.x Future
    CompletableFuture<JsonObject> future = new CompletableFuture<>();
    
    if (!isInitialized()) {
      future.completeExceptionally(new IllegalStateException("LlmAPIService not properly initialized - check LLM configuration in .env.local"));
      return future;
    }
    
    // Create the HTTP request
    HttpRequest<Buffer> request = webClient
      .post(443, LLM_API_URL, LLM_CHAT_COMPLETIONS_PATH)
      .timeout(LLM_REQUEST_TIMEOUT_MS)
      .putHeader("Authorization", "Bearer " + LLM_API_KEY)
      .putHeader("Content-Type", "application/json");
    
    // Log the API call
    vertx.eventBus().publish("log", "Calling LLM API at " + LLM_API_URL + LLM_CHAT_COMPLETIONS_PATH + ",2,LlmAPIService,API,Request");
    
    // Send the request
    request.sendJsonObject(requestBody, ar -> {
      if (ar.succeeded()) {
        HttpResponse<Buffer> response = ar.result();
        
        if (response.statusCode() == 200) {
          try {
            JsonObject responseBody = response.bodyAsJsonObject();
            future.complete(responseBody);
          } catch (Exception e) {
            future.completeExceptionally(new RuntimeException("Failed to parse LLM API response: " + e.getMessage()));
          }
        } else {
          String errorBody = "";
          try {
            JsonObject error = response.bodyAsJsonObject();
            errorBody = error.getJsonObject("error", new JsonObject())
              .getString("message", "Unknown error");
          } catch (Exception e) {
            errorBody = response.bodyAsString();
          }
          future.completeExceptionally(new RuntimeException("LLM API error (" + response.statusCode() + "): " + errorBody));
        }
      } else {
        future.completeExceptionally(ar.cause());
      }
    });
    
    return future;
  }

}