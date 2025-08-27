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

import static agents.director.Driver.logLevel;

/**
 * Service for interacting with OpenAI's Chat Completions API.
 * This is a singleton service (not a verticle) that can be used by any verticle.
 */
public class LlmAPIService {
  private static LlmAPIService instance;
  private WebClient webClient;
  private Vertx vertx;
  private String apiKey;
  private static final String OPENAI_API_URL = "apis.openai.com";
  private static final String CHAT_COMPLETIONS_PATH = "/v1/chat/completions";
  private static final String MODEL = "gpt-4o-mini-2024-07-18";
  private static final int REQUEST_TIMEOUT_MS = 30000; // 30 seconds
  
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
   * Initialize the service with Vertx instance
   * @param vertx The Vertx instance
   * @return true if initialization successful, false if API key missing
   */
  public boolean setupService(Vertx vertx) {
    this.vertx = vertx;
    this.apiKey = System.getenv("OPENAI_API_KEY");
    
    if (apiKey == null || apiKey.trim().isEmpty()) {
      System.err.println("WARNING: OPENAI_API_KEY environment variable not set");
      System.err.println("The conversation API will not function without a valid OpenAI API key");
      vertx.eventBus().publish("log", 
        "OPENAI_API_KEY not configured - LLM service disabled,0,LlmAPIService,Configuration,Error");
      return false;
    }
    
    // Create WebClient with options
    WebClientOptions options = new WebClientOptions()
      .setUserAgent("ZAK-Agent/1.0")
      .setConnectTimeout(5000)
      .setSsl(true)
      .setTrustAll(false);
    
    this.webClient = WebClient.create(vertx, options);
    
    System.out.println("LlmAPIService initialized with OpenAI API");
    vertx.eventBus().publish("log", 
      "LlmAPIService initialized successfully,1,LlmAPIService,StartUp,System");
    
    return true;
  }
  
  /**
   * Check if the service is properly initialized
   * @return true if service is ready to use
   */
  public boolean isInitialized() {
    return webClient != null && apiKey != null;
  }
  
  /**
   * Make a chat completion request to OpenAI
   * @param messages The messages array for the conversation
   * @return Future containing the OpenAI response
   */
  public Future<JsonObject> chatCompletion(JsonArray messages) {
    return chatCompletion(messages, null);
  }
  
  /**
   * Make a chat completion request to OpenAI with optional streaming
   * @param messages The messages array for the conversation
   * @param streamId Optional stream ID for publishing events
   * @return Future containing the OpenAI response
   */
  public Future<JsonObject> chatCompletion(JsonArray messages, String streamId) {
    Promise<JsonObject> promise = Promise.promise();
    
    if (!isInitialized()) {
      promise.fail("LlmAPIService not properly initialized - check OPENAI_API_KEY");
      return promise.future();
    }
    
    // Build the request body
    JsonObject requestBody = new JsonObject()
      .put("model", MODEL)
      .put("messages", messages)
      .put("temperature", 0.7)
      .put("max_tokens", 2000);
    
    if (logLevel >= 3) {
      System.out.println("[DEBUG] Sending request to OpenAI: " + requestBody.encodePrettily());
    }
    
    // Publish LLM request event if streaming
    if (streamId != null) {
      StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
      publisher.publishLLMRequest(messages);
    }
    
    // Create the HTTP request
    HttpRequest<Buffer> request = webClient
      .post(443, OPENAI_API_URL, CHAT_COMPLETIONS_PATH)
      .timeout(REQUEST_TIMEOUT_MS)
      .putHeader("Authorization", "Bearer " + apiKey)
      .putHeader("Content-Type", "application/json");
    
    // Send the request
    request.sendJsonObject(requestBody, ar -> {
      if (ar.succeeded()) {
        HttpResponse<Buffer> response = ar.result();
        
        if (logLevel >= 3) {
          System.out.println("[DEBUG] OpenAI response status: " + response.statusCode());
        }
        
        if (response.statusCode() == 200) {
          try {
            JsonObject responseBody = response.bodyAsJsonObject();
            
            if (logLevel >= 4) {
              System.out.println("[DEBUG] OpenAI response body: " + responseBody.encodePrettily());
            }
            
            vertx.eventBus().publish("log", 
              "OpenAI API call successful,2,LlmAPIService,API,Success");
            
            // Publish LLM response event if streaming
            if (streamId != null) {
              StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
              JsonObject usage = responseBody.getJsonObject("usage", new JsonObject());
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
            promise.fail("Failed to parse OpenAI response: " + e.getMessage());
          }
        } else if (response.statusCode() == 429) {
          // Rate limit exceeded
          JsonObject errorBody = response.bodyAsJsonObject();
          String errorMessage = errorBody.getJsonObject("error", new JsonObject())
            .getString("message", "Rate limit exceeded");
          
          vertx.eventBus().publish("log", 
            "OpenAI API rate limit exceeded,0,LlmAPIService,API,RateLimit");
          
          promise.fail("Rate limit: " + errorMessage);
        } else if (response.statusCode() == 401) {
          // Invalid API key
          vertx.eventBus().publish("log", 
            "OpenAI API authentication failed - invalid API key,0,LlmAPIService,API,Auth");
          
          promise.fail("Invalid OpenAI API key");
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
            "OpenAI API error " + response.statusCode() + ": " + errorBody + 
            ",0,LlmAPIService,API,Error");
          
          promise.fail("OpenAI API error (" + response.statusCode() + "): " + errorBody);
        }
      } else {
        // Network or timeout error
        String errorMessage = ar.cause().getMessage();
        
        if (errorMessage.contains("timeout")) {
          vertx.eventBus().publish("log", 
            "OpenAI API request timeout,0,LlmAPIService,API,Timeout");
          promise.fail("Request timeout - OpenAI API took too long to respond");
        } else {
          vertx.eventBus().publish("log", 
            "OpenAI API connection failed: " + errorMessage + ",0,LlmAPIService,API,Network");
          promise.fail("Failed to connect to OpenAI API: " + errorMessage);
        }
      }
    });
    
    return promise.future();
  }

}