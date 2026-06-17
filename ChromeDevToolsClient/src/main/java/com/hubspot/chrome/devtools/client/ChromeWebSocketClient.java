package com.hubspot.chrome.devtools.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hubspot.chrome.devtools.base.ChromeResponse;
import com.hubspot.chrome.devtools.client.core.Event;
import com.hubspot.chrome.devtools.client.core.EventType;
import com.hubspot.chrome.devtools.client.core.target.SessionID;
import com.hubspot.chrome.devtools.client.exceptions.ChromeDevToolsException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChromeWebSocketClient extends WebSocketClient {

  private static final Logger LOG = LoggerFactory.getLogger(ChromeWebSocketClient.class);
  private static final Map<String, EventType> EVENT_TYPES = Arrays
    .stream(EventType.values())
    .collect(Collectors.toMap(EventType::getType, Function.identity()));

  private final long actionTimeoutMillis;
  private final ConcurrentMap<Integer, CompletableFuture<ChromeResponse>> pendingResponses =
    new ConcurrentHashMap<>();
  private final ObjectMapper objectMapper;
  private final Map<String, ChromeEventListener> chromeEventListeners;
  private final ExecutorService executorService;

  public ChromeWebSocketClient(
    URI uri,
    ObjectMapper objectMapper,
    Map<String, ChromeEventListener> chromeEventListeners,
    ExecutorService executorService,
    long actionTimeoutMillis
  ) {
    super(uri);
    this.objectMapper = objectMapper;
    this.chromeEventListeners = chromeEventListeners;
    this.executorService = executorService;
    this.actionTimeoutMillis = actionTimeoutMillis;
  }

  @Override
  public void onOpen(ServerHandshake handshakedata) {
    LOG.debug("Connected ({})", handshakedata.getHttpStatusMessage());

    // Connection checking triggers false positive lost connection checks, which causes premature
    // websocket disconnects. Chrome doesn't seem to respond to websocket pings with a pong response,
    // so disabling this is only our option at the current time.
    disableConnectionLostChecking();
  }

  private void disableConnectionLostChecking() {
    this.setConnectionLostTimeout(-1);
  }

  @Override
  public void onClose(int code, String reason, boolean remote) {
    LOG.debug("Disconnected from session ({}: {})", code, reason);

    pendingResponses.forEach((id, future) ->
      future.completeExceptionally(
        new ChromeDevToolsException("Websocket disconnected before response " + id)
      )
    );

    pendingResponses.clear();
  }

  @Override
  public void onMessage(String message) {
    LOG.trace("Received message: {}", message);

    try {
      ChromeResponse response = objectMapper.readValue(message, ChromeResponse.class);
      if (response.isResponse()) {
        CompletableFuture<ChromeResponse> future = pendingResponses.remove(
          response.getId()
        );

        if (future != null) {
          future.complete(response);
        } else {
          LOG.debug("Received response for unknown id: {}", response.getId());
        }
      } else if (response.isEvent()) {
        Event event = objectMapper.readValue(message, Event.class);
        SessionID sessionId = response.getSessionId() == null
          ? null
          : new SessionID(response.getSessionId());
        for (ChromeEventListener eventListener : chromeEventListeners.values()) {
          EventType type = EVENT_TYPES.get(response.getMethod());
          executorService.submit(() -> eventListener.onEvent(sessionId, type, event));
        }
      } else if (response.isError()) {
        LOG.error("{}", response.getError());
        CompletableFuture<ChromeResponse> future = pendingResponses.remove(
          response.getId()
        );
        if (future != null) {
          future.completeExceptionally(
            new ChromeDevToolsException(
              response.getError().getMessage(),
              response.getError().getCode()
            )
          );
        } else {
          LOG.debug("Received error for unknown id: {}", response.getId());
        }
      }
    } catch (IOException ioe) {
      LOG.warn("Could not parse response from chrome. Ignoring this response.", ioe);
    }
  }

  @Override
  public void onMessage(ByteBuffer message) {
    LOG.warn(
      "Not set up to handle byte buffer, received buffer of size {}",
      message.array().length
    );
  }

  @Override
  public void onError(Exception ex) {
    LOG.error("Websocket exception for session", ex);

    pendingResponses.forEach((id, future) -> future.completeExceptionally(ex));

    pendingResponses.clear();
  }

  void send(int id, String json) {
    CompletableFuture<ChromeResponse> future = new CompletableFuture<>();
    pendingResponses.put(id, future);
    try {
      send(json);
    } catch (Exception e) {
      pendingResponses.remove(id);
      future.completeExceptionally(e);
      throw e;
    }
  }

  public ChromeResponse getResponse(int id) {
    CompletableFuture<ChromeResponse> future = pendingResponses.get(id);
    if (future == null) {
      throw new ChromeDevToolsException("No pending request for id " + id);
    }

    try {
      return future.get(actionTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof ChromeDevToolsException) {
        throw (ChromeDevToolsException) e.getCause();
      }
      throw new ChromeDevToolsException(e.getCause());
    } catch (Exception e) {
      throw new ChromeDevToolsException(e);
    } finally {
      pendingResponses.remove(id);
    }
  }
}
