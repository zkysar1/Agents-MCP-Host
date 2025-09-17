package agents.director.services;

import agents.director.Driver;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

import static agents.director.Driver.logLevel;

/**
 * Centralized logging service for the ZAK Agent.
 *
 * <p>New behaviour overview:</p>
 * <ul>
 *   <li>Writes buffered logs to <code>logs/current.csv</code> every <b>30&nbsp;seconds</b>.</li>
 *   <li>After <b>1&nbsp;hour</b> the file is rotated: it is renamed to a timestamped
 *       CSV (start‑time of the block) and a fresh <code>current.csv</code> is created.</li>
 *   <li>Only the latest <b>100</b> rotated files are kept (≈1 hour of history); older
 *       files are deleted automatically.</li>
 *   <li>Continues to listen for <code>saveAllDataToFiles_OnTermination</code> to flush
 *       immediately on shutdown.</li>
 * </ul>
 */
public class Logger extends AbstractVerticle {

  /* ---------- configuration ---------- */

  private static final long FLUSH_INTERVAL_MS  = 20_000;     // 20 s - updated for better debugging
  private static final long ROTATE_INTERVAL_MS = 86_400_000L;  // 24 hour
  private static final int  MAX_HISTORIC_FILES = 12;  // keep last 12 days or a few days if restarting
  private static final String HEADER = "Message,Level,Class,Category,Character,SequenceReceived,EpochTimeMillis\n";
  private static final DateTimeFormatter FILE_STAMP =
          DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").withZone(ZoneId.of("UTC"));  // daily stamp

  /* ---------- state ---------- */

  private final LinkedList<String> buffer = new LinkedList<>();
  private int sequenceCounter = 0;
  private long currentBlockStart;

  /* ---------- paths ---------- */

  private String logsDir;
  private String currentFile;

  @Override
  public void start() {
    logsDir    = Driver.zakAgentPath + "/logs";
    currentFile = logsDir + "/current.csv";

    ensureDirectory(() -> {
      currentBlockStart = System.currentTimeMillis();
      setupConsumers();
      scheduleFlush();
    });
  }

  /* ---------- initialisation ---------- */

  private void ensureDirectory(Runnable ready) {
    vertx.fileSystem().mkdirs(logsDir, r -> {
      if (r.succeeded()) {
        vertx.fileSystem().writeFile(currentFile, Buffer.buffer(HEADER), x -> {
          if (x.succeeded()) {
            ready.run();
          } else {
            System.err.println("Failed to write initial log file: " + x.cause().getMessage());
          }
        });
      } else {
        System.err.println("Failed to create logs directory: " + r.cause().getMessage());
      }
    });
  }

  private void setupConsumers() {
    // Receive log events
    vertx.eventBus().consumer("log", msg -> {
      sequenceCounter++;
      long now = System.currentTimeMillis();
      buffer.add(msg.body() + "," + sequenceCounter + "," + now + "\n");

    });

    vertx.eventBus().consumer("saveAllDataToFiles_OnTermination", m -> flushBuffer(ar -> {
      // Flush complete, no need to log this
    }));
  }

  /* ---------- periodic tasks ---------- */

  private void scheduleFlush() {
    vertx.setPeriodic(FLUSH_INTERVAL_MS, id -> {
      long now = System.currentTimeMillis();
      if (now - currentBlockStart >= ROTATE_INTERVAL_MS) {
        rotate(now, r -> flushBuffer(null));
      } else {
        flushBuffer(null);
      }
    });
  }

  /* ---------- flush / rotate ---------- */

  private void flushBuffer(io.vertx.core.Handler<AsyncResult<Void>> handler) {
    if (buffer.isEmpty()) {
      if (handler != null) handler.handle(io.vertx.core.Future.succeededFuture());
      return;
    }

    StringBuilder sb = new StringBuilder();
    buffer.forEach(sb::append);
    buffer.clear();

    vertx.fileSystem().open(currentFile, new OpenOptions().setAppend(true), openRes -> {
      if (openRes.succeeded()) {
        AsyncFile file = openRes.result();
        file.write(Buffer.buffer(sb.toString())).onComplete(wr -> {
          file.close();
          if (handler != null) handler.handle(wr.mapEmpty());
        });
      } else {
        if (handler != null) handler.handle(openRes.mapEmpty());
      }
    });
  }

  private void rotate(long now, io.vertx.core.Handler<AsyncResult<Void>> after) {
    String rotatedPath = logsDir + "/" + FILE_STAMP.format(Instant.ofEpochMilli(currentBlockStart)) + ".csv";

    flushBuffer(flush -> vertx.fileSystem().move(currentFile, rotatedPath, mv -> {
      if (mv.succeeded()) {
        // Permissions are handled automatically by FileSystemHelper during move
        
        currentBlockStart = now;
        vertx.fileSystem().writeFile(currentFile, Buffer.buffer(HEADER), hdr -> {
            hdr.succeeded();// Permissions are handled automatically by FileSystemHelper
            cleanupOld();
          if (after != null) after.handle(mv.mapEmpty());
        });
      } else {
        if (after != null) after.handle(mv.mapEmpty());
      }
    }));
  }

  private void cleanupOld() {
    vertx.fileSystem().readDir(logsDir, "glob:**/*.csv", dir -> {
      if (dir.failed()) return;

      List<String> history = dir.result().stream()
              .filter(p -> !p.endsWith("current.csv"))
              .sorted()
              .toList();

      int excess = history.size() - MAX_HISTORIC_FILES;
      if (excess > 0) {
        history.subList(0, excess).forEach(p -> vertx.fileSystem().delete(p, d -> {}));
      }
    });
  }
}
