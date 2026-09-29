package compapp.web;

import compapp.db.Store;
import compapp.engine.DefinitionService;
import compapp.engine.Engine;
import compapp.engine.EngineException;
import compapp.engine.LeaseManager;
import compapp.model.Clock;
import compapp.model.Definition;
import compapp.model.Json;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class ApiServer {

    private final int port;
    private final Store store;
    private final DefinitionService definitions;
    private final Engine engine;
    private final LeaseManager leases;
    private final Clock clock;
    private final boolean autoWorker;
    private HttpServer server;

    public ApiServer(int port, Store store, DefinitionService definitions, Engine engine,
                     LeaseManager leases, Clock clock, boolean autoWorker) {
        this.port = port;
        this.store = store;
        this.definitions = definitions;
        this.engine = engine;
        this.leases = leases;
        this.clock = clock;
        this.autoWorker = autoWorker;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/api/", this::handleApi);
        server.createContext("/", this::handleStatic);
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();
        if (autoWorker) {
            startBackgroundWorker();
        }
    }

    public int boundPort() {
        return server.getAddress().getPort();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void startBackgroundWorker() {
        Thread worker = new Thread(() -> {
            String owner = "bg-worker";
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    List<Map<String, Object>> active;
                    try (Store.Tx conn = store.open()) {
                        active = store.query(conn,
                                "SELECT id FROM instances WHERE status IN ('RUNNING','AWAIT_MANUAL') ORDER BY id");
                    }
                    for (Map<String, Object> row : active) {
                        long id = ((Number) row.get("id")).longValue();
                        try {
                            autoConfirmUnknowns(id, owner);
                            engine.tick(id, owner);
                        } catch (EngineException e) {
                            // lease contention / stale fence / version conflict: retry later
                        }
                    }
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception ignored) {
                    // keep the demo alive
                }
            }
        }, "saga-bg-worker");
        worker.setDaemon(true);
        worker.start();
    }

    /**
     * Demo automation: an UNKNOWN result is resolved by the scripted confirm
     * verdict (simulating a reconciliation call). The manual confirm endpoint and
     * its optimistic-version conflict remain available on top of this.
     */
    @SuppressWarnings("unchecked")
    private void autoConfirmUnknowns(long instanceId, String worker) {
        Map<String, Object> view = engine.instanceView(instanceId);
        for (Object stepObj : (List<Object>) view.get("steps")) {
            Map<String, Object> step = (Map<String, Object>) stepObj;
            if (!"AWAIT_CONFIRM".equals(step.get("status"))) {
                continue;
            }
            int version = ((Number) step.get("step_version")).intValue();
            engine.manualConfirm(instanceId, (String) step.get("step_id"),
                    "SUCCESS", "auto-reconciler", version);
        }
    }

    private void handleStatic(HttpExchange ex) throws IOException {
        String rawPath = ex.getRequestURI().getPath();
        if (rawPath.equals("/") || rawPath.isBlank()) {
            rawPath = "/index.html";
        }
        if (rawPath.contains("..")) {
            sendText(ex, 400, "bad path");
            return;
        }
        String resource = "/web" + rawPath;
        try (var in = ApiServer.class.getResourceAsStream(resource)) {
            if (in == null) {
                sendText(ex, 404, "not found");
                return;
            }
            byte[] body = in.readAllBytes();
            ex.getResponseHeaders().add("Content-Type", contentType(rawPath));
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        }
    }

    private void sendText(HttpExchange ex, int status, String text) throws IOException {
        byte[] msg = text.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(status, msg.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(msg);
        }
    }

    private String contentType(String path) {
        if (path.endsWith(".html")) {
            return "text/html; charset=utf-8";
        }
        if (path.endsWith(".js")) {
            return "application/javascript; charset=utf-8";
        }
        if (path.endsWith(".css")) {
            return "text/css; charset=utf-8";
        }
        return "application/octet-stream";
    }

    private void handleApi(HttpExchange ex) throws IOException {
        try {
            route(ex);
        } catch (EngineException e) {
            int status = switch (e.code()) {
                case "not_found" -> 404;
                case "conflict", "stale_lease", "version_conflict" -> 409;
                default -> 400;
            };
            writeJson(ex, status, Map.of("error", e.code(), "message", e.getMessage()));
        } catch (IllegalArgumentException e) {
            writeJson(ex, 400, Map.of("error", "invalid", "message", String.valueOf(e.getMessage())));
        } catch (Exception e) {
            writeJson(ex, 500, Map.of("error", "internal", "message", String.valueOf(e.getMessage())));
        } finally {
            ex.close();
        }
    }

    private void route(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod();
        String path = ex.getRequestURI().getPath();
        String[] parts = path.substring("/api/".length()).split("/");

        if ("GET".equals(method) && parts.length == 1 && parts[0].equals("health")) {
            writeJson(ex, 200, Map.of("ok", true, "clockMs", clock.nowMs()));
            return;
        }
        if ("GET".equals(method) && parts[0].equals("definitions") && parts.length == 1) {
            List<Map<String, Object>> defs = new ArrayList<>();
            for (Definition d : definitions.list()) {
                defs.add(Map.of("id", d.id(), "defKey", d.defKey(), "version", d.version(),
                        "adapterVersion", d.adapterVersion(), "digest", d.digest(),
                        "steps", d.steps().stream().map(s -> Map.of(
                                "id", s.id(), "name", s.name(), "dependsOn", s.dependsOn())).toList()));
            }
            writeJson(ex, 200, Map.of("definitions", defs));
            return;
        }
        if ("POST".equals(method) && parts[0].equals("definitions") && parts.length == 1) {
            Map<String, Object> body = Json.readMap(ex.getRequestBody());
            String key = (String) body.get("defKey");
            String adapterVersion = (String) body.getOrDefault("adapterVersion", "deterministic-adapter/dev");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> steps = (List<Map<String, Object>>) body.get("steps");
            Definition def = definitions.publish(key, adapterVersion, steps);
            writeJson(ex, 201, Map.of("id", def.id(), "defKey", def.defKey(), "version", def.version(),
                    "adapterVersion", def.adapterVersion(), "digest", def.digest()));
            return;
        }
        if ("POST".equals(method) && parts[0].equals("instances") && parts.length == 1) {
            Map<String, Object> body = Json.readMap(ex.getRequestBody());
            long defId = ((Number) body.get("defId")).longValue();
            String idemKey = (String) body.get("idemKey");
            @SuppressWarnings("unchecked")
            Map<String, Object> input = (Map<String, Object>) body.getOrDefault("input", Map.of());
            long id = engine.start(defId, idemKey, input);
            writeJson(ex, 201, Map.of("instanceId", id, "idempotentReplay", false));
            return;
        }
        if ("GET".equals(method) && parts[0].equals("instances") && parts.length == 1) {
            List<Map<String, Object>> rows;
            try (Store.Tx conn = store.open()) {
                rows = store.query(conn,
                        "SELECT id, def_id, status, idem_key, parent_instance_id, lease_owner, "
                                + "lease_generation, comp_cursor, created_at_ms FROM instances ORDER BY id");
            }
            writeJson(ex, 200, Map.of("instances", rows));
            return;
        }
        if ("GET".equals(method) && parts[0].equals("instances") && parts.length == 2) {
            writeJson(ex, 200, engine.instanceView(Long.parseLong(parts[1])));
            return;
        }
        if ("POST".equals(method) && parts.length == 3 && parts[0].equals("instances")
                && parts[2].equals("tick")) {
            Map<String, Object> body = Json.readMap(ex.getRequestBody());
            String worker = (String) body.getOrDefault("worker", "manual-worker");
            writeJson(ex, 200, toMap(engine.tick(Long.parseLong(parts[1]), worker)));
            return;
        }
        if (parts.length == 4 && parts[0].equals("instances") && parts[2].equals("confirm")) {
            Map<String, Object> body = Json.readMap(ex.getRequestBody());
            long instanceId = Long.parseLong(parts[1]);
            String stepId = parts[3];
            String verdict = (String) body.getOrDefault("verdict", "SUCCESS");
            String operator = (String) body.getOrDefault("operator", "operator");
            int expectedVersion = ((Number) body.getOrDefault("expectedVersion", 0)).intValue();
            writeJson(ex, 200, engine.manualConfirm(instanceId, stepId, verdict, operator, expectedVersion));
            return;
        }
        if ("POST".equals(method) && parts.length == 3 && parts[0].equals("instances")
                && parts[2].equals("resume-compensation")) {
            Map<String, Object> body = Json.readMap(ex.getRequestBody());
            String worker = (String) body.getOrDefault("worker", "manual-worker");
            writeJson(ex, 200, toMap(engine.resumeCompensation(Long.parseLong(parts[1]), worker)));
            return;
        }
        if ("POST".equals(method) && parts.length == 3 && parts[0].equals("instances")
                && parts[2].equals("derive")) {
            Map<String, Object> body = Json.readMap(ex.getRequestBody());
            String idemKey = (String) body.get("idemKey");
            @SuppressWarnings("unchecked")
            Map<String, Object> input = (Map<String, Object>) body.get("input");
            long child = engine.deriveRetry(Long.parseLong(parts[1]), idemKey, input);
            writeJson(ex, 201, Map.of("instanceId", child, "parentInstanceId", Long.parseLong(parts[1])));
            return;
        }
        if ("POST".equals(method) && parts[0].equals("demo") && parts.length == 2 && parts[1].equals("reset")) {
            writeJson(ex, 200, Map.of("reset", true));
            return;
        }
        writeJson(ex, 404, Map.of("error", "not_found", "path", path));
    }

    private Map<String, Object> toMap(Object value) {
        @SuppressWarnings("unchecked")
        Map<String, Object> m = Json.MAPPER.convertValue(value, Map.class);
        return new LinkedHashMap<>(m);
    }

    private void writeJson(HttpExchange ex, int status, Object body) throws IOException {
        byte[] bytes = Json.writeBytes(body);
        ex.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
