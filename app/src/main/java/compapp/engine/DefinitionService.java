package compapp.engine;

import compapp.adapter.ScriptedAdapter;
import compapp.db.Store;

import compapp.model.CallSpec;
import compapp.model.Clock;
import compapp.model.Definition;
import compapp.model.Json;
import compapp.model.StepDef;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Validates and publishes definitions. On publish the step graph, params and
 * adapter version are frozen into an immutable row with a content digest.
 */
public class DefinitionService {

    private final Store store;
    private final Clock clock;

    public DefinitionService(Store store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    public Definition publish(String defKey, String adapterVersion, List<Map<String, Object>> stepsJson) {
        List<StepDef> steps = parse(stepsJson);
        validateDag(steps);
        String digest = digest(defKey, adapterVersion, stepsJson);
        try (Store.Tx conn = store.open()) {
            Map<String, Object> latest = store.queryOne(conn,
                    "SELECT COALESCE(MAX(version), 0) AS v FROM definitions WHERE def_key = ?", defKey);
            int version = ((Number) latest.get("v")).intValue() + 1;
            long now = clock.nowMs();
            long id = store.insert(conn,
                    "INSERT INTO definitions(def_key, version, adapter_version, steps_json, digest, created_at_ms) "
                            + "VALUES(?,?,?,?,?,?)",
                    defKey, version, adapterVersion, Json.write(stepsJson), digest, now);
            return new Definition(id, defKey, version, adapterVersion, steps, now, digest);
        }
    }

    public Definition getById(long defId) {
        try (Store.Tx conn = store.open()) {
            Map<String, Object> row = store.queryOne(conn, "SELECT * FROM definitions WHERE id = ?", defId);
            if (row == null) {
                throw new IllegalArgumentException("definition not found: " + defId);
            }
            return fromRow(row);
        }
    }

    public Definition getByKey(String defKey) {
        try (Store.Tx conn = store.open()) {
            Map<String, Object> row = store.queryOne(conn,
                    "SELECT * FROM definitions WHERE def_key = ? ORDER BY version DESC LIMIT 1", defKey);
            if (row == null) {
                throw new IllegalArgumentException("definition not found: " + defKey);
            }
            return fromRow(row);
        }
    }

    public List<Definition> list() {
        try (Store.Tx conn = store.open()) {
            return store.query(conn, "SELECT * FROM definitions ORDER BY id").stream()
                    .map(DefinitionService::fromRow).toList();
        }
    }

    static Definition fromRow(Map<String, Object> row) {
        List<Map<String, Object>> stepsRaw = parseJsonList((String) row.get("steps_json"));
        List<StepDef> steps = parse(stepsRaw);
        return new Definition(
                ((Number) row.get("id")).longValue(),
                (String) row.get("def_key"),
                ((Number) row.get("version")).intValue(),
                (String) row.get("adapter_version"),
                steps,
                ((Number) row.get("created_at_ms")).longValue(),
                (String) row.get("digest"));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> parseJsonList(String json) {
        return (List<Map<String, Object>>) (List<?>) Json.readMap("{\"x\":" + json + "}").get("x");
    }

    public static ScriptedAdapter buildAdapter(Definition def, Clock clock) {
        Map<String, ScriptedAdapter.StepScript> scripts = new LinkedHashMap<>();
        for (StepDef step : def.steps()) {
            scripts.put(step.id(), new ScriptedAdapter.StepScript(
                    toActions(step.execute()), toActions(step.confirm()), toActions(step.compensate())));
        }
        return new ScriptedAdapter(def.adapterVersion(), scripts, clock);
    }

    private static List<ScriptedAdapter.Action> toActions(CallSpec spec) {
        List<ScriptedAdapter.Action> out = new ArrayList<>();
        for (String verdict : spec.verdicts()) {
            out.add(new ScriptedAdapter.Action(verdict, spec.delayMs(), spec.result(), null));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    public static List<StepDef> parse(List<Map<String, Object>> raw) {
        List<StepDef> steps = new ArrayList<>();
        for (Map<String, Object> m : raw) {
            String id = (String) m.get("id");
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("step without id");
            }
            List<String> deps = (List<String>) m.getOrDefault("dependsOn", List.of());
            Map<String, Object> params = (Map<String, Object>) m.getOrDefault("params", Map.of());
            steps.add(new StepDef(id,
                    (String) m.getOrDefault("name", id),
                    deps == null ? List.of() : List.copyOf(deps),
                    params == null ? Map.of() : Map.copyOf(params),
                    callSpec(m.get("execute"), "SUCCESS"),
                    callSpec(m.get("confirm"), "SUCCESS"),
                    callSpec(m.get("compensate"), "SUCCESS")));
        }
        return steps;
    }

    @SuppressWarnings("unchecked")
    private static CallSpec callSpec(Object node, String defaultVerdict) {
        if (node == null) {
            return new CallSpec(List.of(defaultVerdict), 0, Map.of());
        }
        if (node instanceof String s) {
            return new CallSpec(List.of(s), 0, Map.of());
        }
        List<Map<String, Object>> raw;
        if (node instanceof List<?> list) {
            raw = new ArrayList<>();
            for (Object item : list) {
                raw.add((Map<String, Object>) item);
            }
        } else {
            raw = List.of((Map<String, Object>) node);
        }
        List<String> verdicts = new ArrayList<>();
        long delay = 0;
        Map<String, Object> result = Map.of();
        for (Map<String, Object> m : raw) {
            verdicts.add((String) m.getOrDefault("verdict", defaultVerdict));
            if (m.get("delayMs") != null) {
                delay = ((Number) m.get("delayMs")).longValue();
            }
            if (m.get("result") instanceof Map<?, ?> r) {
                result = (Map<String, Object>) r;
            }
        }
        return new CallSpec(verdicts, delay, result);
    }

    static void validateDag(List<StepDef> steps) {
        Set<String> ids = new LinkedHashSet<>();
        Map<String, List<String>> deps = new LinkedHashMap<>();
        for (StepDef s : steps) {
            if (!ids.add(s.id())) {
                throw new IllegalArgumentException("duplicate step id: " + s.id());
            }
            deps.put(s.id(), s.dependsOn());
        }
        for (var e : deps.entrySet()) {
            for (String d : e.getValue()) {
                if (!ids.contains(d)) {
                    throw new IllegalArgumentException("step " + e.getKey() + " depends on missing step " + d);
                }
            }
        }
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, Set<String>> dependents = new HashMap<>();
        ids.forEach(id -> indegree.put(id, 0));
        deps.forEach((id, ds) -> {
            for (String d : ds) {
                indegree.merge(id, 1, Integer::sum);
                dependents.computeIfAbsent(d, k -> new HashSet<>()).add(id);
            }
        });
        Deque<String> queue = new ArrayDeque<>();
        indegree.forEach((id, deg) -> {
            if (deg == 0) {
                queue.add(id);
            }
        });
        int visited = 0;
        while (!queue.isEmpty()) {
            String id = queue.poll();
            visited++;
            for (String dependent : dependents.getOrDefault(id, Set.of())) {
                if (indegree.merge(dependent, -1, Integer::sum) == 0) {
                    queue.add(dependent);
                }
            }
        }
        if (visited != ids.size()) {
            throw new IllegalArgumentException("step graph contains a cycle");
        }
    }

    /**
     * Reverse topological order is the compensation order: later-finished steps
     * are undone first, respecting dependency edges.
     */
    public static List<String> reverseTopo(List<StepDef> steps) {
        List<String> topo = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Set<String> stack = new HashSet<>();
        Map<String, StepDef> byId = new LinkedHashMap<>();
        steps.forEach(s -> byId.put(s.id(), s));
        for (StepDef s : steps) {
            dfs(s.id(), byId, visited, stack, topo);
        }
        return topo.reversed();
    }

    private static void dfs(String id, Map<String, StepDef> byId, Set<String> visited,
                            Set<String> stack, List<String> topo) {
        if (visited.contains(id)) {
            return;
        }
        if (!stack.add(id)) {
            throw new IllegalArgumentException("cycle at " + id);
        }
        for (String dep : byId.get(id).dependsOn()) {
            dfs(dep, byId, visited, stack, topo);
        }
        stack.remove(id);
        visited.add(id);
        topo.add(id);
    }

    private static String digest(String defKey, String adapterVersion, List<Map<String, Object>> stepsJson) {
        Map<String, Object> canonical = new TreeMap<>();
        canonical.put("defKey", defKey);
        canonical.put("adapterVersion", adapterVersion);
        canonical.put("steps", stepsJson);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(Json.write(canonical).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.substring(16);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
