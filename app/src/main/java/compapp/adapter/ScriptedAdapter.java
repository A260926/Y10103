package compapp.adapter;

import compapp.model.Clock;
import compapp.model.Json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deterministic adapter driven by per-step scripts embedded in the definition.
 * Delay uses the injected clock only to mark timing; the call still returns
 * synchronously (delayMs is also exposed for workers that want real waiting).
 */
public class ScriptedAdapter implements Adapter {

    public record StepScript(List<Action> execute, List<Action> confirm, List<Action> compensate) {
    }

    public record Action(String verdict, long delayMs, Map<String, Object> result, String error) {
        Action at(int attempt, List<Action> actions) {
            int idx = Math.min(attempt, actions.size() - 1);
            return actions.get(idx);
        }
    }

    private final String version;
    private final Map<String, StepScript> scripts;
    private final Clock clock;

    public ScriptedAdapter(String version, Map<String, StepScript> scripts, Clock clock) {
        this.version = version;
        this.scripts = scripts;
        this.clock = clock;
    }

    @Override
    public String version() {
        return version;
    }

    @Override
    public AdapterResponse execute(String stepId, int attempt, Map<String, Object> params,
                                   Map<String, Object> input, long fencingToken) {
        return call(stepId, attempt, scripts.get(stepId) == null ? null : scripts.get(stepId).execute(), "execute");
    }

    @Override
    public AdapterResponse confirm(String stepId, int attempt, String externalRef,
                                   Map<String, Object> params, long fencingToken) {
        return call(stepId, attempt, scripts.get(stepId) == null ? null : scripts.get(stepId).confirm(), "confirm");
    }

    @Override
    public AdapterResponse compensate(String stepId, int attempt, String externalRef,
                                      Map<String, Object> params, long fencingToken) {
        return call(stepId, attempt, scripts.get(stepId) == null ? null : scripts.get(stepId).compensate(), "compensate");
    }

    private AdapterResponse call(String stepId, int attempt, List<Action> actions, String phase) {
        List<Action> effective = actions == null || actions.isEmpty()
                ? List.of(new Action("SUCCESS", 0, Map.of(), null))
                : actions;
        Action action = effective.get(Math.min(attempt, effective.size() - 1));
        long ts = clock.nowMs();
        if (action.delayMs() > 0) {
            // Deterministic "sleep": we only note the scheduled time; real delay is the
            // worker's responsibility. Clock injection keeps tests exact.
            ts = clock.nowMs() + action.delayMs();
        }
        Verdict verdict = Verdict.parse(action.verdict());
        Map<String, Object> payload = new HashMap<>(action.result() == null ? Map.of() : action.result());
        payload.put("_at_ms", ts);
        payload.put("_phase", phase);
        payload.put("_attempt", attempt);
        String ref = verdict == Verdict.UNKNOWN
                ? "ref-" + stepId + "-unknown-" + attempt
                : "ref-" + stepId + "-" + attempt;
        if (verdict == Verdict.FAILURE || verdict == Verdict.TIMEOUT) {
            return new AdapterResponse(verdict, null, payload,
                    action.error() == null ? phase + " " + verdict.name().toLowerCase() : action.error());
        }
        return new AdapterResponse(verdict, ref, payload, null);
    }

    public static StepScript scriptOf(Map<String, Object> stepJson) {
        return new StepScript(
                actions(stepJson.get("execute")),
                actions(stepJson.get("confirm")),
                actions(stepJson.get("compensate")));
    }

    @SuppressWarnings("unchecked")
    private static List<Action> actions(Object node) {
        if (node == null) {
            return List.of(new Action("SUCCESS", 0, Map.of(), null));
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
        List<Action> actions = new ArrayList<>();
        for (Map<String, Object> m : raw) {
            Object verdict = m.getOrDefault("verdict", "SUCCESS");
            long delay = m.get("delayMs") == null ? 0L : ((Number) m.get("delayMs")).longValue();
            Map<String, Object> result = (Map<String, Object>) m.getOrDefault("result", Map.of());
            actions.add(new Action((String) verdict, delay, result == null ? Map.of() : result, (String) m.get("error")));
        }
        return actions;
    }

    public static String describe() {
        return Json.write(Map.of("verdicts", List.of("SUCCESS", "FAILURE", "TIMEOUT", "UNKNOWN")));
    }
}
