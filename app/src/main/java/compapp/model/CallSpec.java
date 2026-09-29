package compapp.model;

import java.util.List;
import java.util.Map;

/**
 * Simulated behaviour of one adapter action (execute / confirm / compensate).
 * verdicts are consumed in order on each attempt, the last one repeats.
 */
public record CallSpec(List<String> verdicts, long delayMs, Map<String, Object> result) {
    public CallSpec {
        if (verdicts == null || verdicts.isEmpty()) {
            verdicts = List.of("SUCCESS");
        }
        verdicts = List.copyOf(verdicts);
        result = result == null ? Map.of() : Map.copyOf(result);
    }

    public String verdictAt(int attempt) {
        int idx = Math.min(attempt, verdicts.size() - 1);
        return verdicts.get(idx);
    }
}
