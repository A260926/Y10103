package compapp.adapter;

/**
 * Possible outcomes of a simulated adapter call.
 * SUCCESS / FAILURE are explicit; TIMEOUT is an explicit failure signal;
 * UNKNOWN means the result is unknown and must be resolved via confirm.
 */
public enum Verdict {
    SUCCESS,
    FAILURE,
    TIMEOUT,
    UNKNOWN;

    public static Verdict parse(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("missing verdict");
        }
        return switch (raw.trim().toUpperCase()) {
            case "SUCCESS", "OK", "SUCCEEDED" -> SUCCESS;
            case "FAILURE", "FAILED", "FAIL" -> FAILURE;
            case "TIMEOUT", "TIMED_OUT" -> TIMEOUT;
            case "UNKNOWN", "INDETERMINATE" -> UNKNOWN;
            default -> throw new IllegalArgumentException("unsupported verdict: " + raw);
        };
    }
}
