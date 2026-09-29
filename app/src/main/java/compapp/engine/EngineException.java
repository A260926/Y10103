package compapp.engine;

public class EngineException extends RuntimeException {
    private final String code;

    public EngineException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() {
        return code;
    }

    public static EngineException notFound(String message) {
        return new EngineException("not_found", message);
    }

    public static EngineException conflict(String message) {
        return new EngineException("conflict", message);
    }

    public static EngineException staleLease(String message) {
        return new EngineException("stale_lease", message);
    }

    public static EngineException versionConflict(String message) {
        return new EngineException("version_conflict", message);
    }

    public static EngineException invalid(String message) {
        return new EngineException("invalid", message);
    }
}
