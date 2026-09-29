package compapp.engine;

public record Lease(String owner, int generation, long expiresAtMs) {
    public long fencingToken() {
        return generation;
    }
}
