package compapp.adapter;

import java.util.Map;

public record AdapterResponse(Verdict verdict, String externalRef, Map<String, Object> payload, String error) {
    public AdapterResponse {
        payload = payload == null ? Map.of() : Map.copyOf(payload);
    }

    public static AdapterResponse success(String ref, Map<String, Object> payload) {
        return new AdapterResponse(Verdict.SUCCESS, ref, payload, null);
    }

    public static AdapterResponse failure(String error) {
        return new AdapterResponse(Verdict.FAILURE, null, Map.of(), error);
    }
}
