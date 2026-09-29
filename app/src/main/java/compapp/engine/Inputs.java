package compapp.engine;

import compapp.model.Json;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves step parameter templates against the workflow input and confirmed
 * predecessor results, and computes stable input digests so derived retries can
 * prove a previously confirmed step still sees the same inputs.
 */
public final class Inputs {

    private static final Pattern TOKEN = Pattern.compile("\\$\\{([^}]+)}");

    private Inputs() {
    }

    public static Map<String, Object> resolveParams(Map<String, Object> template,
                                                    Map<String, Object> input,
                                                    Map<String, Map<String, Object>> predecessorResults) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (var e : template.entrySet()) {
            out.put(e.getKey(), resolveValue(e.getValue(), input, predecessorResults));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static Object resolveValue(Object value, Map<String, Object> input,
                                       Map<String, Map<String, Object>> predecessorResults) {
        if (value instanceof String s) {
            return resolveString(s, input, predecessorResults);
        }
        if (value instanceof Map<?, ?> m) {
            Map<String, Object> copy = new LinkedHashMap<>();
            m.forEach((k, v) -> copy.put((String) k, resolveValue(v, input, predecessorResults)));
            return copy;
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>();
            for (Object item : list) {
                copy.add(resolveValue(item, input, predecessorResults));
            }
            return copy;
        }
        return value;
    }

    private static Object resolveString(String raw, Map<String, Object> input,
                                        Map<String, Map<String, Object>> predecessorResults) {
        Matcher matcher = TOKEN.matcher(raw);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            Object value = lookup(matcher.group(1).trim(), input, predecessorResults);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(String.valueOf(value)));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static Object lookup(String path, Map<String, Object> input,
                                 Map<String, Map<String, Object>> predecessorResults) {
        String[] parts = path.split("\\.");
        Object current;
        if (parts[0].equals("input")) {
            current = navigate(input, parts, 1);
        } else if (parts[0].equals("steps")) {
            if (parts.length < 3) {
                throw EngineException.invalid("template reference must be steps.<id>.result.<field>");
            }
            Map<String, Object> result = predecessorResults.get(parts[1]);
            if (result == null) {
                throw EngineException.invalid("predecessor result not available for " + parts[1]);
            }
            Map<String, Object> scope = new LinkedHashMap<>(result);
            current = navigate(scope, parts, 2);
        } else {
            current = navigate(input, parts, 0);
        }
        return current == null ? "" : current;
    }

    private static Object navigate(Map<String, Object> root, String[] parts, int start) {
        Object current = root;
        for (int i = start; i < parts.length; i++) {
            if (!(current instanceof Map<?, ?> m)) {
                return null;
            }
            current = m.get(parts[i]);
        }
        return current;
    }

    public static String digest(String stepId, Map<String, Object> params) {
        Map<String, Object> canonical = new TreeMap<>();
        canonical.put("stepId", stepId);
        canonical.put("params", deepSorted(params));
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(Json.write(canonical).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.substring(0, 16);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Object deepSorted(Object value) {
        if (value instanceof Map<?, ?> m) {
            Map<String, Object> sorted = new TreeMap<>();
            m.forEach((k, v) -> sorted.put(String.valueOf(k), deepSorted(v)));
            return sorted;
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>();
            for (Object item : list) {
                copy.add(deepSorted(item));
            }
            return copy;
        }
        return value;
    }
}
