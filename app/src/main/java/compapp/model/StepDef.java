package compapp.model;

import java.util.List;
import java.util.Map;

public record StepDef(
        String id,
        String name,
        List<String> dependsOn,
        Map<String, Object> params,
        CallSpec execute,
        CallSpec confirm,
        CallSpec compensate) {
    public StepDef {
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
        params = params == null ? Map.of() : Map.copyOf(params);
        if (execute == null) throw new IllegalArgumentException("step " + id + " missing execute spec");
        if (confirm == null) throw new IllegalArgumentException("step " + id + " missing confirm spec");
        if (compensate == null) throw new IllegalArgumentException("step " + id + " missing compensate spec");
    }
}
