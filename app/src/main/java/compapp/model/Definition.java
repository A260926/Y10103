package compapp.model;

import java.util.List;

/**
 * Published, immutable definition: graph, params and adapter version are frozen.
 */
public record Definition(
        long id,
        String defKey,
        int version,
        String adapterVersion,
        List<StepDef> steps,
        long createdAtMs,
        String digest) {
    public Definition {
        steps = List.copyOf(steps);
    }

    public StepDef step(String stepId) {
        return steps.stream().filter(s -> s.id().equals(stepId)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown step " + stepId));
    }
}
