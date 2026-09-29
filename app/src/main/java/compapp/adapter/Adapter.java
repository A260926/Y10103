package compapp.adapter;

import java.util.Map;

/**
 * Local deterministic adapter. No external service is ever contacted.
 * An intentRecord carries the persisted intent plus the fencing token so the
 * adapter call can be correlated with its durable intent.
 */
public interface Adapter {

    String version();

    AdapterResponse execute(String stepId, int attempt, Map<String, Object> params,
                            Map<String, Object> input, long fencingToken);

    AdapterResponse confirm(String stepId, int attempt, String externalRef,
                            Map<String, Object> params, long fencingToken);

    AdapterResponse compensate(String stepId, int attempt, String externalRef,
                               Map<String, Object> params, long fencingToken);
}
