package compapp.engine;

import compapp.db.Store;
import compapp.model.Clock;
import compapp.model.Definition;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class TestHarness implements AutoCloseable {

    final AtomicLong time = new AtomicLong(1_000_000L);
    final Clock clock = time::get;
    final Store store;
    final DefinitionService definitions;
    final LeaseManager leases;
    final Engine engine;

    public TestHarness() {
        try {
            Path db = java.nio.file.Files.createTempFile("compapp-test-", ".db");
            db.toFile().deleteOnExit();
            this.store = new Store(db);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        store.migrate();
        this.definitions = new DefinitionService(store, clock);
        this.leases = new LeaseManager(store, clock, 60_000L);
        this.engine = new Engine(store, definitions, leases, clock);
    }

    public void advance(long ms) {
        time.addAndGet(ms);
    }

    public Definition publish(String key, List<Map<String, Object>> steps) {
        return definitions.publish(key, "test-adapter/1.0", steps);
    }

    public Map<String, Object> view(long instanceId) {
        return engine.instanceView(instanceId);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> step(long instanceId, String stepId) {
        return ((List<Map<String, Object>>) view(instanceId).get("steps")).stream()
                .filter(s -> stepId.equals(s.get("step_id"))).findFirst().orElseThrow();
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> events(long instanceId) {
        return (List<Map<String, Object>>) view(instanceId).get("events");
    }

    @Override
    public void close() {
    }
}
