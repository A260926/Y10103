package compapp;

import compapp.db.Store;
import compapp.engine.DefinitionService;
import compapp.engine.Engine;
import compapp.engine.LeaseManager;
import compapp.engine.Presets;
import compapp.model.Clock;
import compapp.web.ApiServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {
        int port = 5233;
        String dbPath = "compapp-demo.db";
        boolean seed = true;
        boolean autoWorker = true;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--db" -> dbPath = args[++i];
                case "--no-seed" -> seed = false;
                case "--no-worker" -> autoWorker = false;
                default -> {
                }
            }
        }
        Clock clock = Clock.SYSTEM;
        Store store = new Store(Path.of(dbPath));
        store.migrate();
        DefinitionService definitions = new DefinitionService(store, clock);
        LeaseManager leases = new LeaseManager(store, clock, LeaseManager.DEFAULT_TTL_MS);
        Engine engine = new Engine(store, definitions, leases, clock);

        if (seed && isFresh(store)) {
            seed(store, definitions, engine);
        }

        ApiServer api = new ApiServer(port, store, definitions, engine, leases, clock, autoWorker);
        api.start();
        System.out.println("补偿流程演练场 已启动: http://localhost:" + api.boundPort());
        System.out.println("API: http://localhost:" + api.boundPort() + "/api/instances");
        Thread.currentThread().join();
    }

    private static boolean isFresh(Store store) {
        try (var conn = store.open()) {
            var rows = store.query(conn, "SELECT COUNT(*) AS c FROM definitions");
            return ((Number) rows.get(0).get("c")).intValue() == 0;
        }
    }

    private static void seed(Store store, DefinitionService definitions, Engine engine) {
        var happy = definitions.publish("happy-order", Presets.ADAPTER_VERSION, Presets.happyFlow());
        var trip = definitions.publish("trip-compensation", Presets.ADAPTER_VERSION, Presets.tripFlow());
        engine.start(happy.id(), "demo-happy-1", Map.of(
                "orderNo", "PO-2026-001", "customer", "alice"));
        engine.start(trip.id(), "demo-trip-1", Map.of(
                "flightNo", "CA1831", "hotel", "Seaside-Inn", "deposit", "1200"));
        System.out.println("预置流程已发布: happy-order#" + happy.version()
                + ", trip-compensation#" + trip.version());
    }
}
