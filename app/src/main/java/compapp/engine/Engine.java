package compapp.engine;

import compapp.adapter.Adapter;
import compapp.adapter.AdapterResponse;
import compapp.adapter.Verdict;
import compapp.db.Store;

import compapp.model.Clock;
import compapp.model.Definition;
import compapp.model.InstanceStatus;
import compapp.model.Json;
import compapp.model.StepDef;
import compapp.model.StepStatus;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Saga engine. Durable protocol per step execution:
 *   1. write intent (with fencing token)
 *   2. call local deterministic adapter (outside the DB transaction)
 *   3. write external receipt (rejected when the fence is stale)
 *   4. advance step state
 * Unknown results are only resolved via confirm, never by blind re-execution.
 */
public class Engine {

    private final Store store;
    private final DefinitionService definitions;
    private final LeaseManager leases;
    private final Clock clock;

    public Engine(Store store, DefinitionService definitions, LeaseManager leases, Clock clock) {
        this.store = store;
        this.definitions = definitions;
        this.leases = leases;
        this.clock = clock;
    }

    public LeaseManager leases() {
        return leases;
    }

    public record InstanceRow(long id, long defId, String inputJson, InstanceStatus status,
                              int leaseGeneration, String leaseOwner, long leaseExpiresAt,
                              String compOrderJson, int compCursor, int rowVersion, Long parentInstanceId) {
    }

    InstanceRow loadInstance(long instanceId) {
        try (Store.Tx conn = store.open()) {
            Map<String, Object> row = store.queryOne(conn, "SELECT * FROM instances WHERE id = ?", instanceId);
            if (row == null) {
                throw EngineException.notFound("instance " + instanceId);
            }
            return toInstanceRow(row);
        }
    }

    static InstanceRow toInstanceRow(Map<String, Object> row) {
        return new InstanceRow(
                ((Number) row.get("id")).longValue(),
                ((Number) row.get("def_id")).longValue(),
                (String) row.get("input_json"),
                InstanceStatus.valueOf((String) row.get("status")),
                ((Number) row.get("lease_generation")).intValue(),
                (String) row.get("lease_owner"),
                row.get("lease_expires_at") == null ? 0L : ((Number) row.get("lease_expires_at")).longValue(),
                (String) row.get("comp_order_json"),
                ((Number) row.get("comp_cursor")).intValue(),
                ((Number) row.get("row_version")).intValue(),
                row.get("parent_instance_id") == null ? null : ((Number) row.get("parent_instance_id")).longValue());
    }

    // ------------------------------------------------------------------ start

    /**
     * Start with an idempotency key. Same key + same content returns the original
     * instance; same key + different content is a 409 conflict.
     */
    public long start(long defId, String idemKey, Map<String, Object> input) {
        Definition def = definitions.getById(defId);
        String inputHash = Inputs.digest("__input__", input == null ? Map.of() : input);
        try (Store.Tx conn = store.open()) {
            if (idemKey != null && !idemKey.isBlank()) {
                Map<String, Object> existing = store.queryOne(conn,
                        "SELECT id, input_hash FROM instances WHERE idem_key = ?", idemKey);
                if (existing != null) {
                    if (!inputHash.equals(existing.get("input_hash"))) {
                        throw EngineException.conflict(
                                "idempotency key " + idemKey + " reused with different content");
                    }
                    return ((Number) existing.get("id")).longValue();
                }
            }
            long now = clock.nowMs();
            long instanceId = store.insert(conn,
                    "INSERT INTO instances(def_id, idem_key, input_hash, input_json, status, "
                            + "lease_generation, comp_cursor, row_version, created_at_ms, updated_at_ms) "
                            + "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    defId, blankToNull(idemKey), inputHash, Json.write(input == null ? Map.of() : input),
                    InstanceStatus.RUNNING.name(), 0, 0, 1, now, now);
            for (StepDef step : def.steps()) {
                store.insert(conn,
                        "INSERT INTO step_runs(instance_id, step_id, status, attempt) VALUES(?,?,?,?)",
                        instanceId, step.id(), StepStatus.PENDING.name(), 0);
            }
            event(conn, instanceId, null, "instance_started",
                    Json.write(Map.of("defId", defId, "idemKey", idemKey == null ? "" : idemKey)));
            return instanceId;
        }
    }

    // ------------------------------------------------------------------- tick

    public TickReport tick(long instanceId, String worker) {
        return tick(instanceId, worker, null, true);
    }

    /**
     * One deterministic worker pass with a fencing lease. A stale fence aborts
     * the pass without mutating state.
     */
    public TickReport tick(long instanceId, String worker, Lease suppliedLease, boolean releaseAtEnd) {
        List<String> events = new ArrayList<>();
        boolean[] progress = {false};
        InstanceRow inst = loadInstance(instanceId);
        if (inst.status == InstanceStatus.COMPLETED
                || inst.status == InstanceStatus.COMPENSATED
                || inst.status == InstanceStatus.LOCKED) {
            return new TickReport(instanceId, inst.status.name(), events, false);
        }
        Lease lease = suppliedLease != null ? suppliedLease : leases.acquireOrRenew(instanceId, worker);
        if (!leases.isValid(instanceId, lease)) {
            throw EngineException.staleLease("lease " + lease.generation() + " is no longer valid");
        }
        // Re-read after taking the lease: another caller (e.g. manual confirm) may
        // have moved the instance between our first read and lease acquisition.
        inst = loadInstance(instanceId);
        Definition def = definitions.getById(inst.defId);
        Adapter adapter = DefinitionService.buildAdapter(def, clock);

        if (inst.status == InstanceStatus.FAILED_COMPENSATION || inst.status == InstanceStatus.COMPENSATING) {
            compensationPass(instanceId, lease, def, adapter, events, progress);
        } else {
            runningPass(instanceId, lease, def, adapter, inst, events, progress);
            // A failure detected mid-tick starts compensation in the same worker pass:
            // already-successful branches are undone without waiting for the next tick.
            InstanceRow mid = loadInstance(instanceId);
            if (mid.status == InstanceStatus.COMPENSATING
                    || mid.status == InstanceStatus.FAILED_COMPENSATION) {
                compensationPass(instanceId, lease, def, adapter, events, progress);
            }
        }
        if (releaseAtEnd) {
            leases.release(instanceId, lease);
        }
        InstanceRow after = loadInstance(instanceId);
        return new TickReport(instanceId, after.status.name(), events, progress[0]);
    }

    private void runningPass(long instanceId, Lease lease, Definition def, Adapter adapter,
                             InstanceRow inst, List<String> events, boolean[] progress) {
        recoverDoubtfulSteps(instanceId, lease, events, progress);

        List<Map<String, Object>> steps = snapshotSteps(instanceId);
        boolean failed = steps.stream().anyMatch(s -> StepStatus.FAILED.name().equals(s.get("status")));
        if (failed) {
            beginCompensation(instanceId, def, lease, events, progress);
            return;
        }

        Map<String, Map<String, Object>> confirmedResults = confirmedResults(steps);
        Map<String, Object> input = Json.readMap(inst.inputJson);

        List<String> ready = new ArrayList<>();
        for (StepDef step : def.steps()) {
            String state = stateOf(steps, step.id());
            if (StepStatus.PENDING.name().equals(state) && depsConfirmed(step, steps)) {
                ready.add(step.id());
            }
        }
        if (ready.isEmpty()) {
            if (steps.stream().allMatch(sx -> StepStatus.CONFIRMED.name().equals(sx.get("status")))) {
                try (Store.Tx conn = store.open()) {
                    setInstanceStatusIn(conn, instanceId, InstanceStatus.COMPLETED, lease);
                    event(conn, instanceId, null, "instance_completed", null);
                }
                events.add("instance_completed");
                progress[0] = true;
                return;
            }
            if (steps.stream().anyMatch(sx -> StepStatus.AWAIT_CONFIRM.name().equals(sx.get("status")))) {
                setInstanceStatus(instanceId, InstanceStatus.AWAIT_MANUAL, lease);
            }
            return;
        }

        for (String stepId : ready) {
            StepDef step = def.step(stepId);
            Map<String, Object> params = Inputs.resolveParams(step.params(), input, confirmedResults);
            executeStep(instanceId, step, params, lease, adapter, events);
            progress[0] = true;
        }

        List<Map<String, Object>> afterSteps = snapshotSteps(instanceId);
        boolean anyFailure = afterSteps.stream()
                .anyMatch(s -> StepStatus.FAILED.name().equals(s.get("status")));
        if (anyFailure) {
            beginCompensation(instanceId, def, lease, events, progress);
            return;
        }
        boolean allConfirmed = afterSteps.stream()
                .allMatch(s -> StepStatus.CONFIRMED.name().equals(s.get("status")));
        if (allConfirmed) {
            try (Store.Tx conn = store.open()) {
                setInstanceStatusIn(conn, instanceId, InstanceStatus.COMPLETED, lease);
                event(conn, instanceId, null, "instance_completed", null);
            }
            events.add("instance_completed");
            return;
        }
        if (afterSteps.stream().anyMatch(s -> StepStatus.AWAIT_CONFIRM.name().equals(s.get("status")))) {
            setInstanceStatus(instanceId, InstanceStatus.AWAIT_MANUAL, lease);
        }
    }

    private Map<String, Map<String, Object>> confirmedResults(List<Map<String, Object>> steps) {
        Map<String, Map<String, Object>> out = new LinkedHashMap<>();
        for (Map<String, Object> s : steps) {
            if (StepStatus.CONFIRMED.name().equals(s.get("status"))) {
                out.put((String) s.get("step_id"), Json.readMap((String) s.get("result_json")));
            }
        }
        return out;
    }

    private boolean depsConfirmed(StepDef step, List<Map<String, Object>> steps) {
        for (String dep : step.dependsOn()) {
            if (!StepStatus.CONFIRMED.name().equals(stateOf(steps, dep))) {
                return false;
            }
        }
        return true;
    }

    List<Map<String, Object>> snapshotSteps(long instanceId) {
        try (Store.Tx conn = store.open()) {
            return store.query(conn,
                    "SELECT * FROM step_runs WHERE instance_id = ? ORDER BY id", instanceId);
        }
    }

    private String stateOf(List<Map<String, Object>> steps, String stepId) {
        return steps.stream().filter(s -> stepId.equals(s.get("step_id"))).findFirst()
                .map(s -> (String) s.get("status")).orElse(null);
    }

    private String currentStepState(long stepRunId) {
        try (Store.Tx conn = store.open()) {
            Map<String, Object> row = store.queryOne(conn,
                    "SELECT status FROM step_runs WHERE id = ?", stepRunId);
            return row == null ? null : (String) row.get("status");
        }
    }

    void setInstanceStatus(long instanceId, InstanceStatus status, Lease lease) {
        try (Store.Tx conn = store.open()) {
            setInstanceStatusIn(conn, instanceId, status, lease);
        }
    }

    void setInstanceStatusIn(Store.Tx conn, long instanceId, InstanceStatus status, Lease lease) {
        int updated = store.update(conn,
                "UPDATE instances SET status = ?, lease_generation = ?, updated_at_ms = ? WHERE id = ?",
                status.name(), lease.generation(), clock.nowMs(), instanceId);
        if (updated == 0) {
            throw EngineException.staleLease("instance vanished");
        }
    }

    void event(Store.Tx conn, long instanceId, String stepId, String kind, String detail) {
        store.insert(conn,
                "INSERT INTO events(instance_id, step_id, kind, detail, created_at_ms) VALUES(?,?,?,?,?)",
                instanceId, stepId, kind, detail, clock.nowMs());
    }

    // ------------------------------------------------------------- execution

    private enum Outcome { CONFIRMED, UNKNOWN, FAILED }

    /**
     * Durable execution of one step. Intent and receipt are separate rows written
     * in separate transactions; the adapter call happens between them.
     */
    private Outcome executeStep(long instanceId, StepDef step, Map<String, Object> params,
                                Lease lease, Adapter adapter, List<String> events) {
        long stepRunId = stepRunId(instanceId, step.id());
        String inputDigest = Inputs.digest(step.id(), params);
        int attempt;
        try (Store.Tx conn = store.open()) {
            Map<String, Object> row = store.queryOne(conn,
                    "SELECT attempt, status FROM step_runs WHERE id = ?", stepRunId);
            attempt = ((Number) row.get("attempt")).intValue() + 1;
            // 1) intent first, stamped with the fencing token.
            store.update(conn,
                    "UPDATE step_runs SET status = ?, attempt = ?, input_digest = ?, "
                            + "executed_generation = ?, step_version = step_version + 1 WHERE id = ?",
                    StepStatus.INTENT_WRITTEN.name(), attempt, inputDigest, lease.generation(), stepRunId);
            store.insert(conn,
                    "INSERT INTO intents(step_run_id, attempt, payload_json, fencing_token, created_at_ms) "
                            + "VALUES(?,?,?,?,?)",
                    stepRunId, attempt, Json.write(Map.of(
                            "stepId", step.id(), "params", params, "adapterVersion", adapter.version())),
                    lease.generation(), clock.nowMs());
            event(conn, instanceId, step.id(), "intent_written",
                    Json.write(Map.of("attempt", attempt, "fencingToken", lease.generation())));
        }
        events.add(step.id() + ":intent");

        // 2) local deterministic adapter, outside any DB transaction.
        Map<String, Object> input = Json.readMap(loadInstance(instanceId).inputJson);
        AdapterResponse response = adapter.execute(
                step.id(), attempt - 1, params, input, lease.fencingToken());

        // 3) receipt. A late receipt under a newer generation is recorded but rejected
        //    and cannot advance the step.
        boolean fenceCurrent = lease.generation() == loadInstance(instanceId).leaseGeneration();
        try (Store.Tx conn = store.open()) {
            store.insert(conn,
                    "INSERT INTO receipts(step_run_id, attempt, external_ref, verdict, payload_json, "
                            + "fencing_token, accepted, created_at_ms) VALUES(?,?,?,?,?,?,?,?)",
                    stepRunId, attempt, response.externalRef(), response.verdict().name(),
                    Json.write(response.payload()), lease.generation(), fenceCurrent ? 1 : 0, clock.nowMs());
            if (!fenceCurrent) {
                event(conn, instanceId, step.id(), "receipt_rejected_stale_fence",
                        Json.write(Map.of("receiptGeneration", lease.generation(),
                                "currentGeneration", loadInstance(instanceId).leaseGeneration())));
            }
        }
        if (!fenceCurrent) {
            throw EngineException.staleLease(
                    "late receipt from generation " + lease.generation() + " rejected");
        }
        events.add(step.id() + ":receipt:" + response.verdict());

        // 4) advance state from the receipt verdict.
        switch (response.verdict()) {
            case SUCCESS -> {
                setStepState(stepRunId, StepStatus.RECEIPT_WRITTEN, response, null);
                events.add(step.id() + ":receipt-written");
                return resolveByConfirm(instanceId, step, stepRunId, attempt, lease, adapter, events, false);
            }
            case UNKNOWN -> {
                setStepState(stepRunId, StepStatus.AWAIT_CONFIRM, response, null);
                try (Store.Tx conn = store.open()) {
                    event(conn, instanceId, step.id(), "await_confirm",
                            Json.write(Map.of("attempt", attempt, "reason", "UNKNOWN result")));
                }
                events.add(step.id() + ":unknown");
                return Outcome.UNKNOWN;
            }
            case FAILURE, TIMEOUT -> {
                setStepState(stepRunId, StepStatus.FAILED, response, response.error());
                try (Store.Tx conn = store.open()) {
                    event(conn, instanceId, step.id(), "execute_failed",
                            Json.write(Map.of("attempt", attempt, "verdict", response.verdict().name(),
                                    "error", String.valueOf(response.error()))));
                }
                events.add(step.id() + ":failed");
                return Outcome.FAILED;
            }
            default -> throw new IllegalStateException("unknown verdict");
        }
    }

    /**
     * Confirm is the only way to turn a receipt / unknown result into a final
     * decision. Never re-executes the business action.
     */
    private Outcome resolveByConfirm(long instanceId, StepDef step, long stepRunId, int attempt,
                                     Lease lease, Adapter adapter, List<String> events, boolean manual) {
        AdapterResponse response = adapter.confirm(step.id(), attempt - 1,
                lastExternalRef(stepRunId), Map.of(), lease.fencingToken());
        try (Store.Tx conn = store.open()) {
            store.insert(conn,
                    "INSERT INTO confirms(step_run_id, attempt, verdict, payload_json, manual, operator, "
                            + "fencing_token, created_at_ms) VALUES(?,?,?,?,?,?,?,?)",
                    stepRunId, attempt, response.verdict().name(), Json.write(response.payload()),
                    manual ? 1 : 0, "", lease.generation(), clock.nowMs());
        }
        boolean current = lease.generation() == loadInstance(instanceId).leaseGeneration();
        if (!current) {
            throw EngineException.staleLease("confirm from stale generation rejected");
        }
        switch (response.verdict()) {
            case SUCCESS -> {
                setStepState(stepRunId, StepStatus.CONFIRMED, response, null);
                try (Store.Tx conn = store.open()) {
                    event(conn, instanceId, step.id(), "confirmed",
                            Json.write(Map.of("attempt", attempt, "manual", manual)));
                }
                events.add(step.id() + ":confirmed");
                return Outcome.CONFIRMED;
            }
            case FAILURE, TIMEOUT -> {
                setStepState(stepRunId, StepStatus.FAILED, response, response.error());
                try (Store.Tx conn = store.open()) {
                    event(conn, instanceId, step.id(), "confirm_failed",
                            Json.write(Map.of("attempt", attempt, "verdict", response.verdict().name())));
                }
                events.add(step.id() + ":confirm-failed");
                return Outcome.FAILED;
            }
            case UNKNOWN -> {
                setStepState(stepRunId, StepStatus.AWAIT_CONFIRM, response, null);
                try (Store.Tx conn = store.open()) {
                    event(conn, instanceId, step.id(), "confirm_still_unknown",
                            Json.write(Map.of("attempt", attempt, "manual", manual)));
                }
                events.add(step.id() + ":still-unknown");
                return Outcome.UNKNOWN;
            }
            default -> throw new IllegalStateException();
        }
    }

    private void setStepState(long stepRunId, StepStatus state, AdapterResponse response, String error) {
        try (Store.Tx conn = store.open()) {
            String resultJson = response.payload() == null ? null : Json.write(response.payload());
            store.update(conn,
                    "UPDATE step_runs SET status = ?, result_json = COALESCE(?, result_json), error = ?, "
                            + "step_version = step_version + 1 WHERE id = ?",
                    state.name(), resultJson, error, stepRunId);
        }
    }

    private String lastExternalRef(long stepRunId) {
        try (Store.Tx conn = store.open()) {
            Map<String, Object> row = store.queryOne(conn,
                    "SELECT external_ref FROM receipts WHERE step_run_id = ? ORDER BY id DESC LIMIT 1",
                    stepRunId);
            return row == null ? null : (String) row.get("external_ref");
        }
    }

    private long stepRunId(long instanceId, String stepId) {
        try (Store.Tx conn = store.open()) {
            Map<String, Object> row = store.queryOne(conn,
                    "SELECT id FROM step_runs WHERE instance_id = ? AND step_id = ?", instanceId, stepId);
            if (row == null) {
                throw EngineException.notFound("step run " + stepId);
            }
            return ((Number) row.get("id")).longValue();
        }
    }

    /**
     * Crash recovery: an intent without a receipt means the previous worker died
     * mid-protocol. The external call may have happened, so the step is parked at
     * AWAIT_CONFIRM and must be resolved via confirm, never directly redone.
     */
    private void recoverDoubtfulSteps(long instanceId, Lease lease, List<String> events, boolean[] progress) {
        List<Map<String, Object>> steps = snapshotSteps(instanceId);
        for (Map<String, Object> s : steps) {
            if (!StepStatus.INTENT_WRITTEN.name().equals(s.get("status"))) {
                continue;
            }
            long stepRunId = ((Number) s.get("id")).longValue();
            String stepId = (String) s.get("step_id");
            int attempt = ((Number) s.get("attempt")).intValue();
            try (Store.Tx conn = store.open()) {
                Map<String, Object> receipt = store.queryOne(conn,
                        "SELECT COUNT(*) AS c FROM receipts WHERE step_run_id = ? AND attempt = ?",
                        stepRunId, attempt);
                if (((Number) receipt.get("c")).intValue() > 0) {
                    continue;
                }
                store.update(conn,
                        "UPDATE step_runs SET status = ?, step_version = step_version + 1 WHERE id = ?",
                        StepStatus.AWAIT_CONFIRM.name(), stepRunId);
                event(conn, instanceId, stepId, "crash_recovered_to_await_confirm",
                        Json.write(Map.of("attempt", attempt,
                                "reason", "intent without receipt; confirm required")));
            }
            events.add(stepId + ":recovered-to-await-confirm");
            progress[0] = true;
        }
    }

    // -------------------------------------------------------- manual confirm

    /**
     * Operator approves or rejects an UNKNOWN step. Optimistic concurrency via
     * expected row_version: two operators approving concurrently get a version
     * conflict; only one approval takes effect.
     */
    public Map<String, Object> manualConfirm(long instanceId, String stepId, String verdictRaw,
                                             String operator, int expectedVersion) {
        InstanceRow inst = loadInstance(instanceId);
        if (inst.status == InstanceStatus.LOCKED) {
            throw EngineException.conflict("instance is locked (derived source)");
        }
        long stepRunId = stepRunId(instanceId, stepId);
        int currentVersion;
        String state;
        try (Store.Tx conn = store.open()) {
            Map<String, Object> row = store.queryOne(conn,
                    "SELECT step_version, status FROM step_runs WHERE id = ?", stepRunId);
            currentVersion = ((Number) row.get("step_version")).intValue();
            state = (String) row.get("status");
        }
        if (currentVersion != expectedVersion) {
            throw EngineException.versionConflict(
                    "step version " + expectedVersion + " is stale; current is " + currentVersion);
        }
        if (!StepStatus.AWAIT_CONFIRM.name().equals(state)) {
            throw EngineException.conflict("step " + stepId + " is not awaiting confirmation (state=" + state + ")");
        }
        Verdict verdict = Verdict.parse(verdictRaw);
        Definition def = definitions.getById(inst.defId);
        StepDef step = def.step(stepId);
        Lease lease = leases.acquireOrRenew(instanceId, operator == null ? "operator" : operator);
        Adapter adapter = DefinitionService.buildAdapter(def, clock);
        int attempt;
        try (Store.Tx conn = store.open()) {
            attempt = ((Number) store.queryOne(conn,
                    "SELECT attempt AS a FROM step_runs WHERE id = ?", stepRunId).get("a")).intValue();
        }
        AdapterResponse response = adapter.confirm(step.id(), attempt - 1,
                lastExternalRef(stepRunId), Map.of(), lease.fencingToken());
        // For an operator decision the explicit verdict overrides the scripted one
        // only when the script says UNKNOWN: manual is the final word. If scripted
        // confirm is deterministic SUCCESS/FAILURE it must match the intent of the
        // playground; we honor the operator's verdict directly.
        AdapterResponse decided = new AdapterResponse(verdict,
                response.externalRef(), response.payload(),
                verdict == Verdict.SUCCESS ? null : "operator:" + verdict.name());
        try (Store.Tx conn = store.open()) {
            int locked = store.update(conn,
                    "UPDATE step_runs SET step_version = step_version + 1 WHERE id = ? AND step_version = ?",
                    stepRunId, expectedVersion);
            if (locked == 0) {
                throw EngineException.versionConflict("concurrent manual confirmation detected");
            }
            store.insert(conn,
                    "INSERT INTO confirms(step_run_id, attempt, verdict, payload_json, manual, operator, "
                            + "fencing_token, created_at_ms) VALUES(?,?,?,?,?,?,?,?)",
                    stepRunId, attempt, decided.verdict().name(), Json.write(decided.payload()),
                    1, operator == null ? "" : operator, lease.generation(), clock.nowMs());
        }
        if (decided.verdict() == Verdict.SUCCESS) {
            setStepState(stepRunId, StepStatus.CONFIRMED, decided, null);
            try (Store.Tx conn = store.open()) {
                event(conn, instanceId, stepId, "manual_confirmed",
                        Json.write(Map.of("operator", operator, "version", expectedVersion)));
            }
            setInstanceStatus(instanceId, InstanceStatus.RUNNING, lease);
        } else {
            setStepState(stepRunId, StepStatus.FAILED, decided, decided.error());
            try (Store.Tx conn = store.open()) {
                event(conn, instanceId, stepId, "manual_rejected",
                        Json.write(Map.of("operator", operator, "verdict", decided.verdict().name())));
            }
            beginCompensation(instanceId, def, lease, new ArrayList<>(), new boolean[]{true});
        }
        leases.release(instanceId, lease);
        return Map.of("instanceId", instanceId, "stepId", stepId, "verdict", decided.verdict().name());
    }

    // --------------------------------------------------------- compensation

    private void beginCompensation(long instanceId, Definition def, Lease lease,
                                   List<String> events, boolean[] progress) {
        InstanceRow inst = loadInstance(instanceId);
        if (inst.status == InstanceStatus.COMPENSATING || inst.status == InstanceStatus.FAILED_COMPENSATION
                || inst.status == InstanceStatus.COMPENSATED) {
            return;
        }
        List<String> order = DefinitionService.reverseTopo(def.steps());
        try (Store.Tx conn = store.open()) {
            store.update(conn,
                    "UPDATE instances SET status = ?, comp_order_json = ?, comp_cursor = 0, "
                            + "row_version = row_version + 1, updated_at_ms = ? WHERE id = ?",
                    InstanceStatus.COMPENSATING.name(), Json.write(order), clock.nowMs(), instanceId);
            event(conn, instanceId, null, "compensation_started",
                    Json.write(Map.of("order", order)));
        }
        events.add("compensation-started:" + order);
        progress[0] = true;
    }

    /**
     * One compensation pass, driven by a persisted cursor over the reverse
     * dependency order. Successful branches are undone in reverse dependency
     * order; still-running branches are brought to a stable boundary first.
     * A failed compensation keeps FAILED_COMPENSATION plus the cursor so the next
     * pass resumes exactly there; the instance is never force-marked compensated.
     */
    private void compensationPass(long instanceId, Lease lease, Definition def, Adapter adapter,
                                  List<String> events, boolean[] progress) {
        InstanceRow inst = loadInstance(instanceId);
        List<String> order = inst.compOrderJson == null
                ? DefinitionService.reverseTopo(def.steps())
                : Json.MAPPER.convertValue(Json.readMap("{\"x\":" + inst.compOrderJson() + "}").get("x"),
                        List.class);

        // Stable boundary: any step still mid-protocol (intent, no receipt) is
        // moved to AWAIT_CONFIRM so compensation never races an in-flight call.
        stabilizeForCompensation(instanceId, lease, events);

        List<Map<String, Object>> steps = snapshotSteps(instanceId);
        int cursor = inst.compCursor();
        boolean blocked = false;

        for (int i = cursor; i < order.size(); i++) {
            String stepId = order.get(i);
            StepDef step = def.step(stepId);
            long stepRunId = stepRunId(instanceId, stepId);
            String state = currentStepState(stepRunId);

            if (StepStatus.COMPENSATED.name().equals(state)) {
                advanceCursor(instanceId, i + 1);
                cursor = i + 1;
                continue;
            }
            if (StepStatus.PENDING.name().equals(state)) {
                advanceCursor(instanceId, i + 1);
                cursor = i + 1;
                continue;
            }
            if (StepStatus.AWAIT_CONFIRM.name().equals(state)) {
                // Running branch has not reached a stable business boundary yet.
                // Park the cursor; the operator must confirm before compensation can
                // cross this step.
                blocked = true;
                events.add(stepId + ":await-confirm-blocks-compensation");
                break;
            }
            if (StepStatus.COMPENSATE_FAILED.name().equals(state)) {
                persistFailedCompensation(instanceId, i, stepId, events);
                return;
            }
            if (!Set.of(StepStatus.CONFIRMED.name(), StepStatus.FAILED.name()).contains(state)) {
                blocked = true;
                events.add(stepId + ":not-at-stable-boundary:" + state);
                break;
            }
            if (StepStatus.FAILED.name().equals(state)) {
                // The failing step itself never confirmed; nothing to undo.
                markCompensatedSkipped(stepRunId, stepId, instanceId, "failed-step-nothing-to-undo", events);
                advanceCursor(instanceId, i + 1);
                cursor = i + 1;
                progress[0] = true;
                continue;
            }

            boolean ok = compensateOne(instanceId, step, stepRunId, lease, adapter, events);
            if (!ok) {
                persistFailedCompensation(instanceId, i, stepId, events);
                return;
            }
            advanceCursor(instanceId, i + 1);
            cursor = i + 1;
            progress[0] = true;
        }

        if (blocked) {
            setInstanceStatus(instanceId, InstanceStatus.COMPENSATING, lease);
            return;
        }
        if (cursor >= order.size()) {
            try (Store.Tx conn = store.open()) {
                setInstanceStatusIn(conn, instanceId, InstanceStatus.COMPENSATED, lease);
                event(conn, instanceId, null, "instance_compensated",
                        Json.write(Map.of("order", order)));
            }
            events.add("instance-compensated");
        }
    }

    private void stabilizeForCompensation(long instanceId, Lease lease, List<String> events) {
        recoverDoubtfulSteps(instanceId, lease, events, new boolean[1]);
    }

    private boolean compensateOne(long instanceId, StepDef step, long stepRunId,
                                  Lease lease, Adapter adapter, List<String> events) {
        int nextAttempt;
        try (Store.Tx conn = store.open()) {
            store.update(conn,
                    "UPDATE step_runs SET status = ?, step_version = step_version + 1 WHERE id = ?",
                    StepStatus.COMPENSATING.name(), stepRunId);
            nextAttempt = ((Number) store.queryOne(conn,
                    "SELECT COALESCE(MAX(attempt), 0) + 1 AS a FROM compensations WHERE step_run_id = ?",
                    stepRunId).get("a")).intValue();
        }
        AdapterResponse response = adapter.compensate(
                step.id(), nextAttempt - 1, lastExternalRef(stepRunId), Map.of(), lease.fencingToken());
        try (Store.Tx conn = store.open()) {
            store.insert(conn,
                    "INSERT INTO compensations(step_run_id, attempt, verdict, payload_json, fencing_token, "
                            + "created_at_ms) VALUES(?,?,?,?,?,?)",
                    stepRunId, nextAttempt, response.verdict().name(), Json.write(response.payload()),
                    lease.generation(), clock.nowMs());
            if (lease.generation() != loadInstance(instanceId).leaseGeneration()) {
                throw EngineException.staleLease("compensation fence stale");
            }
        }
        if (response.verdict() == Verdict.SUCCESS) {
            try (Store.Tx conn = store.open()) {
                store.update(conn,
                        "UPDATE step_runs SET status = ?, step_version = step_version + 1 WHERE id = ?",
                        StepStatus.COMPENSATED.name(), stepRunId);
                event(conn, instanceId, step.id(), "compensated",
                        Json.write(Map.of("attempt", nextAttempt)));
            }
            events.add(step.id() + ":compensated");
            return true;
        }
        try (Store.Tx conn = store.open()) {
            store.update(conn,
                    "UPDATE step_runs SET status = ?, error = ?, step_version = step_version + 1 WHERE id = ?",
                    StepStatus.COMPENSATE_FAILED.name(),
                    response.error() == null ? response.verdict().name() : response.error(), stepRunId);
            event(conn, instanceId, step.id(), "compensation_failed",
                    Json.write(Map.of("attempt", nextAttempt, "verdict", response.verdict().name())));
        }
        events.add(step.id() + ":compensation-failed");
        return false;
    }

    private void markCompensatedSkipped(long stepRunId, String stepId, long instanceId,
                                        String reason, List<String> events) {
        try (Store.Tx conn = store.open()) {
            store.update(conn,
                    "UPDATE step_runs SET status = ?, step_version = step_version + 1 WHERE id = ?",
                    StepStatus.COMPENSATED.name(), stepRunId);
            event(conn, instanceId, stepId, "compensated_skip", reason);
        }
        events.add(stepId + ":compensated-skip:" + reason);
    }

    private void advanceCursor(long instanceId, int cursor) {
        try (Store.Tx conn = store.open()) {
            store.update(conn,
                    "UPDATE instances SET comp_cursor = ?, updated_at_ms = ? WHERE id = ?",
                    cursor, clock.nowMs(), instanceId);
        }
    }

    private void persistFailedCompensation(long instanceId, int cursor, String stepId,
                                           List<String> events) {
        try (Store.Tx conn = store.open()) {
            store.update(conn,
                    "UPDATE instances SET status = ?, comp_cursor = ?, "
                            + "row_version = row_version + 1, updated_at_ms = ? WHERE id = ?",
                    InstanceStatus.FAILED_COMPENSATION.name(), cursor, clock.nowMs(), instanceId);
            event(conn, instanceId, stepId, "failed_compensation",
                    Json.write(Map.of("cursor", cursor,
                            "resume", "compensation continues from this step on the next pass")));
        }
        events.add("failed-compensation-at:" + stepId + "#cursor=" + cursor);
    }

    /** Continue a FAILED_COMPENSATION instance from its persisted cursor. */
    public TickReport resumeCompensation(long instanceId, String worker) {
        InstanceRow inst = loadInstance(instanceId);
        if (inst.status != InstanceStatus.FAILED_COMPENSATION && inst.status != InstanceStatus.COMPENSATING) {
            throw EngineException.conflict("instance is not in a resumable compensation state: " + inst.status);
        }
        Lease lease = leases.acquireOrRenew(instanceId, worker);
        List<String> events = new ArrayList<>();
        boolean[] progress = {false};
        Definition def = definitions.getById(inst.defId);
        // Retry the failed step: reset it so the cursor re-enters it.
        try (Store.Tx conn = store.open()) {
            List<String> order = Json.MAPPER.convertValue(
                    Json.readMap("{\"x\":" + inst.compOrderJson() + "}").get("x"), List.class);
            String stepId = order.get(inst.compCursor());
            long stepRunId = stepRunId(instanceId, stepId);
            store.update(conn,
                    "UPDATE step_runs SET status = ?, error = NULL, step_version = step_version + 1 WHERE id = ?",
                    StepStatus.CONFIRMED.name(), stepRunId);
            store.update(conn,
                    "UPDATE instances SET status = ?, updated_at_ms = ? WHERE id = ?",
                    InstanceStatus.COMPENSATING.name(), clock.nowMs(), instanceId);
            event(conn, instanceId, stepId, "compensation_resume",
                    Json.write(Map.of("cursor", inst.compCursor())));
        }
        Adapter adapter = DefinitionService.buildAdapter(def, clock);
        compensationPass(instanceId, lease, def, adapter, events, progress);
        leases.release(instanceId, lease);
        InstanceRow after = loadInstance(instanceId);
        return new TickReport(instanceId, after.status.name(), events, true);
    }

    // ------------------------------------------------------------ retry

    /**
     * Derive a fresh retry instance from a failed/compensated one. The old
     * instance is locked and never modified. Only steps that were CONFIRMED and
     * whose resolved input digest still matches are reused; everything else is
     * re-executed from scratch in the new instance.
     */
    public long deriveRetry(long parentInstanceId, String idemKey, Map<String, Object> inputOverride) {
        InstanceRow parent = loadInstance(parentInstanceId);
        if (parent.status == InstanceStatus.LOCKED) {
            // Idempotent re-derive: look up existing child.
            try (Store.Tx conn = store.open()) {
                Map<String, Object> child = store.queryOne(conn,
                        "SELECT id FROM instances WHERE parent_instance_id = ? ORDER BY id LIMIT 1",
                        parentInstanceId);
                if (child != null) {
                    return ((Number) child.get("id")).longValue();
                }
            }
        }
        Definition def = definitions.getById(parent.defId);
        Map<String, Object> input = inputOverride == null || inputOverride.isEmpty()
                ? Json.readMap(parent.inputJson)
                : inputOverride;
        String inputHash = Inputs.digest("__input__", input);
        try (Store.Tx conn = store.open()) {
            if (idemKey != null && !idemKey.isBlank()) {
                Map<String, Object> existing = store.queryOne(conn,
                        "SELECT id, input_hash FROM instances WHERE idem_key = ?", idemKey);
                if (existing != null) {
                    if (!inputHash.equals(existing.get("input_hash"))) {
                        throw EngineException.conflict("idempotency key reused with different content");
                    }
                    return ((Number) existing.get("id")).longValue();
                }
            }
            long now = clock.nowMs();
            long childId = store.insert(conn,
                    "INSERT INTO instances(def_id, idem_key, input_hash, input_json, status, "
                            + "parent_instance_id, lease_generation, comp_cursor, row_version, "
                            + "created_at_ms, updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    def.id(), blankToNull(idemKey), inputHash, Json.write(input),
                    InstanceStatus.RUNNING.name(), parentInstanceId, 0, 0, 1, now, now);

            Map<String, Map<String, Object>> parentResults = new LinkedHashMap<>();
            List<Map<String, Object>> parentSteps = store.query(conn,
                    "SELECT * FROM step_runs WHERE instance_id = ? ORDER BY id", parentInstanceId);
            for (Map<String, Object> pr : parentSteps) {
                if (StepStatus.CONFIRMED.name().equals(pr.get("status"))) {
                    parentResults.put((String) pr.get("step_id"),
                            Json.readMap((String) pr.get("result_json")));
                }
            }
            List<String> reused = new ArrayList<>();
            List<String> redone = new ArrayList<>();
            for (StepDef step : def.steps()) {
                Map<String, Object> pr = parentSteps.stream()
                        .filter(r -> step.id().equals(r.get("step_id"))).findFirst().orElseThrow();
                // Params referencing predecessors can only be resolved for reuse when
                // every predecessor is itself reusable; otherwise this step must rerun.
                Map<String, Object> params;
                try {
                    params = Inputs.resolveParams(step.params(), input, parentResults);
                } catch (EngineException missing) {
                    params = Map.of("_unresolved", true);
                }
                String digest = Inputs.digest(step.id(), params);
                boolean canReuse = StepStatus.CONFIRMED.name().equals(pr.get("status"))
                        && digest.equals(pr.get("input_digest"))
                        && parent.defId == def.id();
                long newRunId = store.insert(conn,
                        "INSERT INTO step_runs(instance_id, step_id, status, attempt, input_digest, "
                                + "executed_generation, reused_from_step, result_json, step_version) "
                                + "VALUES(?,?,?,?,?,?,?,?,1)",
                        childId, step.id(),
                        canReuse ? StepStatus.CONFIRMED.name() : StepStatus.PENDING.name(),
                        canReuse ? ((Number) pr.get("attempt")).intValue() : 0,
                        digest, canReuse ? 0 : null,
                        canReuse ? ((Number) pr.get("id")).longValue() : null,
                        canReuse ? (String) pr.get("result_json") : null);
                if (canReuse) {
                    reused.add(step.id());
                    parentResults.put(step.id(), Json.readMap((String) pr.get("result_json")));
                    store.insert(conn,
                            "INSERT INTO events(instance_id, step_id, kind, detail, created_at_ms) "
                                    + "VALUES(?,?,?,?,?)",
                            childId, step.id(), "step_reused_from_parent",
                            Json.write(Map.of("parentInstanceId", parentInstanceId,
                                    "parentStepRunId", pr.get("id"), "inputDigest", digest)), now);
                } else {
                    redone.add(step.id());
                }
            }
            store.update(conn, "UPDATE instances SET status = 'LOCKED', updated_at_ms = ? WHERE id = ?",
                    now, parentInstanceId);
            store.insert(conn,
                    "INSERT INTO events(instance_id, step_id, kind, detail, created_at_ms) "
                            + "VALUES(?,?,?,?,?)",
                    parentInstanceId, null, "instance_locked_derived",
                    Json.write(Map.of("childInstanceId", childId)), now);
            store.insert(conn,
                    "INSERT INTO events(instance_id, step_id, kind, detail, created_at_ms) "
                            + "VALUES(?,?,?,?,?)",
                    childId, null, "instance_derived",
                    Json.write(Map.of("parentInstanceId", parentInstanceId,
                            "reused", reused, "redone", redone)), now);
            return childId;
        }
    }

    // ---------------------------------------------------------------- views

    public Map<String, Object> instanceView(long instanceId) {
        InstanceRow inst = loadInstance(instanceId);
        Definition def = definitions.getById(inst.defId);
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("id", inst.id());
        view.put("defId", def.id());
        view.put("defKey", def.defKey());
        view.put("defVersion", def.version());
        view.put("adapterVersion", def.adapterVersion());
        view.put("frozenDigest", def.digest());
        view.put("status", inst.status().name());
        view.put("input", Json.readMap(inst.inputJson()));
        view.put("parentInstanceId", inst.parentInstanceId());
        view.put("rowVersion", inst.rowVersion());
        view.put("compensationOrder", inst.compOrderJson() == null ? List.of()
                : Json.MAPPER.convertValue(Json.readMap("{\"x\":" + inst.compOrderJson() + "}").get("x"),
                        List.class));
        view.put("compensationCursor", inst.compCursor());
        view.put("lease", Map.of(
                "owner", inst.leaseOwner() == null ? "" : inst.leaseOwner(),
                "generation", inst.leaseGeneration(),
                "expiresAtMs", inst.leaseExpiresAt(),
                "fencingToken", inst.leaseGeneration()));
        try (Store.Tx conn = store.open()) {
            List<Map<String, Object>> stepViews = new ArrayList<>();
            for (Map<String, Object> sr : store.query(conn,
                    "SELECT * FROM step_runs WHERE instance_id = ? ORDER BY id", instanceId)) {
                long stepRunId = ((Number) sr.get("id")).longValue();
                Map<String, Object> sv = new LinkedHashMap<>(sr);
                sv.put("intents", store.query(conn,
                        "SELECT attempt, payload_json, fencing_token, created_at_ms "
                                + "FROM intents WHERE step_run_id = ? ORDER BY id", stepRunId));
                sv.put("receipts", store.query(conn,
                        "SELECT attempt, external_ref, verdict, payload_json, fencing_token, accepted, "
                                + "created_at_ms FROM receipts WHERE step_run_id = ? ORDER BY id", stepRunId));
                sv.put("confirms", store.query(conn,
                        "SELECT attempt, verdict, payload_json, manual, operator, fencing_token, created_at_ms "
                                + "FROM confirms WHERE step_run_id = ? ORDER BY id", stepRunId));
                sv.put("compensations", store.query(conn,
                        "SELECT attempt, verdict, payload_json, fencing_token, created_at_ms "
                                + "FROM compensations WHERE step_run_id = ? ORDER BY id", stepRunId));
                stepViews.add(sv);
            }
            view.put("steps", stepViews);
            view.put("events", store.query(conn,
                    "SELECT id, step_id, kind, detail, created_at_ms FROM events "
                            + "WHERE instance_id = ? ORDER BY id", instanceId));
        }
        return view;
    }

    private String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }
}
