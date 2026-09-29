package compapp.engine;

import compapp.model.Definition;
import compapp.model.InstanceStatus;
import compapp.model.StepStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EngineTest {

    private Map<String, Object> step(String id, List<String> deps, Object execute,
                                    Object confirm, Object compensate) {
        return Map.of("id", id, "dependsOn", deps,
                "execute", execute, "confirm", confirm, "compensate", compensate);
    }

    @Test
    void happyPathSerialFlowCompletes() {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("happy", List.of(
                    step("a", List.of(), "SUCCESS", "SUCCESS", "SUCCESS"),
                    step("b", List.of("a"), "SUCCESS", "SUCCESS", "SUCCESS")));
            long id = h.engine.start(def.id(), "k1", Map.of("x", "1"));
            h.engine.tick(id, "w1");
            h.engine.tick(id, "w1");
            var view = h.view(id);
            assertEquals(InstanceStatus.COMPLETED.name(), view.get("status"));
            assertEquals(StepStatus.CONFIRMED.name(), h.step(id, "a").get("status"));
            assertEquals(StepStatus.CONFIRMED.name(), h.step(id, "b").get("status"));
        }
    }

    @Test
    void idempotentStartReturnsSameInstanceAndConflictsOnDifferentContent() {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("d", List.of(step("a", List.of(), "SUCCESS", "SUCCESS", "SUCCESS")));
            long first = h.engine.start(def.id(), "idem-1", Map.of("amount", 100));
            long second = h.engine.start(def.id(), "idem-1", Map.of("amount", 100));
            assertEquals(first, second);
            EngineException conflict = assertThrows(EngineException.class,
                    () -> h.engine.start(def.id(), "idem-1", Map.of("amount", 999)));
            assertEquals("conflict", conflict.code());
        }
    }

    @Test
    void unknownResultRequiresConfirmAndNeverReExecutes() {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("unknown", List.of(
                    step("a", List.of(), "UNKNOWN", "SUCCESS", "SUCCESS")));
            long id = h.engine.start(def.id(), null, Map.of());
            h.engine.tick(id, "w1");
            assertEquals(StepStatus.AWAIT_CONFIRM.name(), h.step(id, "a").get("status"));
            assertEquals(InstanceStatus.AWAIT_MANUAL.name(), h.view(id).get("status"));
            int version = ((Number) h.step(id, "a").get("step_version")).intValue();
            h.engine.manualConfirm(id, "a", "SUCCESS", "op", version);
            h.engine.tick(id, "w1");
            assertEquals(InstanceStatus.COMPLETED.name(), h.view(id).get("status"));
            // Exactly one intent: unknown must never trigger a blind re-execution.
            @SuppressWarnings("unchecked")
            int intents = ((List<?>) h.step(id, "a").get("intents")).size();
            assertEquals(1, intents);
            @SuppressWarnings("unchecked")
            long confirms = ((List<?>) h.step(id, "a").get("confirms")).size();
            assertTrue(confirms >= 1);
        }
    }

    @Test
    void parallelBranchesCompensatedInReverseDependencyOrder() {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("parallel", List.of(
                    step("root", List.of(), "SUCCESS", "SUCCESS", "SUCCESS"),
                    step("branch-a", List.of("root"), "SUCCESS", "SUCCESS", "SUCCESS"),
                    step("branch-b", List.of("root"), "SUCCESS", "SUCCESS", "SUCCESS"),
                    step("failing", List.of("root"), "FAILURE", "FAILURE", "SUCCESS")));
            long id = h.engine.start(def.id(), null, Map.of());
            h.engine.tick(id, "w1"); // root
            h.engine.tick(id, "w1"); // parallel branches + failing -> compensation order
            assertEquals(InstanceStatus.COMPENSATED.name(), h.view(id).get("status"));
            @SuppressWarnings("unchecked")
            List<String> order = (List<String>) h.view(id).get("compensationOrder");
            // failing (nothing to undo) then branches before root: reverse topo.
            assertTrue(order.indexOf("failing") < order.indexOf("branch-a"));
            assertTrue(order.indexOf("branch-a") < order.indexOf("root"));
            assertEquals(StepStatus.COMPENSATED.name(), h.step(id, "root").get("status"));
            assertEquals(StepStatus.COMPENSATED.name(), h.step(id, "branch-a").get("status"));
            assertEquals(StepStatus.COMPENSATED.name(), h.step(id, "branch-b").get("status"));
        }
    }

    @Test
    void stillRunningBranchReachesStableBoundaryBeforeCompensationCrossesIt() {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("boundary", List.of(
                    step("root", List.of(), "UNKNOWN", "SUCCESS", "SUCCESS"),
                    step("late", List.of("root"), "SUCCESS", "SUCCESS", "SUCCESS"),
                    step("boom", List.of("root"), "FAILURE", "FAILURE", "SUCCESS")));
            long id = h.engine.start(def.id(), null, Map.of());
            h.engine.tick(id, "w1"); // root UNKNOWN -> AWAIT_CONFIRM
            // Cannot run dependents yet; manual reject root => compensation starts.
            int version = ((Number) h.step(id, "root").get("step_version")).intValue();
            h.engine.manualConfirm(id, "root", "SUCCESS", "op", version);
            h.engine.tick(id, "w1"); // late succeeds, boom fails, compensation runs
            assertEquals(InstanceStatus.COMPENSATED.name(), h.view(id).get("status"));
        }
    }

    @Test
    void oldGenerationLeaseLateReceiptIsRejectedAndCannotAdvance() {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("fence", List.of(
                    step("a", List.of(), "SUCCESS", "SUCCESS", "SUCCESS")));
            long id = h.engine.start(def.id(), null, Map.of());
            Lease oldLease = h.leases.acquireOrRenew(id, "old-worker");
            h.advance(60_001);
            Lease newLease = h.leases.takeover(id, "new-worker");
            assertTrue(newLease.generation() > oldLease.generation());
            EngineException stale = assertThrows(EngineException.class,
                    () -> h.engine.tick(id, "old-worker", oldLease, false));
            assertEquals("stale_lease", stale.code());
            // New generation completes normally.
            h.engine.tick(id, "new-worker", newLease, true);
            assertEquals(InstanceStatus.COMPLETED.name(), h.view(id).get("status"));
        }
    }

    @Test
    void partialCompensationFailureKeepsStateAndResumableCursor() {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("partial", List.of(
                    step("a", List.of(), "SUCCESS", "SUCCESS",
                            List.of(Map.of("verdict", "FAILURE", "error", "undo busy"),
                                    Map.of("verdict", "SUCCESS"))),
                    step("b", List.of("a"), "FAILURE", "FAILURE", "SUCCESS")));
            long id = h.engine.start(def.id(), null, Map.of());
            h.engine.tick(id, "w1"); // a confirmed
            h.engine.tick(id, "w1"); // b fails -> compensation: b skip, a compensate FAILS
            assertEquals(InstanceStatus.FAILED_COMPENSATION.name(), h.view(id).get("status"));
            assertEquals(StepStatus.COMPENSATE_FAILED.name(), h.step(id, "a").get("status"));
            int cursor = ((Number) h.view(id).get("compensationCursor")).intValue();
            @SuppressWarnings("unchecked")
            List<String> order = (List<String>) h.view(id).get("compensationOrder");
            assertEquals(order.indexOf("a"), cursor);

            var report = h.engine.resumeCompensation(id, "w2");
            assertEquals(InstanceStatus.COMPENSATED.name(), report.status());
            assertEquals(StepStatus.COMPENSATED.name(), h.step(id, "a").get("status"));
        }
    }

    @Test
    void derivedRetryReusesOnlyConfirmedStepsWithMatchingInputDigestAndLocksOld() {
        try (TestHarness h = new TestHarness()) {
            // a stays CONFIRMED; sibling is UNKNOWN (stable boundary blocks compensation);
            // boom fails, so the instance parks in COMPENSATING with a still confirmed.
            Definition def = h.publish("retry", List.of(
                    Map.of("id", "a", "dependsOn", List.of(),
                            "params", Map.of("account", "${input.account}"),
                            "execute", Map.of("verdict", "SUCCESS",
                                    "result", Map.of("token", "T-${input.account}")),
                            "confirm", "SUCCESS", "compensate", "SUCCESS"),
                    Map.of("id", "sibling", "dependsOn", List.of("a"),
                            "execute", "UNKNOWN", "confirm", "SUCCESS", "compensate", "SUCCESS"),
                    Map.of("id", "boom", "dependsOn", List.of("a"),
                            "execute", "FAILURE", "confirm", "FAILURE", "compensate", "SUCCESS")));
            long parent = h.engine.start(def.id(), null, Map.of("account", "ACC-1"));
            h.engine.tick(parent, "w1"); // a confirmed
            h.engine.tick(parent, "w1"); // sibling UNKNOWN + boom fails, compensation blocked
            assertEquals(InstanceStatus.COMPENSATING.name(), h.view(parent).get("status"));
            assertEquals(StepStatus.CONFIRMED.name(), h.step(parent, "a").get("status"));
            long manual = parent;
            long child = h.engine.deriveRetry(manual, "retry-1", null);
            assertEquals(InstanceStatus.LOCKED.name(), h.view(manual).get("status"));
            assertEquals(StepStatus.CONFIRMED.name(), h.step(child, "a").get("status"));
            assertEquals(StepStatus.AWAIT_CONFIRM.name(), h.step(parent, "sibling").get("status"));
            assertEquals(StepStatus.PENDING.name(), h.step(child, "sibling").get("status"));
            assertEquals(StepStatus.PENDING.name(), h.step(child, "boom").get("status"));
            assertNotNull(h.step(child, "a").get("reused_from_step"));

            // Old instance stays immutable; child can be worked independently.
            h.engine.tick(child, "w1");
            assertEquals(InstanceStatus.LOCKED.name(), h.view(manual).get("status"));
        }
    }

    @Test
    void derivedRetryRecomputesDigestWhenInputChangesAndRefusesToReuse() {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("retry2", List.of(
                    Map.of("id", "a", "dependsOn", List.of(),
                            "params", Map.of("account", "${input.account}"),
                            "execute", "SUCCESS", "confirm", "SUCCESS", "compensate", "SUCCESS"),
                    step("b", List.of("a"), "FAILURE", "FAILURE", "SUCCESS")));
            long parent = h.engine.start(def.id(), null, Map.of("account", "ACC-1"));
            h.engine.tick(parent, "w1");
            h.engine.tick(parent, "w1");
            long child = h.engine.deriveRetry(parent, "retry-2", Map.of("account", "ACC-2"));
            assertEquals(StepStatus.PENDING.name(), h.step(child, "a").get("status"));
            assertNull(h.step(child, "a").get("reused_from_step"));
        }
    }

    @Test
    void crashAfterIntentBeforeReceiptIsRecoveredViaConfirmNotRedo() {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("crash", List.of(
                    step("a", List.of(), "SUCCESS", "SUCCESS", "SUCCESS"),
                    step("b", List.of("a"), "SUCCESS", "SUCCESS", "SUCCESS")));
            long id = h.engine.start(def.id(), null, Map.of());
            Lease lease = h.leases.acquireOrRenew(id, "dying-worker");
            // Simulate crash immediately after the intent row was written.
            long stepRunId = ((Number) h.step(id, "a").get("id")).longValue();
            try (var conn = h.store.open()) {
                h.store.update(conn,
                        "UPDATE step_runs SET status='INTENT_WRITTEN', attempt=1, executed_generation=? WHERE id=?",
                        lease.generation(), stepRunId);
                h.store.insert(conn,
                        "INSERT INTO intents(step_run_id, attempt, payload_json, fencing_token, created_at_ms) "
                                + "VALUES(?,?,?,?,?)",
                        stepRunId, 1, "{}", lease.generation(), h.clock.nowMs());
            }
            h.advance(60_001);
            Lease next = h.leases.takeover(id, "recovery-worker");
            h.engine.tick(id, "recovery-worker", next, true);
            assertEquals(StepStatus.AWAIT_CONFIRM.name(), h.step(id, "a").get("status"));
            @SuppressWarnings("unchecked")
            int intents = ((List<?>) h.step(id, "a").get("intents")).size();
            assertEquals(1, intents, "crashed step must not be re-executed; confirm is required");
            boolean recoveredEvent = h.events(id).stream()
                    .anyMatch(e -> "crash_recovered_to_await_confirm".equals(e.get("kind")));
            assertTrue(recoveredEvent);
        }
    }

    @Test
    void twoConcurrentManualApprovalsProduceVersionConflict() throws Exception {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("manual", List.of(
                    step("a", List.of(), "UNKNOWN", "SUCCESS", "SUCCESS")));
            long id = h.engine.start(def.id(), null, Map.of());
            h.engine.tick(id, "w1");
            int version = ((Number) h.step(id, "a").get("step_version")).intValue();

            // Simulate two operators reading the same version, then both approving.
            h.engine.manualConfirm(id, "a", "SUCCESS", "operator-A", version);
            EngineException conflict = assertThrows(EngineException.class,
                    () -> h.engine.manualConfirm(id, "a", "SUCCESS", "operator-B", version));
            assertEquals("version_conflict", conflict.code());
        }
    }

    @Test
    void parallelRaceBetweenTwoWorkersOnlyOneAdvancesUnderFencing() throws Exception {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("race", List.of(
                    step("a", List.of(), "SUCCESS", "SUCCESS", "SUCCESS")));
            long id = h.engine.start(def.id(), null, Map.of());
            Lease w1 = h.leases.acquireOrRenew(id, "w1");
            h.advance(60_001);
            Lease w2 = h.leases.takeover(id, "w2");
            // Old worker finishes late: rejected.
            assertThrows(EngineException.class, () -> h.engine.tick(id, "w1", w1, false));
            // New worker advances exactly once.
            h.engine.tick(id, "w2", w2, true);
            @SuppressWarnings("unchecked")
            int intents = ((List<?>) h.step(id, "a").get("intents")).size();
            assertEquals(1, intents);
        }
    }

    @Test
    void publishedDefinitionFreezesGraphParamsAndAdapterVersion() {
        try (TestHarness h = new TestHarness()) {
            Definition v1 = h.definitions.publish("frozen", "adapter/1", List.of(
                    step("a", List.of(), "SUCCESS", "SUCCESS", "SUCCESS")));
            Definition v2 = h.definitions.publish("frozen", "adapter/2", List.of(
                    step("a", List.of(), "SUCCESS", "SUCCESS", "SUCCESS"),
                    step("b", List.of("a"), "SUCCESS", "SUCCESS", "SUCCESS")));
            assertNotEquals(v1.digest(), v2.digest());
            assertEquals("adapter/1", h.definitions.getById(v1.id()).adapterVersion());
            long instance = h.engine.start(v1.id(), null, Map.of());
            var view = h.view(instance);
            assertEquals("adapter/1", view.get("adapterVersion"));
            assertEquals(v1.digest(), view.get("frozenDigest"));
        }
    }

    @Test
    void concurrentWorkersRaceSafelyOnSameInstance() throws Exception {
        try (TestHarness h = new TestHarness()) {
            Definition def = h.publish("concurrent", List.of(
                    step("a", List.of(), "SUCCESS", "SUCCESS", "SUCCESS"),
                    step("b", List.of("a"), "SUCCESS", "SUCCESS", "SUCCESS")));
            long id = h.engine.start(def.id(), "race-key", Map.of());

            int workers = 6;
            java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(workers);
            List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            for (int w = 0; w < workers; w++) {
                final String owner = "worker-" + w;
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int round = 0; round < 6; round++) {
                        h.advance(1);
                        try {
                            h.engine.tick(id, owner);
                        } catch (EngineException stale) {
                            // expected for workers that lost the lease generation
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (var f : futures) {
                f.get();
            }
            pool.shutdown();
            assertEquals(InstanceStatus.COMPLETED.name(), h.view(id).get("status"));
            // Exactly one intent per step despite the concurrent workers.
            assertEquals(1, ((List<?>) h.step(id, "a").get("intents")).size());
            assertEquals(1, ((List<?>) h.step(id, "b").get("intents")).size());
        }
    }
}
