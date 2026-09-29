package compapp.engine;

import compapp.db.Store;

import compapp.model.Clock;

import java.sql.Connection;
import java.util.Map;

/**
 * Leases with fencing tokens. Every renewal bumps the generation; writes that
 * carry an older token are rejected ("old worker's late receipt cannot advance
 * a newer generation").
 */
public class LeaseManager {

    public static final long DEFAULT_TTL_MS = 30_000;

    private final Store store;
    private final Clock clock;
    private final long ttlMs;

    public LeaseManager(Store store, Clock clock, long ttlMs) {
        this.store = store;
        this.clock = clock;
        this.ttlMs = ttlMs;
    }

    /**
     * Acquire or renew the lease for an instance. Returns the lease held by the
     * caller. If another live owner holds it, throws conflict.
     */
    public Lease acquireOrRenew(long instanceId, String owner) {
        try (Store.Tx conn = store.open()) {
            // BEGIN IMMEDIATE serializes competing workers at the database; the
            // subsequent read-modify-write is then atomic relative to other acquirers.
            conn.prepareStatement("BEGIN IMMEDIATE").execute();
            try {
                Map<String, Object> row = store.queryOne(conn,
                        "SELECT lease_owner, lease_generation, lease_expires_at FROM instances WHERE id = ?",
                        instanceId);
                if (row == null) {
                    throw EngineException.notFound("instance " + instanceId);
                }
                long now = clock.nowMs();
                String currentOwner = (String) row.get("lease_owner");
                int gen = ((Number) row.get("lease_generation")).intValue();
                long expires = row.get("lease_expires_at") == null ? 0L
                        : ((Number) row.get("lease_expires_at")).longValue();
                if (currentOwner != null && !currentOwner.equals(owner) && expires > now) {
                    throw EngineException.conflict("lease held by " + currentOwner);
                }
                boolean sameOwnerRenew = currentOwner != null && currentOwner.equals(owner) && expires > now;
                int newGen = sameOwnerRenew ? gen : gen + 1;
                long newExpires = now + ttlMs;
                store.update(conn,
                        "UPDATE instances SET lease_owner = ?, lease_generation = ?, lease_expires_at = ?, "
                                + "updated_at_ms = ? WHERE id = ?",
                        owner, newGen, newExpires, now, instanceId);
                Lease lease = new Lease(owner, newGen, newExpires);
                conn.prepareStatement("COMMIT").execute();
                return lease;
            } catch (RuntimeException e) {
                try { conn.prepareStatement("ROLLBACK").execute(); } catch (Exception ignored) { }
                throw e;
            }
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Force-take over an expired lease (used by tests to simulate worker crash +
     * new worker). Bumps generation.
     */
    public Lease takeover(long instanceId, String newOwner) {
        try (Store.Tx conn = store.open()) {
            Map<String, Object> row = store.queryOne(conn,
                    "SELECT lease_generation FROM instances WHERE id = ?", instanceId);
            if (row == null) {
                throw EngineException.notFound("instance " + instanceId);
            }
            int newGen = ((Number) row.get("lease_generation")).intValue() + 1;
            long now = clock.nowMs();
            store.update(conn,
                    "UPDATE instances SET lease_owner = ?, lease_generation = ?, lease_expires_at = ?, "
                            + "updated_at_ms = ? WHERE id = ?",
                    newOwner, newGen, now + ttlMs, now, instanceId);
            return new Lease(newOwner, newGen, now + ttlMs);
        }
    }

    public Lease get(long instanceId) {
        try (Store.Tx conn = store.open()) {
            Map<String, Object> row = store.queryOne(conn,
                    "SELECT lease_owner, lease_generation, lease_expires_at FROM instances WHERE id = ?",
                    instanceId);
            if (row == null) {
                throw EngineException.notFound("instance " + instanceId);
            }
            return new Lease((String) row.get("lease_owner"),
                    ((Number) row.get("lease_generation")).intValue(),
                    row.get("lease_expires_at") == null ? 0L
                            : ((Number) row.get("lease_expires_at")).longValue());
        }
    }

    public boolean isValid(long instanceId, Lease lease) {
        Lease current = get(instanceId);
        return current.generation() == lease.generation()
                && lease.owner().equals(current.owner())
                && current.expiresAtMs() > clock.nowMs();
    }

    public void release(long instanceId, Lease lease) {
        try (Store.Tx conn = store.open()) {
            store.update(conn,
                    "UPDATE instances SET lease_owner = NULL, lease_expires_at = 0, updated_at_ms = ? "
                            + "WHERE id = ? AND lease_generation = ? AND lease_owner = ?",
                    clock.nowMs(), instanceId, lease.generation(), lease.owner());
        }
    }

    public long ttlMs() {
        return ttlMs;
    }
}
