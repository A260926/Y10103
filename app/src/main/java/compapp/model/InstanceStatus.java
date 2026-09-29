package compapp.model;

public enum InstanceStatus {
    RUNNING,
    AWAIT_MANUAL,
    COMPENSATING,
    FAILED_COMPENSATION,
    COMPENSATED,
    COMPLETED,
    LOCKED
}
