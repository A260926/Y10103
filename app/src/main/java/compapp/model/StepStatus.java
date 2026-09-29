package compapp.model;

public enum StepStatus {
    PENDING,
    INTENT_WRITTEN,
    RECEIPT_WRITTEN,
    AWAIT_CONFIRM,
    CONFIRMED,
    FAILED,
    COMPENSATING,
    COMPENSATED,
    COMPENSATE_FAILED
}
