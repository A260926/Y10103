package compapp.model;

public interface Clock {
    long nowMs();

    Clock SYSTEM = System::currentTimeMillis;
}
