package compapp.engine;

import java.util.List;

public record TickReport(long instanceId, String status, List<String> events, boolean progress) {
}
