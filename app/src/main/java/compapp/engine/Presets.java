package compapp.engine;

import java.util.List;
import java.util.Map;

/**
 * Preset flows for the demo.
 *
 * trip-compensation:
 *   reserve-flight -> [book-hotel, charge-deposit] run in parallel.
 *   reserve-flight returns UNKNOWN on execute and resolves SUCCESS via confirm.
 *   book-hotel succeeds.
 *   charge-deposit fails, triggering reverse-dependency compensation:
 *   deposit (nothing, failed), hotel (undo), flight (undo).
 *   hotel's first compensate attempt fails so the instance lands in
 *   FAILED_COMPENSATION with a resumable cursor; the next attempt succeeds.
 */
public final class Presets {

    public static final String ADAPTER_VERSION = "deterministic-adapter/1.0";

    private Presets() {
    }

    public static List<Map<String, Object>> happyFlow() {
        return List.of(
                Map.of("id", "create-order", "name", "创建订单",
                        "execute", Map.of("verdict", "SUCCESS",
                                "result", Map.of("orderId", "ORD-1001")),
                        "confirm", "SUCCESS",
                        "compensate", "SUCCESS"),
                Map.of("id", "send-notice", "name", "发送通知",
                        "dependsOn", List.of("create-order"),
                        "execute", Map.of("verdict", "SUCCESS",
                                "result", Map.of("noticeId", "MSG-77")),
                        "confirm", "SUCCESS",
                        "compensate", "SUCCESS"));
    }

    public static List<Map<String, Object>> tripFlow() {
        return List.of(
                Map.of("id", "reserve-flight", "name", "预留机票",
                        "params", Map.of("flightNo", "${input.flightNo}"),
                        "execute", Map.of("verdict", "UNKNOWN",
                                "result", Map.of("pnr", "PNR-UNKNOWN")),
                        "confirm", Map.of("verdict", "SUCCESS",
                                "result", Map.of("pnr", "PNR-CONFIRMED-9", "seat", "12A")),
                        "compensate", List.of(
                                Map.of("verdict", "SUCCESS",
                                        "result", Map.of("refunded", true)))),
                Map.of("id", "book-hotel", "name", "预订酒店",
                        "dependsOn", List.of("reserve-flight"),
                        "params", Map.of("hotel", "${input.hotel}",
                                "pnr", "${steps.reserve-flight.result.pnr}"),
                        "execute", Map.of("verdict", "SUCCESS",
                                "result", Map.of("bookingId", "HTL-555")),
                        "confirm", "SUCCESS",
                        "compensate", List.of(
                                Map.of("verdict", "FAILURE", "error", "hotel undo timeout (retryable)"),
                                Map.of("verdict", "SUCCESS", "result", Map.of("cancelled", true)))),
                Map.of("id", "charge-deposit", "name", "扣押金",
                        "dependsOn", List.of("reserve-flight"),
                        "params", Map.of("amount", "${input.deposit}"),
                        "execute", Map.of("verdict", "FAILURE", "error", "deposit gateway rejected card"),
                        "confirm", "FAILURE",
                        "compensate", "SUCCESS"));
    }
}
