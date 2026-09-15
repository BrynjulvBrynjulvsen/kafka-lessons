import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/** Run with JDK 21+: java scripts/SmokeTest.java [http://localhost:8080] */
class SmokeTest {
    public static void main(String[] args) throws Exception {
        String base = args.length == 0 ? "http://localhost:8080" : args[0];
        var http = HttpClient.newHttpClient();
        var first = new Inbox();
        var second = new Inbox();
        var wsUri = URI.create(base.replaceFirst("^http", "ws") + "/ws/topics/kafka-demo");
        var ws1 = http.newWebSocketBuilder().buildAsync(wsUri, first).get(10, TimeUnit.SECONDS);
        var ws2 = http.newWebSocketBuilder().buildAsync(wsUri, second).get(10, TimeUnit.SECONDS);
        try {
            check(first.next().contains("\"subscribed\""), "First subscription failed");
            check(second.next().contains("\"subscribed\""), "Second subscription failed");
            String key = "smoke-" + UUID.randomUUID();
            var request = HttpRequest.newBuilder(URI.create(base + "/api/messages"))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                    "{\"key\":\"" + key + "\",\"value\":\"Hello from the live smoke test\"}"))
                .build();
            var response = http.send(request, HttpResponse.BodyHandlers.ofString());
            check(response.statusCode() == 200, "Produce failed: " + response.body());
            String observed = first.nextFor(key);
            check(observed.equals(second.nextFor(key)), "Viewers received different events");
            check(observed.contains("\"record-consumed\""), "Unexpected event type");
            for (String field : new String[]{"partition", "offset"}) {
                check(number(response.body(), field).equals(number(observed, field)), field + " differs");
            }
            System.out.println("PASS: HTTP -> Kafka -> two WebSocket viewers");
            System.out.println("Produced: " + response.body());
            System.out.println("Observed: " + observed);
        } finally {
            ws1.sendClose(1000, "done").join();
            ws2.sendClose(1000, "done").join();
        }
    }

    static String number(String json, String field) {
        var match = Pattern.compile("\"" + field + "\"\\s*:\\s*(\\d+)").matcher(json);
        if (!match.find()) throw new AssertionError("Missing field " + field + ": " + json);
        return match.group(1);
    }

    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static class Inbox implements WebSocket.Listener {
        private final LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>();
        private final StringBuilder fragments = new StringBuilder();
        public void onOpen(WebSocket socket) { socket.request(1); }
        public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
            fragments.append(data);
            if (last) { messages.add(fragments.toString()); fragments.setLength(0); }
            socket.request(1);
            return null;
        }
        String next() throws InterruptedException {
            String message = messages.poll(15, TimeUnit.SECONDS);
            if (message == null) throw new AssertionError("Timed out waiting for WebSocket event");
            return message;
        }
        String nextFor(String key) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                String message = messages.poll(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                if (message != null && message.contains(key)) return message;
            }
            throw new AssertionError("Timed out waiting for key " + key);
        }
    }
}
