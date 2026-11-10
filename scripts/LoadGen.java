import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Load generator for the validation scripts, JDK only (run as a single-file program):
 *
 *   java scripts/LoadGen.java http://localhost:8080 prefix 3000 64
 *   java scripts/LoadGen.java http://localhost:8080 - - 64 keys.txt
 *
 * Submits prefix-1..N from P parallel clients, or, with a keys file, exactly the keys listed in
 * it (one per line, each ending in -<number>). Priority follows i % 10 (0-1 HIGH, 2-5 NORMAL,
 * 6-9 LOW) and the payload is derived from the key, so running it twice re-sends identical
 * requests. Prints one line per request: "<http-code> <taskId|-> <bizKey> <priority> <seconds>",
 * with code 000 when the server did not answer.
 */
public class LoadGen {

    private static final Pattern TASK_ID = Pattern.compile("\"taskId\"\\s*:\\s*\"([^\"]+)\"");

    public static void main(String[] args) throws Exception {
        String base = args[0];
        int parallelism = Integer.parseInt(args[3]);
        List<String> keys = new ArrayList<>();
        if (args.length > 4) {
            for (String line : Files.readAllLines(Path.of(args[4]))) {
                if (!line.isBlank()) {
                    keys.add(line.strip());
                }
            }
        } else {
            for (int i = 1; i <= Integer.parseInt(args[2]); i++) {
                keys.add(args[1] + "-" + i);
            }
        }

        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        ExecutorService pool = Executors.newFixedThreadPool(parallelism);
        for (String key : keys) {
            int n = Integer.parseInt(key.substring(key.lastIndexOf('-') + 1));
            pool.submit(() -> {
                String priority = switch (n % 10) {
                    case 0, 1 -> "HIGH";
                    case 2, 3, 4, 5 -> "NORMAL";
                    default -> "LOW";
                };
                String body = "{\"bizKey\":\"" + key + "\",\"priority\":\"" + priority
                        + "\",\"payload\":{\"text\":\"Document " + key + ": a long report that needs a short summary.\"}}";
                HttpRequest request = HttpRequest.newBuilder(URI.create(base + "/api/tasks"))
                        .timeout(Duration.ofSeconds(10))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
                long start = System.nanoTime();
                String code = "000";
                String id = "-";
                try {
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    code = String.valueOf(response.statusCode());
                    Matcher m = TASK_ID.matcher(response.body());
                    if (m.find()) {
                        id = m.group(1);
                    }
                } catch (Exception e) {
                    // no answer: connection refused or reset (e.g. the server was killed)
                }
                double seconds = (System.nanoTime() - start) / 1e9;
                synchronized (System.out) {
                    System.out.printf("%s %s %s %s %.4f%n", code, id, key, priority, seconds);
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);
    }
}
