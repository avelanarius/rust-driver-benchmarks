import com.datastax.driver.core.*;
import com.google.common.util.concurrent.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class MainClass {

    private static Cluster cluster;
    private static Config config;
    private static final String INSERT_STRING = "INSERT INTO benchks.benchtab (pk, v1, v2) VALUES(?, ?, ?)";
    private static final String SELECT_STRING = "SELECT v1, v2 FROM benchks.benchtab WHERE pk = ?";
    private static final int SAMPLE_COUNT = 20000;

    private static PreparedStatement INSERT_PS;
    private static PreparedStatement SELECT_PS;

    private static List<Long> selectLatencies = Collections.synchronizedList(new ArrayList<>());
    private static List<Long> insertLatencies = Collections.synchronizedList(new ArrayList<>());

    private static class LatencyContainer {
        public long insertStartNanos = -1;
        public long insertEndNanos = -1;
        public long selectStartNanos = -1;
        public long selectEndNanos = -1;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        config = new Config(args);
        System.err.println("Parsed config: ");
        System.err.println(config.toString());

        cluster = Cluster.builder().addContactPoints(config.node_addresses).withProtocolVersion(ProtocolVersion.V4).build();
        cluster.getConfiguration().getPoolingOptions().setMaxQueueSize((int) Math.max(2048, 2 * config.concurrency));
        Session session = cluster.connect();

        prepareKeyspaceAndTable(session);

        if (!config.dont_prepare) {
            prepareKeyspaceAndTable(session);

            if (config.workload.equals(Config.Workload.Selects)) {
                prepareSelectsBenchmark(session);
            }
        }


        ArrayList<CompletableFuture<ResultSet>> arr = new ArrayList<>();

        System.err.println("Starting the benchmark");

        long benchmarkStart = System.nanoTime();

        INSERT_PS = session.prepare(INSERT_STRING);
        SELECT_PS = session.prepare(SELECT_STRING);

        for (int i = 0; i < config.concurrency; i++) {
            if (i + 1 == config.concurrency) {
                arr.add(execute(session, i * (config.tasks / config.concurrency), config.tasks));
            } else {
                arr.add(execute(session, i * (config.tasks / config.concurrency), (i + 1) * (config.tasks / config.concurrency)));
            }
        }

        for (Future<?> f : arr) {
            f.get(); // make sure nothing has thrown and everything finished
        }

        long benchmarkEnd = System.nanoTime();
        System.err.println(String.format("Finished\nBenchmark time: %d ms", (benchmarkEnd - benchmarkStart) / 1_000_000));
        System.out.println(String.format("time %d", (benchmarkEnd - benchmarkStart) / 1_000_000));

        for (Long latency : insertLatencies) {
            System.out.println("insert " + latency);
        }
        for (Long latency : selectLatencies) {
            System.out.println("select " + latency);
        }

        session.close();
        if (cluster != null) cluster.close();
    }

    static void prepareKeyspaceAndTable(Session session) {
        session.execute("DROP KEYSPACE IF EXISTS benchks");
        session.execute("CREATE KEYSPACE IF NOT EXISTS benchks WITH REPLICATION = {'class' " + ": 'SimpleStrategy', 'replication_factor' : 1}");
        session.execute("CREATE TABLE IF NOT EXISTS benchks.benchtab (pk " + "bigint PRIMARY KEY, v1 bigint, v2 bigint)");
        if (!cluster.getMetadata().checkSchemaAgreement()) {
            throw new RuntimeException("Schema not in agreement after preparing keyspace and table.");
        }
    }

    private static void prepareSelectsBenchmark(Session session) throws InterruptedException, ExecutionException {
        System.out.println("Preparing a selects benchmark (inserting values)...");

        ArrayList<CompletableFuture<ResultSet>> arr = new ArrayList<>();
        INSERT_PS = session.prepare(INSERT_STRING);

        Config.Workload originalWorkload = config.workload;
        config.workload = Config.Workload.Inserts; // Switch for setup purposes

        for (int i = 0; i < config.concurrency; i++) {
            if (i + 1 == config.concurrency) {
                arr.add(execute(session, i * (config.tasks / config.concurrency), config.tasks));
            } else {
                arr.add(execute(session, i * (config.tasks / config.concurrency), (i + 1) * (config.tasks / config.concurrency)));
            }
        }
        for (Future<?> f : arr) {
            f.get(); // make sure nothing has thrown and everything finished
        }

        config.workload = originalWorkload;
    }

    public static CompletableFuture<ResultSet> execute(Session s, long currentIter, long maxIter) {
        if (currentIter >= maxIter) {
            // No more iterations
            return CompletableFuture.completedFuture(null);
        }

        boolean shouldSample = ThreadLocalRandom.current().nextLong(config.tasks) < SAMPLE_COUNT;
        LatencyContainer latencyContainer = shouldSample ? new LatencyContainer() : null;

        ListenableFuture<ResultSet> fut = null;
        if (config.workload.equals(Config.Workload.Inserts) || config.workload.equals(Config.Workload.Mixed)) {
            if (shouldSample) {
                latencyContainer.insertStartNanos = System.nanoTime();
            }
            fut = s.executeAsync(INSERT_PS.bind(currentIter, 2L * currentIter, 3L * currentIter));
        }

        if (config.workload.equals(Config.Workload.Selects)) {
            if (shouldSample) {
                latencyContainer.selectStartNanos = System.nanoTime();
            }
            fut = s.executeAsync(SELECT_PS.bind(currentIter));
        } else if (config.workload.equals(Config.Workload.Mixed)) {
            fut = Futures.transform(fut, new AsyncFunction<ResultSet, ResultSet>() {
                public ListenableFuture<ResultSet> apply(ResultSet rs) throws Exception {
                    if (shouldSample) {
                        latencyContainer.insertEndNanos = System.nanoTime();
                        latencyContainer.selectStartNanos = System.nanoTime();
                    }
                    return (s.executeAsync(SELECT_PS.bind(currentIter)));
                }
            });
        }

        if (config.workload.equals(Config.Workload.Selects) || config.workload.equals(Config.Workload.Mixed)) {
            fut = Futures.transform(fut, new AsyncFunction<ResultSet, ResultSet>() {
                public ListenableFuture<ResultSet> apply(ResultSet rs) throws Exception {
                    Row r = rs.one();
                    if ((r.getLong("v1") != 2L * currentIter) || (r.getLong("v2") != 3L * currentIter)) {
                        throw new RuntimeException(String.format("Received incorrect data. " + "Expected: (%s, %s, %s). " + "Received: (%s, %s ,%s).", currentIter, 2L * currentIter, 3L * currentIter, r.getLong("pk"), r.getLong("v1"), r.getLong("v2")));
                    }
                    if (shouldSample) {
                        latencyContainer.selectEndNanos = System.nanoTime();
                    }
                    return Futures.immediateFuture(rs);
                }
            });
        } else {
            fut = Futures.transform(fut, new AsyncFunction<ResultSet, ResultSet>() {
                public ListenableFuture<ResultSet> apply(ResultSet rs) throws Exception {
                    if (shouldSample) {
                        latencyContainer.insertEndNanos = System.nanoTime();
                    }
                    return Futures.immediateFuture(rs);
                }
            });
        }

        // Convert ResultSetFuture to CompletableFuture
        CompletableFuture<ResultSet> futCompletable = new CompletableFuture<>();
        Futures.addCallback(fut, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet result) {
                if (shouldSample) {
                    if (latencyContainer.selectStartNanos != -1 && latencyContainer.selectEndNanos != -1) {
                        selectLatencies.add(latencyContainer.selectEndNanos - latencyContainer.selectStartNanos);
                    }
                    if (latencyContainer.insertStartNanos != -1 && latencyContainer.insertEndNanos != -1) {
                        insertLatencies.add(latencyContainer.insertEndNanos - latencyContainer.insertStartNanos);
                    }
                }

                futCompletable.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                futCompletable.completeExceptionally(t);
            }
        });

        // Execute next iteration after that
        return futCompletable.thenCompose(rs -> execute(s, currentIter + 1, maxIter));
    }
}


