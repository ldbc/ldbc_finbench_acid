
import com.graphdbapi.driver.Graph;
import com.graphdbapi.driver.GraphDb;
import com.graphdbapi.driver.GraphTransaction;
import com.graphdbapi.driver.Isolation;
import com.graphdbapi.driver.v1.Driver;
import com.graphdbapi.driver.v1.Record;
import com.graphdbapi.driver.v1.StatementResult;
import com.graphdbapi.driver.v1.Value;
import com.graphdbapi.driver.v1.graph.PropertyType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class Acid {
    // vertex
    public static final String ACCOUNT = "Account";
    // edge
    public static final String TRANSFER = "transfer";
    // property
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String BALANCE = "balance";
    public static final String NUM_TRANSFERRED = "numTransferred";
    public static final String TRANS_HISTORY = "transHistory";
    public static final String VERSION_HISTORY = "versionHistory";
    public static final String AMOUNT = "amount";

    private final Graph graph;
    private final Driver driver;
    private final ExecutorService executorService;

    public Acid() {
        executorService = Executors.newFixedThreadPool(8);
        driver = GraphDb.connect("bolt://127.0.0.1:7687", "admin", "admin");
        Graph graph;
        try {
            graph = GraphDb.driverByName(driver, "acid");
            graph.deleteGraph();
        } catch (Exception e) {
            System.out.println("graph exist");
        } finally {
            graph = GraphDb.newGraph(driver, "acid");
            this.graph = graph;
        }
    }

    private void initSchema() {
        Map<String, PropertyType> accountMap = new HashMap<>();
        accountMap.put(NAME, PropertyType.STRING);
        accountMap.put(BALANCE, PropertyType.DOUBLE);
        accountMap.put(NUM_TRANSFERRED, PropertyType.LONG);
        accountMap.put(TRANS_HISTORY, PropertyType.STRING);
        accountMap.put(VERSION_HISTORY, PropertyType.STRING);
        graph.schema().createVertexType(ACCOUNT, ID, accountMap);

        Map<String, PropertyType> transferMap = new HashMap<>();
        transferMap.put(AMOUNT, PropertyType.DOUBLE);
        transferMap.put(VERSION_HISTORY, PropertyType.STRING);
        graph.schema().createEdgeType(TRANSFER, ACCOUNT, ACCOUNT, true, true,  null, transferMap);
    }

    private void transaction(Consumer<GraphTransaction> consumer) {
        GraphTransaction tx = graph.beginTransaction(Isolation.SERIALIZE);
        try {
            consumer.accept(tx);
            tx.success();
        } catch (Exception e) {
            tx.failure();
            e.printStackTrace();
        } finally {
            tx.close();
        }
    }

    private Map<String, Object> transaction(Function<GraphTransaction, Map<String, Object>> function) {
        GraphTransaction tx = graph.beginTransaction(Isolation.SERIALIZE);
        try {
            Map<String, Object> apply = function.apply(tx);
            tx.success();
            return apply;
        } catch (Exception e) {
            tx.failure();
            e.printStackTrace();
        } finally {
            tx.close();
        }
        return null;
    }

    private Boolean transaction0(Function<GraphTransaction, Boolean> function) {
        GraphTransaction tx = graph.beginTransaction(Isolation.SERIALIZE);
        try {
            Boolean apply = function.apply(tx);
            if (apply) {
                tx.success();
            } else {
                tx.failure();
            }
            return apply;
        } catch (Exception e) {
            tx.failure();
            e.printStackTrace();
        } finally {
            tx.close();
        }
        return null;
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Atomicity

    private void atomicityInit() {
        transaction(e -> {
            e.executeQuery(
                "CREATE (:Account {id: '1', name: 'AliceAcc', transHistory: '100'}),\n" +
                    " (:Account {id: '2', name: 'Bob', transHistory: '50,150'})"
            ).list();
        });
    }

    private void atomicityC(Map<String, Object> parameters) {
        transaction(e -> {
            e.executeQuery(String.format(
                "MATCH (a1:Account {id: '%s'})\n" +
                    "CREATE (a2:Account {id: '%s'})\n" +
                    "CREATE (a1)-[t:transfer]->(a2)\n" +
                    "SET\n" +
                    "  a1.transHistory = a1.transHistory + ',' + '%s',\n" +
                    " t.amount = %s"
                , parameters.get("account1Id"), parameters.get("account2Id"), parameters.get("newTrans"), parameters.get("newTrans"))).list();

        });
    }

    private Boolean atomicityRB(Map<String, Object> parameters) {
        return transaction0(e -> {
            e.executeQuery(String.format("MATCH (a1:Account {id: '%s'}) SET a1.transHistory = a1.transHistory + ',' + '%s'", parameters.get("account1Id"), parameters.get("newTransHistory"))).list();
            if (e.executeQuery(String.format("MATCH (a2:Account {id: '%s'}) RETURN a2", parameters.get("account2Id"))).hasNext()) {
                return false;
            } else {
                e.executeQuery(String.format("CREATE (a2:Account {id: '%s'})", parameters.get("account2Id")));
            }
            return true;
        });
    }

    private Map<String, Object> atomicityCheck() {
        return transaction((e -> {
            Record record = e.executeQuery("MATCH (a:Account)\n RETURN count(a) AS numAccounts, count(a.name) AS numNames, sum(size(split(a.transHistory,','))) AS numTransferred").next();
            Map<String, Object> map = new HashMap<>();
            map.put("numAccounts", record.get("numAccounts").asLong());
            map.put("numNames", record.get("numNames").asLong());
            map.put("numTransferred", record.get("numTransferred").asLong());
            return map;
        }));
    }

    private void atomicityCTest() {
        atomicityInit();

        final int nTransactions = 50;
        int aborted = 0;

        Map<String, Object> committed = new HashMap<>(atomicityCheck());
        Map<String, Object> parameters = new HashMap<>();

        for (int i = 0; i < nTransactions; i++) {
            parameters.put("account1Id", 1L);
            parameters.put("account2Id", 3 + i);
            parameters.put("newTrans", 200 + i);
            try {
                atomicityC(parameters);
                committed.put("numAccounts", (long) committed.get("numAccounts") + 1);
                committed.put("numTransferred", (long) committed.get("numTransferred") + 1);
            } catch (Exception e) {
                aborted++;
            }
        }

        Map<String, Object> results = atomicityCheck();

        boolean flag = true;
        for (Map.Entry<String, Object> entry : committed.entrySet()) {
            if ((long) results.get(entry.getKey()) != (long) entry.getValue()) {
                flag = false;
                break;
            }
        }

        System.out.printf("Number of aborted Transaction: %s%n", aborted);
        System.out.println(flag ? "AtomicityCTest passed" : "AtomicityCTest failed");
    }

    private void atomicityRBTest() {
        atomicityInit();

        final int nTransactions = 50;
        int aborted = 0;

        Map<String, Object> committed = new HashMap<>(atomicityCheck());
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("account1Id", 1L);
        parameters.put("newTrans", 200L);

        for (int i = 0; i < nTransactions; i++) {
            parameters.put("account2Id", i % 2 == 0 ? 2 : 3 + i);
            boolean successful;
            try {
                successful = atomicityRB(parameters);
            } catch (Exception e) {
                successful = false;
            }
            if (successful) {
                committed.put("numAccounts", (long) committed.get("numAccounts") + 1);
                committed.put("numTransferred", (long) committed.get("numTransferred") + 1);
            } else {
                aborted++;
            }
        }

        Map<String, Object> results = atomicityCheck();

        boolean flag = true;
        for (Map.Entry<String, Object> entry : committed.entrySet()) {
            if ((long) results.get(entry.getKey()) != (long) entry.getValue()) {
                flag = false;
                break;
            }
        }

        System.out.printf("Number of aborted Transaction: %s%n", aborted);
        System.out.println(flag ? "AtomicityRBTest passed" : "AtomicityRBTest failed");
    }

    // G0(Dirty Write)

    private void g0Init() {
        transaction(e -> {
            e.executeQuery(
                "CREATE (:Account {id: '1', versionHistory: '0'})-[:transfer {versionHistory: '0'}]->(:Account {id: '2', versionHistory: '0'})"
            ).list();
        });
    }

    private void g0(Map<String, Object> parameters) {
        transaction(e -> {
            e.executeQuery(String.format(
                "MATCH (a1:Account {id: '%s'})-[t:transfer]->(a2:Account {id: '%s'})\n" +
                    "SET a1.versionHistory = a1.versionHistory + ',' + '%s'\n" +
                    "SET a2.versionHistory = a2.versionHistory + ',' + '%<s'\n" +
                    "SET t.versionHistory  = t.versionHistory + ',' + '%<s'"
                , parameters.get("account1Id"), parameters.get("account2Id"), parameters.get("transactionId"))).list();
        });
    }

    private Map<String, Object> g0check(Map<String, Object> parameters) {
        return transaction(e -> {
            Record record = e.executeQuery(String.format(
                "MATCH (a1:Account {id: '%s'})-[t:transfer]->(a2:Account {id: '%s'})\n" +
                    "RETURN\n" +
                    "  a1.versionHistory AS a1VersionHistory,\n" +
                    "  t.versionHistory  AS tVersionHistory,\n" +
                    "  a2.versionHistory AS a2VersionHistory"
                , parameters.get("account1Id"), parameters.get("account2Id"))).next();
            Map<String, Object> map = new HashMap<>();
            map.put("a1VersionHistory", record.get("a1VersionHistory").asString());
            map.put("tVersionHistory", record.get("tVersionHistory").asString());
            map.put("a2VersionHistory", record.get("a2VersionHistory").asString());
            return map;
        });
    }

    private void g0Test() {
        g0Init();
        final int transactionNumber = 200;
        int aborted = 0;

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("account1Id", 1);
        parameters.put("account2Id", 2);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 1; i <= transactionNumber; i++) {
            parameters.put("transactionId", i);
            Future<?> future = executorService.submit(() -> {
                g0(parameters);
            });
            futures.add(future);
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                aborted++;
            }
        }

        Map<String, Object> result = g0check(parameters);
        String p1 = (String) result.get("a1VersionHistory");
        String k = (String) result.get("tVersionHistory");
        String p2 = (String) result.get("a2VersionHistory");

        System.out.printf("G0Test: Number of aborted transactions: %d\n", aborted);
        System.out.println(p1.equals(p2) && p1.equals(k) ? "G0Test passed" : "G0Test failed");
    }

    // G1a(Aborted Reads)

    private void g1aInit() {
        transaction(e -> {
            e.executeQuery(
                "CREATE (:Account {id: '1', balance: 99.0})"
            ).list();
        });
    }

    private Boolean g1aW(Map<String, Object> parameters) {
        return transaction0(e -> {
            StatementResult result = e.executeQuery(String.format(
                "MATCH (a:Account {id: '%s'})\n" +
                    "RETURN ID(a) AS internalAId"
                , parameters.get("accountId")));
            if (!result.hasNext()) {
                throw new IllegalStateException("G1a1 StatementResult empty");
            }
            Record record = result.next();
            Value internalAId = record.get("internalAId");

            sleep((Long) parameters.get("sleepTime"));
            e.executeQuery(String.format(
                "MATCH (a:Account)\n" +
                    "WHERE ID(a) = '%s'\n" +
                    "SET a.balance = 200"
                , internalAId)).list();
            sleep((Long) parameters.get("sleepTime"));
            return false;
        });
    }

    private Map<String, Object> g1aR(Map<String, Object> parameters) {
        return transaction(e -> {
            StatementResult result = e.executeQuery(String.format(
                "MATCH (a:Account {id: '%s'}) RETURN a.balance AS aBalance"
                , parameters.get("accountId")));
            if (!result.hasNext()) {
                throw new IllegalStateException("G1a T2 StatementResult empty");
            }
            Record record = result.next();
            Map<String, Object> map = new HashMap<>();
            map.put("aBalance", record.get("aBalance").asDouble());
            return map;
        });
    }

    private void g1aTest() {
        g1aInit();
        final int wc = 5;
        final int rc = 5;
        AtomicInteger abortedW = new AtomicInteger();
        AtomicInteger numAnomaly = new AtomicInteger();

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("accountId", 1);
        parameters.put("sleepTime", 250L);
        double expected = (double) g1aR(parameters).get("aBalance");

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < wc; i++) {
            futures.add(executorService.submit(() -> {
                boolean successful = g1aW(parameters);
                if (!successful) {
                    abortedW.getAndIncrement();
                }
            }));
        }

        for (int i = 0; i < rc; i++) {
            futures.add(executorService.submit(() -> {
                Map<String, Object> result = g1aR(parameters);
                double aBalance = (double) result.get("aBalance");
                if (aBalance != expected) {
                    numAnomaly.getAndIncrement();
                }
            }));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.printf("Number of aborted Transaction: %s%n", abortedW.get());
        System.out.println(numAnomaly.get() == 0 ? "G1ATest passed" : "G1ATest failed");
    }

    // G1b(Intermediate Reads)

    private void g1bInit() {
        transaction(e -> {
            e.executeQuery(
                "CREATE (:Account {id: '1', balance: 99.0})"
            ).list();
        });
    }

    private void g1bW(Map<String, Object> parameters) {
        transaction(e -> {
            e.executeQuery(String.format(
                "MATCH (a:Account {id: '%s'}) SET a.balance = %s"
                , parameters.get("accountId"), parameters.get("even"))).list();
            sleep((Long) parameters.get("sleepTime"));
            e.executeQuery(String.format(
                "MATCH (a:Account {id: '%s'}) SET a.balance = %s"
                , parameters.get("accountId"), parameters.get("odd"))).list();
        });
    }

    private Map<String, Object> g1bR(Map<String, Object> parameters) {
        return transaction(e -> {
            StatementResult result = e.executeQuery(String.format(
                "MATCH (a:Account {id: '%s'}) RETURN a.balance AS aBalance"
                , parameters.get("accountId")));
            if (!result.hasNext()) {
                throw new IllegalStateException("G1b T2 StatementResult empty");
            }
            Record record = result.next();
            Map<String, Object> map = new HashMap<>();
            map.put("aBalance", record.get("aBalance").asDouble());
            return map;
        });
    }

    private void g1bTest() {
        g1bInit();
        final int wc = 20;
        final int rc = 20;
        int aborted = 0;
        AtomicInteger numAnomaly = new AtomicInteger();

        final long odd = 99;
        final long even = 200;
        List<Future<?>> futures = new ArrayList<>();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("accountId", 1);
        parameters.put("sleepTime", 250L);
        parameters.put("even", even);
        parameters.put("odd", odd);
        for (int i = 0; i < wc; i++) {
            futures.add(executorService.submit(() -> {
                g1bW(parameters);
            }));
        }

        for (int i = 0; i < rc; i++) {
            futures.add(executorService.submit(() -> {
                Map<String, Object> result = g1bR(parameters);
                double aBalance = (double) result.get("aBalance");
                if (aBalance % 2 != 1) {
                    numAnomaly.getAndIncrement();
                }
            }));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                aborted++;
            }
        }
        System.out.printf("Number of aborted Transaction: %s%n", aborted);
        System.out.println(numAnomaly.get() == 0 ? "G1BTest passed" : "G1BTest failed");
    }

    // G1c(Circular Information Flow)

    private void g1cInit() {
        transaction(e -> {
            e.executeQuery(
                "CREATE (:Account {id: '1', balance: 0}), (:Account {id: '2', balance: 0})"
            ).list();
        });
    }

    private Map<String, Object> g1c(Map<String, Object> parameters) {
        return transaction(e -> {
            Record record = e.executeQuery(String.format(
                "MATCH (a1:Account {id: '%s'})\n" +
                    "SET a1.balance = %s\n" +
                    "WITH count(*) AS dummy\n" +
                    "MATCH (a2:Account {id: '%s'})\n" +
                    "RETURN a2.balance AS account2Balance\n"
                , parameters.get("account1Id"), parameters.get("transactionId"), parameters.get("account2Id"))).next();
            Map<String, Object> map = new HashMap<>();
            map.put("account2Balance", record.get("account2Balance").asDouble());
            return map;
        });
    }

    private void g1cTest() {
        g1cInit();
        final int c = 100;
        int aborted = 0;
        int numAnomaly = 0;
        Random random = new Random();
        double[] result = new double[c + 1];

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 1; i <= c; i++) {
            final boolean order = random.nextBoolean();
            long account1Id = order ? 1L : 2L;
            long account2Id = order ? 2L : 1L;
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("transactionId", i);
            parameters.put("account1Id", account1Id);
            parameters.put("account2Id", account2Id);
            int finalI = i;
            futures.add(executorService.submit(() -> {
                try {
                    result[finalI] = (double) g1c(parameters).get("account2Balance");
                } catch (Exception e) {
                    result[finalI] = -1;
                }
            }));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (int i = 1; i <= c; i++) {
            double account2Balance1 = result[i - 1];
            if (account2Balance1 == -1) {
                aborted++;
            }
            if (account2Balance1 == 0) {
                continue;
            }
            double account2Balance2 = result[(int) (account2Balance1 - 1)];
            if (account2Balance2 == -1 || i == account2Balance2) {
                numAnomaly++;
            }
        }
        System.out.printf("Number of aborted Transaction: %s%n", aborted);
        System.out.println(numAnomaly == 0 ? "G1CTest passed" : "G1CTest failed");

    }

    // IMP(Item-Many-Preceders)

    private void impInit() {
        transaction(e -> {
            e.executeQuery(
                "CREATE (:Account {id: '1', balance: 1})"
            ).list();
        });
    }

    private void impW(Map<String, Object> parameters) {
        transaction(e -> {
            e.executeQuery(String.format(
                "MATCH (a:Account {id:'%s'}) SET a.balance = a.balance + 1 RETURN a"
                , parameters.get("accountId"))).list();
        });
    }

    private Map<String, Object> impR(Map<String, Object> parameters) {
        return transaction(e -> {
            StatementResult result1 = e.executeQuery(String.format(
                "MATCH (a:Account {id: '%s'}) RETURN a.balance AS firstRead"
                , parameters.get("accountId")));
            if (!result1.hasNext()) {
                throw new IllegalStateException("IMP result1 empty");
            }
            double firstRead = result1.next().get("firstRead").asDouble();
            sleep((Long) parameters.get("sleepTime"));

            StatementResult result2 = e.executeQuery(String.format(
                "MATCH (a:Account {id: '%s'}) RETURN a.balance AS secondRead"
                , parameters.get("accountId")));
            if (!result2.hasNext()) {
                throw new IllegalStateException("IMP result2 empty");
            }
            double secondRead = result2.next().get("secondRead").asDouble();

            Map<String, Object> map = new HashMap<>();
            map.put("firstRead", firstRead);
            map.put("secondRead", secondRead);
            return map;
        });
    }

    private void impTest() {
        impInit();
        final int c = 20;

        AtomicInteger aborted = new AtomicInteger();
        AtomicInteger numAnomaly = new AtomicInteger();

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("accountId", 1);
        parameters.put("sleepTime", 250L);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 1; i <= c; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    impW(parameters);
                } catch (Exception e) {
                    aborted.getAndIncrement();
                }
            }));
        }
        for (int i = 1; i <= c; i++) {
            futures.add(executorService.submit(() -> {
                Map<String, Object> result = impR(parameters);
                double firstRead = (double) result.get("firstRead");
                double secondRead = (double) result.get("secondRead");
                if (firstRead != secondRead) {
                    numAnomaly.getAndIncrement();
                }
            }));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted Transaction: %s%n", aborted.get());
        System.out.println(numAnomaly.get() == 0 ? "IMPTest passed" : "IMPTest failed");
    }

    // PMP(Predicate-Many-Preceders)

    private void pmpInit() {
        transaction(e -> {
            e.executeQuery(
                "CREATE (:Account {id: '1'}), (:Account {id: '2'})"
            ).list();
        });
    }

    private void pmpW(Map<String, Object> parameters) {
        transaction(e -> {
            e.executeQuery(String.format(
                "MATCH (a1:Account {id: '%s'}), (a2:Account {id: '%s'}) CREATE (a1)-[:transfer]->(a2)"
                , parameters.get("account1Id"), parameters.get("account2Id"))).list();
        });
    }

    private Map<String, Object> pmpR(Map<String, Object> parameters) {
        return transaction(e -> {
            StatementResult result1 = e.executeQuery(String.format(
                "MATCH (a2:Account {id: '%s'})<-[:transfer]-(a1:Account) RETURN count(a1) AS firstRead"
                , parameters.get("account2Id")));
            if (!result1.hasNext()) {
                throw new IllegalStateException("PMP result1 empty");
            }
            double firstRead = result1.next().get("firstRead").asDouble();
            sleep((Long) parameters.get("sleepTime"));

            StatementResult result2 = e.executeQuery(String.format(
                "MATCH (a2:Account {id: '%s'})<-[:transfer]-(a3:Account) RETURN count(a3) AS secondRead"
                , parameters.get("account2Id")));
            if (!result2.hasNext()) {
                throw new IllegalStateException("PMP result2 empty");
            }
            double secondRead = result2.next().get("secondRead").asDouble();

            Map<String, Object> map = new HashMap<>();
            map.put("firstRead", firstRead);
            map.put("secondRead", secondRead);
            return map;
        });
    }

    private void pmpTest() {
        pmpInit();
        final int c = 20;

        AtomicInteger aborted = new AtomicInteger();
        AtomicInteger numAnomaly = new AtomicInteger();

        List<Future<?>> futures = new ArrayList<>();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("account1Id", 1);
        parameters.put("account2Id", 2);
        parameters.put("sleepTime", 250L);
        for (int i = 0; i < c; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    pmpW(parameters);
                } catch (Exception e) {
                    aborted.getAndIncrement();
                }
            }));
        }
        for (int i = 0; i < c; i++) {
            futures.add(executorService.submit(() -> {
                Map<String, Object> result = pmpR(parameters);
                double firstRead = (double) result.get("firstRead");
                double secondRead = (double) result.get("secondRead");
                if (firstRead != secondRead) {
                    numAnomaly.getAndIncrement();
                }
            }));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted Transaction: %s%n", aborted.get());
        System.out.println(numAnomaly.get() == 0 ? "PMPTest passed" : "PMPTest failed");
    }

    // OTV(Observed Transaction Vanishes)

    private void otvInit() {
        transaction(e -> {
            e.executeQuery(
                "CREATE (a1:Account {id: '1', balance: 0})-[:transfer]->"
                    + "  (:Account {id: '2', balance: 0})-[:transfer]->"
                    + "  (:Account {id: '3', balance: 0})-[:transfer]->"
                    + "  (:Account {id: '4', balance: 0})-[:transfer]->(a1)"
            ).list();
        });
    }

    private void otvW(Map<String, Object> parameters) {
        transaction(e -> {
            Random random = new Random();
            for (int i = 0; i < 100; i++) {
                long accountId  = random.nextInt((int) parameters.get("cycleSize") + 1);
                e.executeQuery(String.format(
                    "MATCH path = (a1:Account {id: '%s'})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(a1)\n"
                        + " SET a1.balance = a1.balance + 1\n"
                        + " SET a2.balance = a2.balance + 1\n"
                        + " SET a3.balance = a3.balance + 1\n"
                        + " SET a4.balance = a4.balance + 1\n"
                    , accountId)).list();
            }
        });
    }

    private Map<String, Object> otvR(Map<String, Object> parameters) {
        return transaction(e -> {
            StatementResult result1 = e.executeQuery(String.format(
                "MATCH (a1:Account {id: '%s'})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(p1)\n"
                    + "RETURN [a1.balance, a2.balance, a3.balance, a4.balance] AS firstRead"
                , parameters.get("accountId")));
            if (!result1.hasNext()) {
                throw new IllegalStateException("OTV2 result1 empty");
            }
            List<Object> firstRead = result1.next().get("firstRead").asList();
            sleep((Long) parameters.get("sleepTime"));

            StatementResult result2 = e.executeQuery(String.format(
                "MATCH (a1:Account {id: '%s'})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(a1)\n"
                    + "RETURN [a1.balance, a2.balance, a3.balance, a4.balance] AS secondRead"
                , parameters.get("accountId")));
            if (!result2.hasNext()) {
                throw new IllegalStateException("OTV2 result2 empty");
            }
            List<Object> secondRead = result2.next().get("secondRead").asList();

            Map<String, Object> map = new HashMap<>();
            map.put("firstRead", firstRead);
            map.put("secondRead", secondRead);
            return map;
        });
    }

    private void otvTest() {
        otvInit();
        final int rc = 50;

        AtomicInteger aborted = new AtomicInteger();
        AtomicInteger numAnomaly = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("cycleSize", 4);
        futures.add(executorService.submit(() -> {
            try {
                otvW(parameters);
            } catch (Exception e) {
                aborted.getAndIncrement();
            }
        }));
        Random random = new Random();
        for (int i = 0; i < rc; i++) {
            futures.add(executorService.submit(() -> {
                Map<String, Object> readParameters = new HashMap<>();
                readParameters.put("accountId", random.nextInt(4) + 1);
                readParameters.put("sleepTime", 250L);
                Map<String, Object> result = otvR(readParameters);
                double balance1Max = Collections.max((List<Double>) result.get("firstRead"));
                double balance2Min = Collections.min((List<Double>) result.get("secondRead"));
                if (balance1Max != balance2Min) {
                    numAnomaly.getAndIncrement();
                }
            }));
        }

        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted Transaction: %s%n", aborted.get());
        System.out.println(numAnomaly.get() == 0 ? "OTVTest passed" : "OTVTest failed");
    }

    // FR(Fractured Reads)

    private void frInit() {
        transaction(e -> {
            e.executeQuery(
                "CREATE (a1:Account {id: '1', balance: 0})-[:transfer]->"
                    + "  (:Account {id: '2', balance: 0})-[:transfer]->"
                    + "  (:Account {id: '3', balance: 0})-[:transfer]->"
                    + "  (:Account {id: '4', balance: 0})-[:transfer]->(a1)"
            ).list();
        });
    }

    private void frW(Map<String, Object> parameters) {
        transaction(e -> {
            e.executeQuery(String.format(
                "MATCH path = (a1:Account {id: '%s'})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(a1)\n"
                    + " SET a1.balance = a1.balance + 1\n"
                    + " SET a2.balance = a2.balance + 1\n"
                    + " SET a3.balance = a3.balance + 1\n"
                    + " SET a4.balance = a4.balance + 1\n"
                , parameters.get("accountId"))).list();
        });
    }

    private Map<String, Object> frR(Map<String, Object> parameters) {
        return transaction(e -> {
            StatementResult result1 = e.executeQuery(String.format(
                "MATCH (a1:Account {id: '%s'})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(a1)\n"
                    + "RETURN [a1.balance, a2.balance, a3.balance, a4.balance] AS firstRead"
                , parameters.get("accountId")));
            if (!result1.hasNext()) {
                throw new IllegalStateException("FR2 result1 empty");
            }
            List<Object> firstRead = result1.next().get("firstRead").asList();
            sleep((Long) parameters.get("sleepTime"));

            StatementResult result2 = e.executeQuery(String.format(
                "MATCH (a1:Account {id: '%s'})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(a1)\n"
                    + "RETURN [a1.balance, a2.balance, a3.balance, a4.balance] AS secondRead"
                , parameters.get("accountId")));
            if (!result2.hasNext()) {
                throw new IllegalStateException("FR2 result2 empty");
            }
            List<Object> secondRead = result2.next().get("secondRead").asList();

            Map<String, Object> map = new HashMap<>();
            map.put("firstRead", firstRead);
            map.put("secondRead", secondRead);
            return map;
        });
    }

    private void frTest() {
        frInit();
        final int c = 100;
        AtomicInteger aborted = new AtomicInteger();
        AtomicInteger numAnomaly = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("accountId", 1);
        parameters.put("sleepTime", 250L);
        for (int i = 0; i < c; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    frW(parameters);
                } catch (Exception e) {
                    aborted.getAndIncrement();
                }
            }));
        }

        for (int i = 0; i < c; i++) {
            futures.add(executorService.submit(() -> {
                Map<String, Object> result = frR(parameters);
                List<Double> firstRead = (List<Double>) result.get("firstRead");
                List<Double> secondRead = (List<Double>) result.get("secondRead");
                if (!firstRead.equals(secondRead)) {
                    numAnomaly.getAndIncrement();
                }
            }));
        }

        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted Transaction: %s%n", aborted.get());
        System.out.println(numAnomaly.get() == 0 ? "FRTest passed" : "FRTest failed");
    }

    // LU(Lost Updates)

    private void luInit() {
        transaction(e -> {
            e.executeQuery(
                "CREATE (:Account {id: '1', numTransferred: 0})"
            ).list();
        });
    }

    private void luW(Map<String, Object> parameters) {
        transaction(e -> {
            e.executeQuery(String.format(
                "MATCH (a1:Account {id: '%s'})\n" +
                    "CREATE (a2:Account {id: '%s'})\n" +
                    "CREATE (a1)-[:transfer]->(a2)\n" +
                    "SET a1.numTransferred = a1.numTransferred + 1\n" +
                    "RETURN a1.numTransferred\n"
                , parameters.get("account1Id"), parameters.get("account2Id"))).list();
        });
    }

    private Map<String, Object> luR(Map<String, Object> parameters) {
        return transaction(e -> {
            Record record = e.executeQuery(String.format(
                "MATCH (a:Account {id: '%s'})\n" +
                    "OPTIONAL MATCH (a)-[t:transfer]->()\n" +
                    "WITH a, count(t) AS numTransferEdges\n" +
                    "RETURN numTransferEdges,\n" +
                    "       a.numTransferred AS numTransferProp\n"
                , parameters.get("accountId"))).next();
            Map<String, Object> map = new HashMap<>();
            map.put("numTransferEdges", record.get("numTransferEdges").asLong());
            map.put("numTransferProp", record.get("numTransferProp").asDouble());
            return map;
        });
    }

    private void luTest() {
        luInit();

        long transactionNumber = 200;
        AtomicLong aborted = new AtomicLong();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < transactionNumber; i++) {
            int finalI = i;
            futures.add(executorService.submit(() -> {
                try {
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put("account1Id", 1);
                    parameters.put("account2Id", finalI + 2);
                    luW(parameters);
                } catch (Exception e) {
                    aborted.getAndIncrement();
                }
            }));
        }

        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("accountId", 1);
        Map<String, Object> result = luR(parameters);
        long numTransferEdges = (long) result.get("numTransferEdges");
        double numTransferProp = (double) result.get("numTransferProp");

        System.out.printf("Number of aborted Transaction: %s%n", aborted.get());
        if (numTransferEdges == numTransferProp && numTransferProp == transactionNumber - aborted.get()) {
            System.out.println("LUTest passed");
        } else {
            System.out.println("LUTest failed");
        }
    }

    // WS(Write Skews)

    private void wsInit() {
        transaction(e -> {
            for (int i = 1; i <= 10; i++) {
                e.executeQuery(String.format(
                    "CREATE (:Account {id: '%s', balance: 70}), (:Account {id: '%s', balance: 80})"
                    , i * 2 - 1, i * 2)).list();
            }
        });
    }

    private void wsW(Map<String, Object> parameters) {
        transaction(e -> {
            StatementResult result = e.executeQuery(String.format(
                "MATCH (a1:Account {id: '%s'}), (a2:Account {id: '%s'})\n"
                    + "WHERE a1.balance + a2.balance >= 100\n"
                    + "RETURN a1, a2"
                , parameters.get("account1Id"), parameters.get("account2Id")));
            if (result.hasNext()) {
                sleep((Long) parameters.get("sleepTime"));
                long accountId = new Random().nextBoolean() ?
                    Long.parseLong(parameters.get("account1Id").toString()) :
                    Long.parseLong(parameters.get("account2Id").toString());
                e.executeQuery(String.format(
                    "MATCH (a:Account {id: '%s'})\n" +
                        "SET a.balance = a.balance - 100"
                    , accountId)).list();
            }
        });
    }

    private Map<String, Object> wsR(Map<String, Object> parameters) {
        return transaction(e -> {
            StatementResult result = e.executeQuery("MATCH (a1:Account), (a2:Account {id: a1.id+1})\n"
                + "WHERE a1.balance + a2.balance <= 0 and toInteger(a1.id) % 2 = 1 \n"
                + "RETURN a1.id AS a1id, a1.balance AS a1balance, a2.id AS a2id, a2.balance AS a2balance"
            );
            if (result.hasNext()) {
                Record record = result.next();
                Map<String, Object> map = new HashMap<>();
                map.put("a1id", record.get("a1id").asString());
                map.put("a1balance", record.get("a1balance").asDouble());
                map.put("a2id", record.get("a2id").asString());
                map.put("a2balance", record.get("a2balance").asDouble());
                return map;
            } else {
                return null;
            }
        });
    }

    private void wsTest() {
        wsInit();
        int wc = 50;
        int numAccountPairs = 10;
        Random random = new Random();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("sleepTime", 250L);
        AtomicLong aborted = new AtomicLong();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < wc; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    long accountId = random.nextInt(numAccountPairs) * 2 + 1;
                    parameters.put("account1Id", accountId);
                    parameters.put("account2Id", accountId + 1);
                    wsW(parameters);
                } catch (Exception e) {
                    aborted.getAndIncrement();
                }
            }));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Map<String, Object> result = wsR(null);
        System.out.printf("Number of aborted Transaction: %s%n", aborted);
        if (result == null) {
            System.out.println("WSTest passed");
        } else {
            System.out.println("WSTest failed");
        }
    }

    // temporary test

    public static void main(String[] args) throws Exception {
        Acid boltDriver = new Acid();
        boltDriver.initSchema();
        System.out.println("start acid");
        try {
            System.out.println("\n======>atomicityCTest<======");
            boltDriver.atomicityCTest();
            boltDriver.graph.clearGraph();

            System.out.println("\n======>atomicityRBTest<======");
            boltDriver.atomicityRBTest();
            boltDriver.graph.clearGraph();

            System.out.println("\n======>g0Test<======");
            boltDriver.g0Test();
            boltDriver.graph.clearGraph();

            System.out.println("\n======>g1aTest<======");
            boltDriver.g1aTest();
            boltDriver.graph.clearGraph();

            System.out.println("\n======>g1bTest<======");
            boltDriver.g1bTest();
            boltDriver.graph.clearGraph();

            System.out.println("\n======>g1cTest<======");
            boltDriver.g1cTest();
            boltDriver.graph.clearGraph();

            System.out.println("\n======>impTest<======");
            boltDriver.impTest();
            boltDriver.graph.clearGraph();

            System.out.println("\n======>pmpTest<======");
            boltDriver.pmpTest();
            boltDriver.graph.clearGraph();

            System.out.println("\n======>otvTest<======");
            boltDriver.otvTest();
            boltDriver.graph.clearGraph();

            System.out.println("\n======>frTest<======");
            boltDriver.frTest();
            boltDriver.graph.clearGraph();

            System.out.println("\n======>luTest<======");
            boltDriver.luTest();
            boltDriver.graph.clearGraph();

            System.out.println("\n======>wsTest<======");
            boltDriver.wsTest();
            boltDriver.graph.clearGraph();
        } finally {
            boltDriver.driver.close();
            boltDriver.executorService.shutdown();
            System.out.println("finish acid");
        }
    }
}

