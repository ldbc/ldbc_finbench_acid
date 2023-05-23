package ldbc.finbench.acid;

import com.google.common.collect.ImmutableMap;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import ldbc.finbench.acid.driver.TestDriver;
import ldbc.finbench.acid.transactions.TransactionThread;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class AcidTest<TTestDriver extends TestDriver> {

    protected TTestDriver testDriver;
    protected ExecutorService executorService = Executors.newFixedThreadPool(8);
    boolean printStackTrace = true;

    public AcidTest(TTestDriver testDriver) {
        this.testDriver = testDriver;
    }

    @Before
    public void initialize() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        testDriver.nukeDatabase();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void printStackTrace(Exception e) {
        if (printStackTrace) {
            e.printStackTrace();
        }
    }

    @Test
    public void atomicityCTest() throws Exception {
        testDriver.atomicityInit();

        final int nTransactions = 50;
        int aborted = 0;

        Map<String, Object> committed = new HashMap<>(testDriver.atomicityCheck());
        Map<String, Object> parameters = new HashMap<>();

        for (int i = 0; i < nTransactions; i++) {
            parameters.put("account1Id", 1L);
            parameters.put("account2Id", 3 + i);
            parameters.put("newTrans", 200 + i);
            try {
                testDriver.atomicityC(parameters);
                committed.put("numAccounts", (long) committed.get("numAccounts") + 1);
                committed.put("numTransferred", (long) committed.get("numTransferred") + 1);
            } catch (Exception e) {
                aborted++;
                printStackTrace(e);
            }
        }
        System.out.printf("AtomicityCTest: Number of aborted transactions: %d\n", aborted);

        Map<String, Object> results = testDriver.atomicityCheck();
        Assert.assertEquals((long) committed.get("numAccounts"), (long) results.get("numAccounts"));
        Assert.assertEquals((long) committed.get("numNames"), (long) results.get("numNames"));
        Assert.assertEquals((long) committed.get("numTransferred"), (long) results.get("numTransferred"));
        Assert.assertTrue(aborted != nTransactions);
        System.out.println("AtomicityCTest passed");
    }

    @Test
    public void atomicityRbTest() throws Exception {
        testDriver.atomicityInit();

        final int nTransactions = 50;
        int aborted = 0;

        Map<String, Object> committed = new HashMap<>(testDriver.atomicityCheck());
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("account1Id", 1L);
        parameters.put("newTrans", 200L);

        for (int i = 0; i < nTransactions; i++) {
            parameters.put("account2Id", i % 2 == 0 ? 2 : 3 + i);
            boolean successful;
            try {
                successful = testDriver.atomicityRB(parameters);
            } catch (Exception e) {
                printStackTrace(e);
                successful = false;
            }
            if (successful) {
                committed.put("numAccounts", (long) committed.get("numAccounts") + 1);
                committed.put("numTransferred", (long) committed.get("numTransferred") + 1);
            } else {
                aborted++;
            }
        }

        System.out.printf("AtomicityRbTest: Number of aborted transactions: %d\n", aborted);

        Map<String, Object> results = testDriver.atomicityCheck();
        Assert.assertEquals((long) committed.get("numAccounts"), (long) results.get("numAccounts"));
        Assert.assertEquals((long) committed.get("numNames"), (long) results.get("numNames"));
        Assert.assertEquals((long) committed.get("numTransferred"), (long) results.get("numTransferred"));
        Assert.assertTrue(aborted != nTransactions);
        System.out.println("AtomicityRbTest passed");
    }

    @Test
    public void g0Test() throws Exception {
        testDriver.g0Init();
        final int wc = 200;
        int aborted = 0;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 1; i <= wc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::g0,
                    ImmutableMap.of("account1Id", 1L, "account2Id", 2L, "transactionId", i)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Object>> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                aborted++;
                printStackTrace(e);
            }
        }
        System.out.printf("G0Test: Number of aborted transactions: %d\n", aborted);

        Map<String, Object> results = testDriver.g0check(ImmutableMap.of("account1Id", 1L, "account2Id", 2L));
        if (results.containsKey("a1VersionHistory")) {
            final List<Long> a1VersionHistory = new ArrayList<>((List<Long>) results.get("a1VersionHistory"));
            final List<Long> tVersionHistory = new ArrayList<>((List<Long>) results.get("tVersionHistory"));
            final List<Long> a2VersionHistory = new ArrayList<>((List<Long>) results.get("a2VersionHistory"));

            a1VersionHistory.retainAll(tVersionHistory);
            a1VersionHistory.retainAll(a2VersionHistory);

            tVersionHistory.retainAll(a1VersionHistory);
            tVersionHistory.retainAll(a2VersionHistory);

            a2VersionHistory.retainAll(a1VersionHistory);
            a2VersionHistory.retainAll(tVersionHistory);

            System.out.printf("G0Test:    %s %s %s %5b %5b %5b\n",
                    a1VersionHistory, tVersionHistory, a2VersionHistory,
                    a1VersionHistory.equals(tVersionHistory),
                    a1VersionHistory.equals(a2VersionHistory),
                    tVersionHistory.equals(a1VersionHistory));
            Assert.assertEquals(a1VersionHistory, tVersionHistory);
            Assert.assertEquals(a1VersionHistory, a2VersionHistory);
            Assert.assertEquals(tVersionHistory, a1VersionHistory);
        }
        Assert.assertTrue(aborted != wc);
        System.out.println("G0Test passed");
    }

    @Test
    public void g1aTest() throws Exception {
        testDriver.g1aInit();
        final int wc = 5;
        final int rc = 5;
        int abortedW = 0;
        int abortedR = 0;
        int numAnomaly = 0;

        long expected = (long) testDriver.g1aR(ImmutableMap.of("accountId", 1L)).get("aBalance");

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < wc; i++) {
            clients.add(
                    new TransactionThread<>(i, testDriver::g1aW, ImmutableMap.of("accountId", 1L, "sleepTime", 250L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::g1aR, ImmutableMap.of("accountId", 1L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);

        for (int i = 0; i < wc; i++) {
            try {
                final Map<String, Object> results = futures.get(i).get();
            } catch (Exception e) {
                abortedW++;
                printStackTrace(e);
            }
        }
        for (int i = wc; i < wc + rc; i++) {
            try {
                final Map<String, Object> results = futures.get(i).get();
                final long aBalance = (long) results.get("aBalance");
                System.out.printf("G1aTest:   %4d %4d %5b\n", expected, aBalance, expected == aBalance);
                if (expected != aBalance) {
                    numAnomaly++;
                }
            } catch (Exception e) {
                abortedR++;
                printStackTrace(e);
            }
        }

        System.out.printf("G1aTest: Number of aborted transactions: %d %d\n", abortedW, abortedR);
        Assert.assertEquals(0, numAnomaly);
        Assert.assertTrue(abortedW != wc);
        Assert.assertTrue(abortedR != rc);
        System.out.println("G1aTest passed");
    }

    @Test
    public void g1bTest() throws Exception {
        testDriver.g1bInit();
        final int wc = 20;
        final int rc = 20;
        int abortedW = 0;
        int abortedR = 0;
        int numAnomaly = 0;

        final long odd = 99;
        final long even = 200;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < wc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::g1bW,
                    ImmutableMap.of("accountId", 1L, "even", even, "odd", odd, "sleepTime", 1L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::g1bR, ImmutableMap.of("accountId", 1L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);

        for (int i = 0; i < wc; i++) {
            try {
                final Map<String, Object> results = futures.get(i).get();
            } catch (Exception e) {
                abortedW++;
                printStackTrace(e);
            }
        }
        for (int i = wc; i < wc + rc; i++) {
            try {
                final Map<String, Object> results = futures.get(i).get();
                final long aBalance = (long) results.get("aBalance");
                System.out.printf("G1bTest:   %4d %4d %5b\n", odd, aBalance, aBalance % 2 == 1);
                if (aBalance % 2 != 1) {
                    numAnomaly++;
                }
            } catch (Exception e) {
                abortedR++;
                printStackTrace(e);
            }
        }

        System.out.printf("G1bTest: Number of aborted transactions: %d %d\n", abortedW, abortedR);
        Assert.assertEquals(0, numAnomaly);
        Assert.assertTrue(abortedW != wc);
        Assert.assertTrue(abortedR != rc);
        System.out.println("G1bTest passed");
    }

    @Test
    public void g1cTest() throws Exception {
        testDriver.g1cInit();
        final int c = 100;
        int aborted = 0;
        int numAnomaly = 0;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        final Random random = new Random();
        for (long i = 1; i <= c; i++) {
            final boolean order = random.nextBoolean();
            long account1Id = order ? 1L : 2L;
            long account2Id = order ? 2L : 1L;
            clients.add(new TransactionThread<>(i, testDriver::g1c,
                    ImmutableMap.of("account1Id", account1Id, "account2Id", account2Id, "transactionId", i)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        long[] results = new long[c];
        for (int i = 0; i < c; i++) {
            try {
                results[i] = (long) futures.get(i).get().get("account2Balance");
            } catch (Exception e) {
                results[i] = -1;
                aborted++;
                printStackTrace(e);
            }
        }

        System.out.printf("G1cTest: Number of aborted transactions: %d\n", aborted);

        // not that transactions are indexed from 1
        // but lists are indexed from 0
        for (int i = 1; i <= c; i++) {
            int account2Balance1 = (int) results[i - 1];
            if (account2Balance1 == -1) {
                continue;
            }

            if (account2Balance1 == 0L) {
                continue;
            }

            int account2Balance2 = (int) results[account2Balance1 - 1];
            if (account2Balance2 == -1) {
                System.out.printf("G1cTest failed: Transaction %d read data by aborted transaction %d", i,
                        account2Balance1);
                numAnomaly++;
            } else if (i == account2Balance2) {
                System.out.printf("G1cTest failed: Transaction %d read data by transaction %d", i, account2Balance1);
                numAnomaly++;
            }
            System.out.printf("G1cTest:   %4d %4d %4d %5b\n", i, account2Balance1, account2Balance2,
                    i != account2Balance2);
        }
        Assert.assertTrue(aborted != c);
        Assert.assertEquals(0, numAnomaly);
        System.out.println("G1cTest passed");
    }

    @Test
    public void impTest() throws Exception {
        testDriver.impInit();
        final int c = 20;

        int abortedW = 0;
        int abortedR = 0;
        int numAnomaly = 0;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < c * 2; i++) {
            if (i % 2 == 0) {
                clients.add(new TransactionThread<>(i, testDriver::impW, ImmutableMap.of("accountId", 1L)));
            } else {
                clients.add(
                        new TransactionThread<>(i, testDriver::impR,
                                ImmutableMap.of("accountId", 1L, "sleepTime", 250L)));
            }
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);

        for (int i = 0; i < c * 2; i++) {
            try {
                final Map<String, Object> results = futures.get(i).get();
                if (i % 2 != 0) {
                    final long firstRead = (long) results.get("firstRead");
                    final long secondRead = (long) results.get("secondRead");
                    System.out.printf("IMP:   %4d %4d %5b\n", firstRead, secondRead, firstRead == secondRead);
                    if (firstRead != secondRead) {
                        numAnomaly++;
                    }
                }
            } catch (Exception e) {
                if (i % 2 == 0) {
                    abortedW++;
                } else {
                    abortedR++;
                }
                printStackTrace(e);
            }
        }

        System.out.printf("IMPTest: Number of aborted transactions: %d %d\n", abortedR, abortedW);
        Assert.assertEquals(0, numAnomaly);
        Assert.assertTrue(abortedW != c);
        Assert.assertTrue(abortedR != c);
        System.out.println("IMPTest passed");
    }

    @Test
    public void pmpTest() throws Exception {
        testDriver.pmpInit();
        final int c = 20;
        int abortedW = 0;
        int abortedR = 0;
        int numAnomaly = 0;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < c * 2; i++) {
            if (i % 2 == 0) {
                clients.add(
                        new TransactionThread<>(i, testDriver::pmpW,
                                ImmutableMap.of("account1Id", 1L, "account2Id", 2L)));
            } else {
                clients.add(new TransactionThread<>(i, testDriver::pmpR,
                        ImmutableMap.of("account1Id", 1L, "account2Id", 2L, "sleepTime", 250L)));
            }
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        int i = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                final Map<String, Object> results = future.get();
                if (results.containsKey("firstRead")) {
                    final long firstRead = (long) results.get("firstRead");
                    final long secondRead = (long) results.get("secondRead");
                    System.out.printf("PMP:   %4d %4d %5b\n", firstRead, secondRead, firstRead == secondRead);
                    if (firstRead != secondRead) {
                        numAnomaly++;
                    }
                }
            } catch (Exception e) {
                if (i % 2 == 0) {
                    abortedW++;
                } else {
                    abortedR++;
                }
                printStackTrace(e);
            } finally {
                i++;
            }
        }
        System.out.printf("PMPTest: Number of aborted transactions: %d %d\n", abortedR, abortedW);
        Assert.assertEquals(0, numAnomaly);
        Assert.assertTrue(abortedW != c);
        Assert.assertTrue(abortedR != c);
        System.out.println("PMPTest passed");
    }

    @Test
    public void otvTest() throws Exception {
        testDriver.otvInit();
        final int rc = 50;

        int aborted = 0;
        int numAnomaly = 0;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        clients.add(new TransactionThread<>(0, testDriver::otvW, ImmutableMap.of("cycleSize", 4)));
        Random random = new Random();
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::otvR,
                    ImmutableMap.of("accountId", random.nextInt(4) + 1, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Object>> future : futures) {
            try {
                final Map<String, Object> results = future.get();
                if (results.containsKey("firstRead")) {
                    final List<Long> firstRead = ((List<Long>) results.get("firstRead"));
                    final List<Long> secondRead = ((List<Long>) results.get("secondRead"));
                    System.out.printf("OTV:   %4s %4s %5b\n", firstRead, secondRead,
                            Collections.max(firstRead) <= Collections.min(secondRead));
                    if (Collections.max(firstRead) > Collections.min(secondRead)) {
                        numAnomaly++;
                    }
                }
            } catch (Exception e) {
                aborted++;
                printStackTrace(e);
            }
        }
        System.out.printf("OTVTest: Number of aborted transactions: %d\n", aborted);
        Assert.assertEquals(0, numAnomaly);
        System.out.println("OTVTest passed");
    }

    @Test
    public void frTest() throws Exception {
        testDriver.frInit();
        final int c = 100;
        int numAnomaly = 0;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < c * 2; i++) {
            if (i % 2 == 0) {
                clients.add(new TransactionThread<>(i, testDriver::frW, ImmutableMap.of("accountId", 1L)));
            } else {
                clients.add(
                        new TransactionThread<>(i, testDriver::frR,
                                ImmutableMap.of("accountId", 1L, "sleepTime", 250L)));
            }
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        int abortedW = 0;
        int abortedR = 0;
        int i = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                final Map<String, Object> results = future.get();
                if (i % 2 == 1) {
                    final List<Long> firstRead = ((List<Long>) results.get("firstRead"));
                    final List<Long> secondRead = ((List<Long>) results.get("secondRead"));
                    System.out.printf("FR:   %4s %4s %5b\n", firstRead, secondRead, firstRead.equals(secondRead));
                    if (!firstRead.equals(secondRead)) {
                        numAnomaly++;
                    }
                }
            } catch (Exception e) {
                if (i % 2 == 0) {
                    abortedW++;
                } else {
                    abortedR++;
                }
                printStackTrace(e);
            } finally {
                i++;
            }
        }
        System.out.printf("FRTest: Number of aborted transactions: %d %d\n", abortedW, abortedR);
        Assert.assertEquals(0, numAnomaly);
        Assert.assertTrue(abortedR != c);
        Assert.assertTrue(abortedW != c);
        System.out.println("FRTest passed");
    }

    @Test
    public void luTest() throws Exception {
        testDriver.luInit();
        final int nTransactions = 200;

        int aborted = 0;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < nTransactions; i++) {
            clients.add(new TransactionThread<>(i, testDriver::luW,
                    ImmutableMap.of("account1Id", 1L, "account2Id", (i + 2L))));
        }
        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Object>> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                aborted++;
                printStackTrace(e);
            }
        }

        Map<String, Object> results = testDriver.luR(ImmutableMap.of("accountId", 1L));
        final long numTransferProp = (long) results.get("numTransferProp");
        final long numTransferEdges = (long) results.get("numTransferEdges");
        final boolean pass = ((nTransactions - aborted == numTransferProp)
                && (nTransactions - aborted == numTransferEdges));
        System.out.printf("LU:    %4d %4d %4d %5b\n", nTransactions - aborted, numTransferProp, numTransferEdges, pass);

        System.out.printf("LUTest: Number of aborted transactions: %d\n", aborted);
        Assert.assertEquals(nTransactions - aborted, numTransferEdges);
        Assert.assertEquals(nTransactions - aborted, numTransferProp);
        Assert.assertTrue(aborted != nTransactions);
        System.out.println("LUTest passed");
    }

    @Test
    public void wsTest() throws Exception {
        testDriver.wsInit();
        final int wc = 50;

        int numAccountPairs = 10;
        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < wc; i++) {
            // account1Id indices range from 1 to 2*numAccountPairs+1
            long account1Id = random.nextInt(numAccountPairs) * 2 + 1;
            long account2Id = account1Id + 1;
            clients.add(new TransactionThread<>(i, testDriver::wsW,
                    ImmutableMap.of("account1Id", account1Id, "account2Id", account2Id, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        int aborted = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                aborted++;
                printStackTrace(e);
            }
        }
        System.out.printf("WSTest: Number of aborted transactions: %d\n", aborted);

        Map<String, Object> results = testDriver.wsR(ImmutableMap.of());
        System.out.println(results);
        Assert.assertTrue(aborted != wc);
        Assert.assertTrue(results.isEmpty());
        System.out.println("WSTest passed");
    }

    @After
    public void cleanup() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        // closes the resources used in drivers
        testDriver.close();
    }
}
