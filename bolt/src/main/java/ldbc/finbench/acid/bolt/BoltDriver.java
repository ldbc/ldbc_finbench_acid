package ldbc.finbench.acid.bolt;

import com.google.common.collect.ImmutableMap;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import ldbc.finbench.acid.driver.TestDriver;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;

// Driver for Bolt-compatible graph databases (Memgraph and Neo4j)
public class BoltDriver extends TestDriver<Transaction, Map<String, Object>, Result> {

    protected Driver driver;

    public BoltDriver(String host, int port) {
        Config config = Config.builder().withLogging(Logging.javaUtilLogging(Level.WARNING)).build();
        try {
            String ip = InetAddress.getByName(host).getHostAddress();
            driver = GraphDatabase.driver("bolt://" + ip + ":" + port, AuthTokens.none(), config);
        } catch (UnknownHostException e) {
            driver = GraphDatabase.driver("bolt://localhost:" + port, AuthTokens.none(), config);
        }
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    @Override
    public Transaction startTransaction() {
        return driver.session().beginTransaction();
    }

    @Override
    public void commitTransaction(Transaction tt) {
        tt.commit();
        tt.close();
    }

    @Override
    public void abortTransaction(Transaction tt) {
        tt.rollback();
        tt.close();
    }

    @Override
    public Result runQuery(Transaction tt, String querySpecification, Map<String, Object> queryParameters) {
        return tt.run(querySpecification, queryParameters);
    }

    @Override
    public void nukeDatabase() {
        final Transaction tt = startTransaction();
        tt.run("MATCH (n) DETACH DELETE n");
        commitTransaction(tt);
    }

    @Override
    public void atomicityInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Account {id: 1, name: 'AliceAcc', transHistory: [100]}),\n"
                + " (:Account {id: 2, name: 'BobAcc', transHistory: [50, 150]})");
        commitTransaction(tt);
    }

    @Override
    public void atomicityC(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        tt.run("MATCH (a1:Account {id: $account1Id})\n"
                + "CREATE (a2:Account)\n"
                + "CREATE (a1)-[t:transfer]->(a2:Account)\n"
                + "SET\n"
                + "  a1.transHistory = a1.transHistory + [$newTrans],\n"
                + "  a2.id = $account2Id,\n"
                + "  t.amount = $newTrans", parameters);
        commitTransaction(tt);
    }

    @Override
    public Boolean atomicityRB(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        tt.run("MATCH (a1:Account {id: $account1Id})\n"
                + "SET a1.transHistory = a1.transHistory + [$newTrans]", parameters);
        final Result result = tt.run("MATCH (a2:Account {id: $account2Id}) RETURN a2", parameters);
        if (result.hasNext()) {
            abortTransaction(tt);
            return false;
        } else {
            tt.run("CREATE (a2:Account {id: $account2Id, transHistory: []})", parameters);
            commitTransaction(tt);
            return true;
        }
    }

    @Override
    public Map<String, Object> atomicityCheck() {
        final Transaction tt = startTransaction();

        Result result = tt.run("MATCH (a:Account)\n"
                + "RETURN count(a) AS numAccounts, count(a.name) AS numNames, sum(size(a.transHistory)) AS numTransferred");
        Record record = result.next();
        final long numAccounts = record.get("numAccounts").asLong();
        final long numNames = record.get("numNames").asLong();
        final long numTransferred = record.get("numTransferred").asLong();

        return ImmutableMap.of("numAccounts", numAccounts, "numNames", numNames, "numTransferred", numTransferred);
    }

    @Override
    public void g0Init() {
        final Transaction tt = startTransaction();
        tt.run(
                "CREATE (:Account {id: 1, versionHistory: [0]})-[:transfer {versionHistory: [0]}]->(:Account {id: 2, versionHistory: [0]})");
        commitTransaction(tt);
    }

    @Override
    public Map<String, Object> g0(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        tt.run("MATCH (a1:Account {id: $account1Id})-[t:transfer]->(a2:Account {id: $account2Id})\n"
                + "SET a1.versionHistory = a1.versionHistory + [$transactionId]\n"
                + "SET a2.versionHistory = a2.versionHistory + [$transactionId]\n"
                + "SET t.versionHistory  = t.versionHistory  + [$transactionId]", parameters);
        commitTransaction(tt);

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g0check(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        Result result = tt.run("MATCH (a1:Account {id: $account1Id})-[t:transfer]->(a2:Account {id: $account2Id})\n"
                + "RETURN\n"
                + "  a1.versionHistory AS a1VersionHistory,\n"
                + "  t.versionHistory  AS tVersionHistory,\n"
                + "  a2.versionHistory AS a2VersionHistory", parameters);
        Record record = result.next();
        final List<Object> a1VersionHistory = record.get("a1VersionHistory").asList();
        final List<Object> tVersionHistory = record.get("tVersionHistory").asList();
        final List<Object> a2VersionHistory = record.get("a2VersionHistory").asList();

        return ImmutableMap.of("a1VersionHistory", a1VersionHistory, "tVersionHistory", tVersionHistory,
                "a2VersionHistory", a2VersionHistory);
    }

    @Override
    public void g1aInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Account {id: 1, balance: 99})");
        commitTransaction(tt);
    }

    @Override
    public Map<String, Object> g1aW(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        // we cannot pass p as a parameter so we pass its internal ID instead
        final Result result = tt.run("MATCH (a:Account {id: $accountId})\n"
                + "RETURN ID(a) AS internalAId", parameters);
        if (!result.hasNext()) {
            throw new IllegalStateException("G1a1 Result empty");
        }
        final Value internalAId = result.next().get("internalAId");

        sleep((Long) parameters.get("sleepTime"));

        tt.run("MATCH (a:Account)\n"
                + "WHERE ID(a) = $internalAId\n"
                + "SET a.balance = 200", ImmutableMap.of("internalAId", internalAId));

        sleep((Long) parameters.get("sleepTime"));

        abortTransaction(tt);
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g1aR(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        final Result result = tt.run("MATCH (a:Account {id: $accountId}) RETURN a.balance AS aBalance", parameters);
        if (!result.hasNext()) {
            throw new IllegalStateException("G1a T2 Result empty");
        }
        final long aBalance = result.next().get("aBalance").asLong();

        return ImmutableMap.of("aBalance", aBalance);
    }

    @Override
    public void g1bInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Account {id: 1, balance: 99})");
        commitTransaction(tt);
    }

    @Override
    public Map<String, Object> g1bW(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        tt.run("MATCH (a:Account {id: $accountId}) SET a.balance = $even", parameters);
        sleep((Long) parameters.get("sleepTime"));
        tt.run("MATCH (a:Account {id: $accountId}) SET a.balance = $odd", parameters);

        commitTransaction(tt);
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g1bR(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        final Result result = tt.run("MATCH (a:Account {id: $accountId}) RETURN a.balance AS aBalance", parameters);
        if (!result.hasNext()) {
            throw new IllegalStateException("G1b T2 Result empty");
        }
        final long aBalance = result.next().get("aBalance").asLong();

        return ImmutableMap.of("aBalance", aBalance);
    }

    @Override
    public void g1cInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Account {id: 1, balance: 0}), (:Account {id: 2, balance: 0})");
        commitTransaction(tt);
    }

    @Override
    public Map<String, Object> g1c(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();
        Result result = tt.run("MATCH (a1:Account {id: $account1Id})\n"
                + "SET a1.balance = $transactionId\n"
                + "WITH count(*) AS dummy\n"
                + "MATCH (a2:Account {id: $account2Id})\n"
                + "RETURN a2.balance AS account2Balance\n", parameters);
        final long account2Balance = result.next().get("account2Balance").asLong();
        commitTransaction(tt);

        return ImmutableMap.of("account2Balance", account2Balance);
    }

    // IMP

    @Override
    public void impInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Account {id: 1, balance: 1})");
        commitTransaction(tt);
    }

    @Override
    public Map<String, Object> impW(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();
        tt.run("MATCH (a:Account {id: $accountId}) SET a.balance = a.balance + 1 RETURN a", parameters);
        commitTransaction(tt);
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> impR(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        final Result result1 = tt.run("MATCH (a:Account {id: $accountId}) RETURN a.balance AS firstRead", parameters);
        if (!result1.hasNext()) {
            throw new IllegalStateException("IMP result1 empty");
        }
        final long firstRead = result1.next().get("firstRead").asLong();

        sleep((Long) parameters.get("sleepTime"));

        final Result result2 = tt.run("MATCH (a:Account {id: $accountId}) RETURN a.balance AS secondRead", parameters);
        if (!result2.hasNext()) {
            throw new IllegalStateException("IMP result2 empty");
        }
        final long secondRead = result2.next().get("secondRead").asLong();

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    // PMP

    @Override
    public void pmpInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Account {id: 1}), (:Account {id: 2})");
        commitTransaction(tt);
    }

    @Override
    public Map<String, Object> pmpW(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();
        tt.run("MATCH (a1:Account {id: $account1Id}), (a2:Account {id: $account2Id})\n"
                + "CREATE (a1)-[:transfer]->(a2)", parameters);
        commitTransaction(tt);
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> pmpR(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        final Result result1 = tt.run("MATCH (a2:Account {id: $account2Id})<-[:transfer]-(a1:Account)\n"
                + "RETURN count(a1) AS firstRead", parameters);
        if (!result1.hasNext()) {
            throw new IllegalStateException("PMP result1 empty");
        }
        final long firstRead = result1.next().get("firstRead").asLong();

        sleep((Long) parameters.get("sleepTime"));

        final Result result2 = tt.run(
                "MATCH (a2:Account {id: $account2Id})<-[:transfer]-(a3:Account) RETURN count(a3) AS secondRead",
                parameters);
        if (!result2.hasNext()) {
            throw new IllegalStateException("PMP result2 empty");
        }
        final long secondRead = result2.next().get("secondRead").asLong();

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    @Override
    public void otvInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (a1:Account {id: 1, balance: 0})-[:transfer]->"
                + "  (:Account {id: 2, balance: 0})-[:transfer]->"
                + "  (:Account {id: 3, balance: 0})-[:transfer]->"
                + "  (:Account {id: 4, balance: 0})-[:transfer]->(a1)");
        commitTransaction(tt);
    }

    @Override
    public Map<String, Object> otvW(Map<String, Object> parameters) {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            long accountId = random.nextInt((int) parameters.get("cycleSize") + 1);

            final Transaction tt = startTransaction();
            tt.run(
                    "MATCH path = (n:Account {id: $accountId})-[:transfer*..4]->(n)\n"
                            + " UNWIND nodes(path)[0..4] as a\n"
                            + " SET a.balance = a.balance + 1\n",
                    ImmutableMap.of("accountId", accountId));
            commitTransaction(tt);
        }
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> otvR(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();
        final Result result1 = tt.run(
                "MATCH p1 = (a1:Account {id: $accountId})-[:transfer*..4]->(a1)\n"
                        + "RETURN extract(a in nodes(p1) | a.balance) AS firstRead",
                parameters);
        if (!result1.hasNext()) {
            throw new IllegalStateException("OTV2 result1 empty");
        }
        final List<Object> firstRead = result1.next().get("firstRead").asList();

        sleep((Long) parameters.get("sleepTime"));

        final Result result2 = tt.run(
                "MATCH p2 = (a2:Account {id: $accountId})-[:transfer*..4]->(a2)\n"
                        + "RETURN extract(a in nodes(p2) | a.balance) AS secondRead",
                parameters);
        if (!result2.hasNext()) {
            throw new IllegalStateException("OTV2 result2 empty");
        }
        final List<Object> secondRead = result2.next().get("secondRead").asList();

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    @Override
    public void frInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (a1:Account {id: 1, balance: 0})-[:transfer]->"
                + "  (:Account {id: 2, balance: 0})-[:transfer]->"
                + "  (:Account {id: 3, balance: 0})-[:transfer]->"
                + "  (:Account {id: 4, balance: 0})-[:transfer]->(a1)");
        commitTransaction(tt);
    }

    @Override
    public Map<String, Object> frW(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();
        tt.run(
                "MATCH path = (n:Account {id: $accountId})-[:transfer*..4]->(n)\n"
                        + " UNWIND nodes(path)[0..4] as a\n"
                        + " SET a.balance = a.balance + 1\n",
                parameters);
        commitTransaction(tt);

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> frR(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        // Neo4j: Extract is no longer supported in neo4j >= 5.x, use list comprehension
        // instead
        // Memgraph: Not yet implemented: atom expression '[a IN
        // nodes(path1)|a.balance]'
        final Result result1 = tt.run(
                "MATCH p1 = (a1:Account {id: $accountId})-[:transfer*..4]->(a1)\n"
                        + "RETURN extract(a in nodes(p1) | a.balance) AS firstRead",
                parameters);
        if (!result1.hasNext()) {
            throw new IllegalStateException("FR2 result1 empty");
        }
        final List<Object> firstRead = result1.next().get("firstRead").asList();

        sleep((Long) parameters.get("sleepTime"));

        final Result result2 = tt.run(
                "MATCH p2 = (a2:Account {id: $accountId})-[:transfer*..4]->(a2)\n"
                        + "RETURN extract(a in nodes(p2) | a.balance) AS secondRead",
                parameters);
        if (!result2.hasNext()) {
            throw new IllegalStateException("FR2 result2 empty");
        }
        final List<Object> secondRead = result2.next().get("secondRead").asList();

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    // LU

    @Override
    public void luInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Account {id: 1, numTransferred: 0})");
        commitTransaction(tt);
    }

    @Override
    public Map<String, Object> luW(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();
        tt.run("MATCH (a1:Account {id: 1})\n"
                + "CREATE (a1)-[:transfer]->(a2)\n"
                + "SET a1.numTransferred = a1.numTransferred + 1\n"
                + "RETURN a1.numTransferred\n");
        commitTransaction(tt);
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> luR(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();
        final Result result = tt.run("MATCH (a:Account {id: $accountId})\n"
                + "OPTIONAL MATCH (a)-[t:transfer]->()\n"
                + "WITH a, count(t) AS numTransferEdges\n"
                + "RETURN numTransferEdges,\n"
                + "       a.numTransferred AS numTransferred\n", parameters);
        final Record record = result.next();
        long numTransferEdges = record.get("numTransferEdges").asLong();
        long numTransferred = record.get("numTransferred").asLong();
        return ImmutableMap.of("numTransferEdges", numTransferEdges, "numTransferred", numTransferred);
    }

    @Override
    public void wsInit() {
        final Transaction tt = startTransaction();

        // create 10 pairs of accounts with indices (1,2), ..., (19,20)
        for (int i = 1; i <= 10; i++) {
            tt.run("CREATE (:Account {id: $account1Id, balance: 70}), (:Account {id: $account2Id, balance: 80})",
                    ImmutableMap.of("account1Id", 2 * i - 1, "account2Id", 2 * i));
        }

        commitTransaction(tt);
    }

    @Override
    public Map<String, Object> wsW(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        final Result result = tt.run(
                "MATCH (a1:Account {id: $account1Id}), (a2:Account {id: $account2Id})\n"
                        + "WHERE a1.balance + a2.balance >= 100\n"
                        + "RETURN a1, a2",
                parameters);

        if (result.hasNext()) {
            sleep((Long) parameters.get("sleepTime"));

            long accountId = new Random().nextBoolean()
                    ? (long) parameters.get("account1Id")
                    : (long) parameters.get("account2Id");

            tt.run("MATCH (a:Account {id: $accountId})\n"
                    + "SET a.balance = a.balance - 100",
                    ImmutableMap.of("accountId", accountId));
            commitTransaction(tt);
        }

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> wsR(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();
        // we select pairs of accounts using (id, id+1) pairs
        final Result result = tt.run("MATCH (a1:Account), (a2:Account {id: a1.id+1})\n"
                + "WHERE a1.balance + a2.balance <= 0 and a1.id % 2 = 1 \n"
                + "RETURN a1.id AS a1id, a1.balance AS a1balance, a2.id AS a2id, a2.balance AS a2balance");

        if (result.hasNext()) {
            Record record = result.next();
            return ImmutableMap.of(
                    "a1id", record.get("a1id"),
                    "a1balance", record.get("a1balance"),
                    "a2id", record.get("a2id"),
                    "a2balance", record.get("a2balance"));
        } else {
            return ImmutableMap.of();
        }
    }

}
