package ldbc.finbench.acid.ultipa;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import ldbc.finbench.acid.driver.TestDriver;

import java.io.IOException;
import java.util.*;


public class UltipaDriver extends TestDriver<UltipaConnection, Map<String, Object>, UltipaResultSet> {

    String httpServer;
    String host;
    int port;
    String username;
    String password;

    public UltipaDriver(String httpServer,
                        String host,
                        int port,
                        String username,
                        String password) {
        this.httpServer = httpServer;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }


    public static void assertSuccess(UltipaResultSet response) {
        if (!response.isOk()) {
            System.err.println("http request failed: " + response);
            assert false;
        }
    }

    public static void catchException(final Exception exception) {
        exception.printStackTrace();
    }

    @Override
    public void close() throws Exception {
        reset();
    }


    public UltipaDriver reset() {
        try {
            UltipaConnection ultipaConnection = new UltipaConnection(httpServer, host, port, username, password);
            ultipaConnection.connect();
            ultipaConnection.disconnect();
        } catch (IOException exception) {
            catchException(exception);
        }
        return this;
    }

    public String format(String text, Map<String, Object> queryParameters) {
        Set<String> keys = queryParameters.keySet();
        for (String key : keys) {
            String param = "$" + key;
            if (text.contains(param)) {
                Object value = queryParameters.get(key);
                text = text.replace(param, value.toString());
            }
        }
        return text;
    }

    @Override
    public UltipaConnection startTransaction() {
        UltipaConnection ultipaConnection = new UltipaConnection(httpServer, host, port, username, password);
        try {
            ultipaConnection.connect();
            ultipaConnection.begin();
        } catch (IOException ioException) {
            catchException(ioException);
        }
        return ultipaConnection;
    }

    @Override
    public void commitTransaction(UltipaConnection tt) {
        tt.commit();
    }

    @Override
    public void abortTransaction(UltipaConnection tt) {

    }

    @Override
    public UltipaResultSet runQuery(UltipaConnection tt, String querySpecification, Map<String, Object> queryParameters) {
        UltipaResultSet map = tt.run(querySpecification, queryParameters);
        return map;
    }

    @Override
    public void nukeDatabase() {
        final String cypher = "MATCH (n) DETACH DELETE n";

        final UltipaConnection tt = startTransaction();

        try {
            try {
                tt.run(("drop().node_schema(@person)"));
                tt.run(("create().node_schema('person')"));
                tt.run(("create().node_property(@person,'name','string')"));
                tt.run(("create().node_property(@person,'isBlocked','int32')"));
            } catch (Exception e) {
                catchException(e);
            }

            try {
                tt.run(("drop().node_schema(@company)"));
                tt.run(("create().node_schema('company')"));
                tt.run(("create().node_property(@company,'name','string')"));
                tt.run(("create().node_property(@company,'isBlocked','int32')"));
            } catch (Exception e) {
                catchException(e);
            }

            try {
                tt.run(("drop().node_schema(@account)"));
                tt.run(("create().node_schema('account')"));
                tt.run(("create().node_property(@account,'createTime','datetime')"));
                tt.run(("create().node_property(@account,'isBlocked','int32')"));
                tt.run(("create().node_property(@account,'type','string')"));

                tt.run(("create().node_property(@account,'transHistory','int64[]')"));
                tt.run(("create().node_property(@account,'balance','int64')"));
                tt.run(("create().node_property(@account,'numTransferred','int64')"));
                tt.run(("create().node_property(@account,'versionHistory','int64[]')"));
                tt.run(("create().node_property(@account,'name','string')"));


            } catch (Exception e) {
                catchException(e);
            }

            try {
                tt.run(("drop().node_schema(@loan)"));
                tt.run(("create().node_schema('loan')"));
                tt.run(("create().node_property(@loan,'loanAmount','int64')"));
                tt.run(("create().node_property(@loan,'balance','int64')"));
            } catch (Exception e) {
                catchException(e);
            }

            try {
                tt.run(("drop().node_schema(@medium)"));
                tt.run(("create().node_schema('medium')"));
                tt.run(("create().node_property(@medium,'name','string')"));
                tt.run(("create().node_property(@medium,'isBlocked','int32')"));
            } catch (Exception e) {
                catchException(e);
            }

            try {
                tt.run(("drop().edge_schema(@transfer)"));
                tt.run(("create().edge_schema('transfer')"));
                tt.run(("create().edge_property(@transfer,'timestamp','datetime')"));
                tt.run(("create().edge_property(@transfer,'amount','int64')"));
                tt.run(("create().edge_property(@transfer,'type','string')"));
                tt.run(("create().edge_property(@transfer,'versionHistory','int64[]')"));

            } catch (Exception e) {
                catchException(e);
            }

            try {
                tt.run(("drop().edge_schema(@withdraw)"));
                tt.run(("create().edge_schema('withdraw')"));
                tt.run(("create().edge_property(@withdraw,'timestamp','datetime')"));
                tt.run(("create().edge_property(@withdraw,'amount','int64')"));
            } catch (Exception e) {
                catchException(e);
            }


            try {
                tt.run(("drop().edge_schema(@repay)"));
                tt.run(("create().edge_schema('repay')"));
                tt.run(("create().edge_property(@repay,'timestamp','datetime')"));
                tt.run(("create().edge_property(@repay,'amount','int64')"));
            } catch (Exception e) {
                catchException(e);
            }

            try {
                tt.run(("drop().edge_schema(@deposit)"));
                tt.run(("create().edge_schema('deposit')"));
                tt.run(("create().edge_property(@deposit,'timestamp','datetime')"));
                tt.run(("create().edge_property(@deposit,'amount','int64')"));
            } catch (Exception e) {
                catchException(e);
            }

            try {
                tt.run(("drop().edge_schema(@signIn)"));
                tt.run(("create().edge_schema('signIn')"));
                tt.run(("create().edge_property(@signIn,'timestamp','datetime')"));
            } catch (Exception e) {
                catchException(e);
            }

            try {
                tt.run(("drop().edge_schema(@apply)"));
                tt.run(("create().edge_schema('apply')"));
                tt.run(("create().edge_property(@apply,'timestamp','datetime')"));
            } catch (Exception e) {
                catchException(e);
            }

            try {
                tt.run(("drop().edge_schema(@guarantee)"));
                tt.run(("create().edge_schema('guarantee')"));
                tt.run(("create().edge_property(@guarantee,'timestamp','datetime')"));
            } catch (Exception e) {
                catchException(e);
            }

            final UltipaResultSet response = tt.run("delete().nodes()");
            assertSuccess(response);
            commitTransaction(tt);
        } finally {
            tt.close();
        }

    }

    @Override
    public void atomicityInit() {
        final String cypher = "CREATE (:Account {id: 1, name: 'AliceAcc', transHistory: [100]}),\n"
                + " (:Account {id: 2, name: 'BobAcc', transHistory: [50, 150]})";

        final UltipaConnection tt = startTransaction();
        try {
            final UltipaResultSet response = tt.run("insert().into(@account).nodes([{_id: 1, name: 'AliceAcc',transHistory:[100]},\n" +
                    " {_id:2, name:'BobAcc',transHistory:[50, 150]}]) as nodes return nodes._uuid");
            assertSuccess(response);
            commitTransaction(tt);
        } finally {
            tt.close();
        }

    }

    @Override
    public void atomicityC(Map<String, Object> parameters) {
        String cypher = "MATCH (a1:Account {id: $account1Id})\n"
                + "CREATE (a1)-[t:transfer]->(a2:Account)\n"
                + "SET\n"
                + "  a1.transHistory = a1.transHistory + [$newTrans],\n"
                + "  a2.id = $account2Id,\n"
                + "  t.amount = $newTrans";

        final UltipaConnection tt = startTransaction();
        try {
            final UltipaResultSet response = tt.run(
                    "update().nodes({@account && _id == $account1Id}).set({transHistory:append(this.transHistory,$newTrans)}) as a1 " +
                            "insert().into(@account).nodes([{_id:$account2Id}]) as a2 " +
                            "insert().into(@transfer).edges({_from:a1._id,_to:a2._id,amount:$newTrans}) ", parameters);
            assertSuccess(response);
            commitTransaction(tt);
        } finally {
            tt.close();
        }

    }

    @Override
    public Boolean atomicityRB(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();

        try {
            {
                String cypher = "MATCH (a1:Account {id: $account1Id})\n"
                        + "SET a1.transHistory = a1.transHistory + [$newTrans]";
            }

            tt.run("update().nodes({@account && _id == $account1Id}).set({transHistory:append(this.transHistory,$newTrans)})", parameters)
            ;
            {
                String cypher = "MATCH (a2:Account {id: $account2Id}) RETURN a2";
                final UltipaResultSet result = tt.run("find().nodes({@account && _id == $account2Id}) as a2 return a2", parameters);
                assertSuccess(result);

                if (!result.isEmpty("a2")) {
                    abortTransaction(tt);
                    return false;
                } else {
                    {
                        String insert_cypher = "CREATE (a2:Account {id: $account2Id, transHistory: 0})";
                    }

                    tt.run("insert().into(@account).nodes([{_id:$account2Id,transHistory:0}]) ", parameters);
                    commitTransaction(tt);
                    return true;
                }
            }
        } finally {
            tt.close();
        }

    }

    @Override
    public Map<String, Object> atomicityCheck() {
        final UltipaConnection tt = startTransaction();

        try {
            {
                String cypher = "MATCH (a:Account)\n"
                        + "RETURN count(a) AS numAccounts, count(a.name) AS numNames, sum(size(a.transHistory)) AS numTransferred";
            }

            UltipaResultSet result = tt.run("find().nodes({@account}) as a " +
                    "RETURN count(a) AS numAccounts, count(a.name) AS numNames, sum(size(a.transHistory)) AS numTransferred");
            assertSuccess(result);

            final long numAccounts = result.aliasAsLong("numAccounts");
            final long numNames = result.aliasAsLong("numNames");
            final long numTransferred = result.aliasAsLong("numTransferred");

            return ImmutableMap.of("numAccounts", numAccounts, "numNames", numNames, "numTransferred", numTransferred);
        } finally {
            tt.close();
        }

    }

    @Override
    public void g0Init() {
        final UltipaConnection tt = startTransaction();
        try {
            String cypher = "CREATE (:Account {id: 1, versionHistory: [0]})-[:transfer {versionHistory: [0]}]->(:Account {id: 2, versionHistory: [0]})";
            UltipaResultSet result = tt.run(("insert().into(@account).nodes([{_id:1,versionHistory:[0]},{_id:2,versionHistory:[0]}])"));
            assertSuccess(result);
            result = tt.run(("insert().into(@transfer).edges([{_from:1,_to:2,versionHistory:[0]}])"));
            assertSuccess(result);
            commitTransaction(tt);
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> g0(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        try {
            String cypher = "MATCH (a1:Account {id: $account1Id})-[t:transfer]->(a2:Account {id: $account2Id})\n"
                    + "SET a1.versionHistory = a1.versionHistory + [$transactionId]\n"
                    + "SET a2.versionHistory = a2.versionHistory + [$transactionId]\n"
                    + "SET t.versionHistory  = t.versionHistory  + [$transactionId]";


            String uql = format("n({@account && _id == $account1Id} as a1).e({@transfer} as t).n({@account && _id == $account2Id} as a2) " +
                    " with t " +
                    "update().nodes({_uuid == a1._uuid || _uuid == a2._uuid }).set({versionHistory:append(this.versionHistory,$transactionId)}) as nodes " +

                    "update().edges({_uuid == t._uuid}).set({versionHistory:append(this.versionHistory,$transactionId)}) as edges return nodes,edges", parameters);

            UltipaResultSet result = tt.run(uql);
            assertSuccess(result);
            commitTransaction(tt);

            return ImmutableMap.of();
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> g0check(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();

        try {
            String cypher = "MATCH (a1:Account {id: $account1Id})-[t:transfer]->(a2:Account {id: $account2Id})\n"
                    + "RETURN\n"
                    + "  a1.versionHistory AS a1VersionHistory,\n"
                    + "  t.versionHistory  AS tVersionHistory,\n"
                    + "  a2.versionHistory AS a2VersionHistory";

            String uql = format("n({@account && _id == $account1Id} as a1).re({@transfer} as t).n({@account && _id == $account2Id} as a2)"
                    + "RETURN\n"
                    + "  a1.versionHistory AS a1VersionHistory,\n"
                    + "  t.versionHistory  AS tVersionHistory,\n"
                    + "  a2.versionHistory AS a2VersionHistory", parameters);
            UltipaResultSet result = tt.run(uql);
            assertSuccess(result);

            final List a1VersionHistory = result.aliasAsList("a1VersionHistory", Collections.emptyList(), List.class).get(0);
            final List tVersionHistory = result.aliasAsList("tVersionHistory", Collections.emptyList(), List.class).get(0);
            final List a2VersionHistory = result.aliasAsList("a2VersionHistory", Collections.emptyList(), List.class).get(0);

            return ImmutableMap.of("a1VersionHistory", a1VersionHistory, "tVersionHistory", tVersionHistory,
                    "a2VersionHistory", a2VersionHistory);
        } finally {
            tt.close();
        }
    }

    @Override
    public void g1aInit() {
        String cypher = "CREATE (:Account {id: 1, balance: 99})";
        UltipaResultSet result;
        final UltipaConnection tt = startTransaction();
        try {
            result = tt.run("insert().into(@account).nodes([{_id:1, balance:99}])");
            assertSuccess(result);
            commitTransaction(tt);
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> g1aW(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        UltipaResultSet result;

        try {
            // we cannot pass p as a parameter so we pass its internal ID instead
            {
                String cypher = "MATCH (a:Account {id: $accountId})\n"
                        + "RETURN ID(a) AS internalAId";

                result = tt.run(format("find().nodes({@account && _id == $accountId}) as nodes return nodes._id as internalAId", parameters));
                assertSuccess(result);
            }
            List<String> internalAIdList = (result.aliasAsList("internalAId", Collections.emptyList(), String.class));
            if (internalAIdList.isEmpty()) {
                throw new IllegalStateException("G1a1 Result empty");
            }
            final Object internalAId = internalAIdList.get(0);

            sleep((Long) parameters.get("sleepTime"));

            {
                String cypher = "MATCH (a:Account)\n"
                        + "WHERE ID(a) = $internalAId\n"
                        + "SET a.balance = 200";

                result = tt.run(format("update().nodes({@account && _id == $accountId}).set({balance:200})", ImmutableMap.of("accountId", internalAId)));
                assertSuccess(result);
            }


            sleep((Long) parameters.get("sleepTime"));

            abortTransaction(tt);
            return ImmutableMap.of();
        } catch (IllegalStateException e) {
            tt.close();
            throw e;
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> g1aR(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();

        try {
            String cypher = "MATCH (a:Account {id: $accountId}) RETURN a.balance AS aBalance";

            UltipaResultSet result;

            result = tt.run("find().nodes({@account && _id == $accountId}) as a return a.balance AS aBalance", parameters);
            assertSuccess(result);

            List<Long> aBalanceList = result.aliasAsList("aBalance", Collections.emptyList(), Long.class);

            if (aBalanceList.isEmpty()) {
                throw new IllegalStateException("G1a T2 Result empty");
            }
            final long aBalance = (long) Double.parseDouble(aBalanceList.get(0).toString());
            return ImmutableMap.of("aBalance", aBalance);
        } catch (IllegalStateException e) {
            tt.close();
            throw e;
        } finally {
            tt.close();
        }

    }

    @Override
    public void g1bInit() {
        String cypher = "CREATE (:Account {id: 1, balance: 99})";
        UltipaResultSet result;
        final UltipaConnection tt = startTransaction();

        try {
            result = tt.run("insert().into(@account).nodes([{_id:1, balance:99}])");
            assertSuccess(result);
            commitTransaction(tt);
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> g1bW(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        try {
            UltipaResultSet result;
            {
                String cypher = "MATCH (a:Account {id: $accountId}) SET a.balance = $even";
                result = tt.run(format("update().nodes({@account && _id == $accountId}).set({balance:$even})", parameters));
                assertSuccess(result);
            }
            sleep((Long) parameters.get("sleepTime"));
            {
                String cypher = "MATCH (a:Account {id: $accountId}) SET a.balance = $odd";
                result = tt.run(format("update().nodes({@account && _id == $accountId}).set({balance:$odd})", parameters));
                assertSuccess(result);
            }
            commitTransaction(tt);
            return ImmutableMap.of();
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> g1bR(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        try {
            String cypher = "MATCH (a:Account {id: $accountId}) RETURN a.balance AS aBalance";

            UltipaResultSet result;
            result = tt.run(format("find().nodes({@account && _id == $accountId}) as a return a.balance AS aBalance", parameters));
            assertSuccess(result);

            List<Long> aBalanceList = result.aliasAsList("aBalance", Collections.emptyList(), Long.class);

            if (aBalanceList.isEmpty()) {
                throw new IllegalStateException("G1b T2 Result empty");
            }
            final long aBalance = (long) Double.parseDouble(aBalanceList.get(0).toString());
            return ImmutableMap.of("aBalance", aBalance);
        } catch (IllegalStateException e) {
            tt.close();
            throw e;
        } finally {
            tt.close();
        }

    }

    @Override
    public void g1cInit() {
        String cypher = "CREATE (:Account {id: 1, balance: 0}), (:Account {id: 2, balance: 0})";
        UltipaResultSet result;
        final UltipaConnection tt = startTransaction();
        try {
            result = tt.run("insert().into(@account).nodes([{_id:1, balance:0},{_id:2, balance:0}])");
            assertSuccess(result);
            commitTransaction(tt);
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> g1c(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        try {
            String cypher = "MATCH (a1:Account {id: $account1Id})\n"
                    + "SET a1.balance = $transactionId\n"
                    + "WITH count(*) AS dummy\n"
                    + "MATCH (a2:Account {id: $account2Id})\n"
                    + "RETURN a2.balance AS account2Balance\n";

            UltipaResultSet result = tt.run(format("update().nodes({@account && _id == $account1Id}).set({balance:$transactionId}) as nodes " +
                    "with count(nodes) as dummy " +
                    "find().nodes({@account && _id == $account2Id}) as a2 return a2.balance AS account2Balance", parameters));
            assertSuccess(result);
            final long account2Balance = (result.aliasAsLong("account2Balance"));
            commitTransaction(tt);

            return ImmutableMap.of("account2Balance", account2Balance);
        } finally {
            tt.close();
        }
    }

    // IMP

    @Override
    public void impInit() {
        String cypher = "CREATE (:Account {id: 1, balance: 1})";
        UltipaResultSet result;
        final UltipaConnection tt = startTransaction();
        try {
            result = tt.run("insert().into(@account).nodes([{_id:1, balance:1}])");
            assertSuccess(result);
            commitTransaction(tt);
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> impW(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        String cypher = "MATCH (a:Account {id: $accountId}) SET a.balance = a.balance + 1 RETURN a";
        UltipaResultSet result;
        try {
            result = tt.run(format("update().nodes({@account}).set({balance:this.balance + 1 })", parameters));
            assertSuccess(result);
            commitTransaction(tt);
            return ImmutableMap.of();
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> impR(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();

        try {
            {
                String cypher = "MATCH (a:Account {id: $accountId}) RETURN a.balance AS firstRead";
            }
            final UltipaResultSet result1 =
                    tt.run(format("find().nodes({@account &&  _id == $accountId}) as a RETURN a.balance AS firstRead", parameters));
            assertSuccess(result1);

            List<Long> firstReadList = result1.aliasAsList("firstRead", Collections.emptyList(), Long.class);
            if (firstReadList.isEmpty()) {
                throw new IllegalStateException("IMP result1 empty");
            }
            final long firstRead = (long) Double.parseDouble(firstReadList.get(0).toString());

            sleep((Long) parameters.get("sleepTime"));

            {
                String cypher = "MATCH (a:Account {id: $accountId}) RETURN a.balance AS secondRead";
            }
            final UltipaResultSet result2 =
                    tt.run(format("find().nodes({@account && _id == $accountId}) as a  RETURN a.balance AS secondRead", parameters));
            assertSuccess(result2);

            List<Long> secondReadList = result2.aliasAsList("secondRead", Collections.emptyList(), Long.class);

            if (secondReadList.isEmpty()) {
                throw new IllegalStateException("IMP result2 empty");
            }
            final long secondRead = (long) Double.parseDouble(secondReadList.get(0).toString());

            return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
        } catch (IllegalStateException e) {
            tt.close();
            throw e;
        } finally {
            tt.close();
        }

    }

    // PMP

    @Override
    public void pmpInit() {
        String cypher = "CREATE (:Account {id: 1}), (:Account {id: 2})";
        UltipaResultSet result;
        final UltipaConnection tt = startTransaction();
        try {
            result = tt.run("insert().into(@account).nodes([{_id:1},{_id:2}])");
            assertSuccess(result);
            commitTransaction(tt);
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> pmpW(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        try {
            UltipaResultSet result;
            String cypher = "MATCH (a1:Account {id: $account1Id}), (a2:Account {id: $account2Id})\n"
                    + "CREATE (a1)-[:transfer]->(a2)";
            result = tt.run(format("find().nodes({@account && _id == $account1Id}) as a1 find().nodes({@account && _id == $account2Id}) as a2 with a1,a2 insert().into(@transfer).edges([{_from:a1._id,_to:a2._id}])", parameters));
            assertSuccess(result);
            commitTransaction(tt);
            return ImmutableMap.of();
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> pmpR(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();

        try {
            {
                String cypher = "MATCH (a2:Account {id: $account2Id})<-[:transfer]-(a1:Account)\n"
                        + "RETURN count(a1) AS firstRead";
            }
            final UltipaResultSet result1 =
                    tt.run(format("n({@account && _id == $account2Id} as a2).le({@transfer}).n({@account} as a1) " +
                            "RETURN count(a1) AS firstRead", parameters));
            assertSuccess(result1);

            List<Long> firstReadList = result1.aliasAsList("firstRead", Collections.emptyList(), Long.class);


            if (firstReadList.isEmpty()) {
                throw new IllegalStateException("PMP result1 empty");
            }

            final long firstRead = (long) Double.parseDouble(firstReadList.get(0).toString());

            sleep((Long) parameters.get("sleepTime"));

            {
                String cypher = "MATCH (a2:Account {id: $account2Id})<-[:transfer]-(a3:Account) RETURN count(a3) AS secondRead";
            }

            final UltipaResultSet result2 =
                    tt.run(format("n({@account && _id == $account2Id} as a2).le({@transfer}).n({@account} as a3) RETURN count(a3) AS secondRead",
                            parameters));
            assertSuccess(result2);

            final List<Long> secondReadList = result2.aliasAsList("secondRead", Collections.emptyList(), Long.class);
            if (secondReadList.isEmpty()) {
                throw new IllegalStateException("PMP result2 empty");
            }
            final long secondRead = Long.parseLong(secondReadList.get(0).toString());

            return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
        } catch (IllegalStateException e) {
            tt.close();
            throw e;
        } finally {
            tt.close();
        }
    }

    @Override
    public void otvInit() {
        String cypher = "CREATE (a1:Account {id: 1, balance: 0})-[:transfer]->"
                + "  (:Account {id: 2, balance: 0})-[:transfer]->"
                + "  (:Account {id: 3, balance: 0})-[:transfer]->"
                + "  (:Account {id: 4, balance: 0})-[:transfer]->(a1)";
        UltipaResultSet result;
        final UltipaConnection tt = startTransaction();
        try {
            result = tt.run("insert().into(@account).nodes([{_id:1,balance:0},{_id:2,balance:0},{_id:3,balance:0},{_id:4,balance:0}])");
            assertSuccess(result);

            result = tt.run("insert().into(@transfer).edges([{_from:1,_to:2},{_from:2,_to:3},{_from:3,_to:4},{_from:4,_to:1}])");
            assertSuccess(result);

            commitTransaction(tt);
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> otvW(Map<String, Object> parameters) {
        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            UltipaResultSet result;

            long accountId = random.nextInt((int) parameters.get("cycleSize") + 1);

            final UltipaConnection tt = startTransaction();
            try {
                String cypher = "MATCH path = (a1:Account {id: $accountId})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(a1)\n"
                        + " SET a1.balance = a1.balance + 1\n"
                        + " SET a2.balance = a2.balance + 1\n"
                        + " SET a3.balance = a3.balance + 1\n"
                        + " SET a4.balance = a4.balance + 1\n";

                String uql = format("n({@account && _id == $accountId} as a1).re({@transfer}).n(as a2).re({@transfer}).n(as a3).re({@transfer}).n(as a4).re({@transfer}).n(a1) " +
                                "with a1,a2,a3,a4 " +
                                "update().nodes({_id == a1._id}).set({balance: a1.balance + 1}) as n1 with n1 " +
                                "update().nodes({_id == a2._id}).set({balance: a2.balance + 1}) as n2  with n2 " +
                                "update().nodes({_id == a3._id}).set({balance: a3.balance + 1}) as n3  with n3 " +
                                "update().nodes({_id == a4._id}).set({balance: a4.balance + 1}) as n4  with n4 " +
                                "return n1,n2,n3,n4",

                        ImmutableMap.of("accountId", accountId));

                result = tt.run(uql);
                assertSuccess(result);

                commitTransaction(tt);
            } finally {
                tt.close();
            }
        }
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> otvR(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        try {
            {
                String cypher = "MATCH (a1:Account {id: $accountId})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(p1)\n"
                        + "RETURN [a1.balance, a2.balance, a3.balance, a4.balance] AS firstRead";
            }

            String uql = "n({@account && _id == $accountId} as a1).re({@transfer}).n(as a2).re({@transfer}).n(as a3).re({@transfer}).n(as a4).re({@transfer}).n(as p1) "
                    + "RETURN a1.balance as a1_balance, a2.balance as a2_balance, a3.balance as a3_balance, a4.balance as a4_balance";

            final UltipaResultSet result1 = tt.run(format(uql, parameters));
            assertSuccess(result1);

            final List<Long> firstRead = result1.aliasAsList("a1_balance", Collections.emptyList(), Long.class);
            if (firstRead.isEmpty()) {
                throw new IllegalStateException("OTV2 result1 empty");
            }

            sleep((Long) parameters.get("sleepTime"));

            {
                String cypher = "MATCH (a1:Account {id: $accountId})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(a1)\n"
                        + "RETURN [a1.balance, a2.balance, a3.balance, a4.balance] AS secondRead";
            }
            final UltipaResultSet result2 = tt.run(format("n({@account && _id == $accountId} as a1).re({@transfer}).n(as a2).re({@transfer}).n(as a3).re({@transfer}).n(as a4).re({@transfer}).n(a1) "
                            + "RETURN a1.balance as a1_balance, a2.balance as a2_balance, a3.balance as a3_balance, a4.balance as a4_balance",
                    parameters));
            assertSuccess(result2);

            final List<Long> secondRead = result2.aliasAsList("a1_balance", Collections.emptyList(), Long.class);
            if (secondRead.isEmpty()) {
                throw new IllegalStateException("OTV2 result2 empty");
            }
            return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
        } catch (IllegalStateException e) {
            tt.close();
            throw e;
        } finally {
            tt.close();
        }
    }

    @Override
    public void frInit() {

        String cypher = "CREATE (a1:Account {id: 1, balance: 0})-[:transfer]->"
                + "  (:Account {id: 2, balance: 0})-[:transfer]->"
                + "  (:Account {id: 3, balance: 0})-[:transfer]->"
                + "  (:Account {id: 4, balance: 0})-[:transfer]->(a1)";
        UltipaResultSet result;
        final UltipaConnection tt = startTransaction();
        try {
            result = tt.run("insert().into(@account).nodes([{_id:1,balance:0},{_id:2,balance:0},{_id:3,balance:0},{_id:4,balance:0}])");
            assertSuccess(result);
            result = tt.run("insert().into(@transfer).edges([{_from:1,_to:2},{_from:2,_to:3},{_from:3,_to:4},{_from:4,_to:1}])");
            assertSuccess(result);

            commitTransaction(tt);
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> frW(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        try {
            UltipaResultSet result;
            String cypher = "MATCH path = (a1:Account {id: $accountId})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(a1)\n"
                    + " SET a1.balance = a1.balance + 1\n"
                    + " SET a2.balance = a2.balance + 1\n"
                    + " SET a3.balance = a3.balance + 1\n"
                    + " SET a4.balance = a4.balance + 1\n";

            String uql = format("n({@account && _id == $accountId} as a1).re({@transfer}).n(as a2).re({@transfer}).n(as a3).re({@transfer}).n(as a4).re({@transfer}).n({_uuid == a1._uuid}) with a1, a2, a3, a4\n" +
                            "\n" +
                            "update().nodes({_id == a1._id}).set({balance: a1.balance + 1}) as n1 with n1\n" +
                            "\n" +
                            "update().nodes({_id == a2._id}).set({balance: a2.balance + 1}) as n2 with n2\n" +
                            "\n" +
                            "update().nodes({_id == a3._id}).set({balance: a3.balance + 1}) as n3 with n3\n" +
                            "\n" +
                            "update().nodes({_id == a4._id}).set({balance: a4.balance + 1})  as n4 return n1,n2,n3,n4"
                    , parameters);
            result = tt.run(uql);
            assertSuccess(result);

            commitTransaction(tt);

            return ImmutableMap.of();
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> frR(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();

        try {
            // Neo4j: Extract is no longer supported in neo4j >= 5.x, use list comprehension instead
            // Memgraph:  Not yet implemented: atom expression '[a IN nodes(path1)|a.balance]'

            {
                String cypher = "MATCH (a1:Account {id: $accountId})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(a1)\n"
                        + "RETURN [a1.balance, a2.balance, a3.balance, a4.balance] AS firstRead";
            }

            String uql = format("n({@account && _id == $accountId} as a1).re({@transfer}).n(as a2).re({@transfer}).n(as a3).re({@transfer}).n(as a4).re({@transfer}).n(a1) "
                    + "RETURN a1.balance as a1_balance, a2.balance as a2_balance, a3.balance as a3_balance, a4.balance as a4_balance", parameters);
            final UltipaResultSet result1 = tt.run(uql);
            assertSuccess(result1);

            List<Object> firstRead = Collections.emptyList();
            if (result1.count() > 0) {
                firstRead = ImmutableList.of(result1.aliasAsLong("a1_balance"),
                        result1.aliasAsLong("a2_balance"),
                        result1.aliasAsLong("a3_balance"),
                        result1.aliasAsLong("a4_balance")
                );
            }
            if (firstRead.isEmpty()) {
                throw new IllegalStateException("FR2 result1 empty");
            }

            sleep((Long) parameters.get("sleepTime"));
            {
                String cypher = "MATCH (a1:Account {id: $accountId})-[:transfer]->(a2)-[:transfer]->(a3)-[:transfer]->(a4)-[:transfer]->(a1)\n"
                        + "RETURN [a1.balance, a2.balance, a3.balance, a4.balance] AS secondRead";
            }


            final UltipaResultSet result2 = tt.run("n({@account && _id == $accountId} as a1).re({@transfer}).n(as a2).re({@transfer}).n(as a3).re({@transfer}).n(as a4).re({@transfer}).n({this._uuid == a1._uuid}) "
                    + "RETURN a1.balance as a1_balance, a2.balance as a2_balance, a3.balance as a3_balance, a4.balance as a4_balance", parameters);
            assertSuccess(result2);

            List<Object> secondRead = Collections.emptyList();
            if (result2.count() > 0) {
                secondRead = ImmutableList.of(result1.aliasAsLong("a1_balance"),
                        result1.aliasAsLong("a2_balance"),
                        result1.aliasAsLong("a3_balance"),
                        result1.aliasAsLong("a4_balance")
                );
            }
            if (secondRead.isEmpty()) {
                throw new IllegalStateException("FR2 result2 empty");
            }

            return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
        } catch (IllegalStateException e) {
            tt.close();
            throw e;
        } finally {
            tt.close();
        }
    }

    // LU

    @Override
    public void luInit() {
        final UltipaConnection tt = startTransaction();
        try {
            String cypher = "CREATE (:Account {id: 1, numTransferred: 0})";
            UltipaResultSet result1 = tt.run(("insert().into(@account).nodes([{_id: 1,numTransferred:0}])"));
            assertSuccess(result1);
            commitTransaction(tt);
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> luW(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        try {
            UltipaResultSet result1;
            String cypher = "MATCH (a1:Account {id: 1})\n"
                    + "CREATE (a1)-[:transfer]->(a2)\n"
                    + "SET a1.numTransferred = a1.numTransferred + 1\n"
                    + "RETURN a1.numTransferred\n";
            String uql = "find().nodes({_id == 1}) as a1 insert().into(@account).nodes({}) as a2 insert().into(@transfer).edges([{_from:a1._id,_to:a2._id}]) as e1 with e1 update().nodes({_id == 1}).set({numTransferred:a1.numTransferred + 1}) as un return un.numTransferred,e1";
            result1 = tt.run(uql);
            assertSuccess(result1);
            commitTransaction(tt);
            return ImmutableMap.of();
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> luR(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();

        try {
            String cypher = "MATCH (a:Account {id: $accountId})\n"
                    + "OPTIONAL MATCH (a)-[t:transfer]->()\n"
                    + "WITH a, count(t) AS numTransferEdges\n"
                    + "RETURN numTransferEdges,\n"
                    + "       a.numTransferred AS numTransferProp\n";
            String uql = format("find().nodes({@account && _id == $accountId}) as a " +
                    "optional n(a).e({@transfer} as t).n() "
                    + "WITH a, count(t) AS numTransferEdges\n"
                    + "RETURN numTransferEdges,\n"
                    + "       a.numTransferred AS numTransferProp\n", parameters);
            final UltipaResultSet result = tt.run(uql);
            assertSuccess(result);
            long numTransferEdges = result.aliasAsLong("numTransferEdges");
            long numTransferProp = result.aliasAsLong("numTransferProp");
            return ImmutableMap.of("numTransferEdges", numTransferEdges, "numTransferProp", numTransferProp);
        } finally {
            tt.close();
        }

    }

    @Override
    public void wsInit() {
        final UltipaConnection tt = startTransaction();
        try {
            // create 10 pairs of accounts with indices (1,2), ..., (19,20)
            for (int i = 1; i <= 10; i++) {
                String cypher = "CREATE (:Account {id: $account1Id, balance: 70}), (:Account {id: $account2Id, balance: 80})";
                UltipaResultSet result1 = tt.run(format("insert().into(@account).nodes([{_id: $account1Id,balance:70},{_id:$account2Id,balance:80}])",
                        ImmutableMap.of("account1Id", 2 * i - 1, "account2Id", 2 * i)));
                assertSuccess(result1);
            }

            commitTransaction(tt);
        } finally {
            tt.close();
        }
    }

    @Override
    public Map<String, Object> wsW(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        UltipaResultSet result;

        try {
            {
                String cypher = "MATCH (a1:Account {id: $account1Id}), (a2:Account {id: $account2Id})\n"
                        + "WHERE a1.balance + a2.balance >= 100\n"
                        + "RETURN a1, a2";

            }

            result = tt.run(format("find().nodes({@account && _id == $account1Id}) as a1 " +
                            "find().nodes({@account && _id == $account2Id}) as a2 " +
                            "with a1,a2 where (a1.balance + a2.balance) >= 100" +
                            "return a1,a2"
                    , parameters));
            assertSuccess(result);

            if (result.count() > 0) {
                sleep((Long) parameters.get("sleepTime"));

                long accountId = new Random().nextBoolean()
                        ? (long) parameters.get("account1Id") :
                        (long) parameters.get("account2Id");

                String cypher = "MATCH (a:Account {id: $accountId})\n"
                        + "SET a.balance = a.balance - 100";
                result = tt.run(format("update().nodes({@account && _id ==  $accountId}).set({balance:this.balance - 100})",
                        ImmutableMap.of("accountId", accountId)));
                assertSuccess(result);
                commitTransaction(tt);
            }

            return ImmutableMap.of();
        } finally {
            tt.close();
        }

    }

    @Override
    public Map<String, Object> wsR(Map<String, Object> parameters) {
        final UltipaConnection tt = startTransaction();
        // we select pairs of accounts using (id, id+1) pairs

        try {
            String cypher = "MATCH (a1:Account), (a2:Account {id: a1.id+1})\n"
                    + "WHERE a1.balance + a2.balance <= 0 and a1.id % 2 = 1 \n"
                    + "RETURN a1.id AS a1id, a1.balance AS a1balance, a2.id AS a2id, a2.balance AS a2balance";
            final UltipaResultSet result = tt.run((
                    "find().nodes({@account}) as a1 " +
                            "find().nodes({@account && this._id == (a1.id + 1)}) as a2 " +
                            "with a1,a2 " +
                            "where (a1.balance + a2.balance) <= 0 and (a1.id % 2) == 1 " +
                            "RETURN a1.id AS a1id, a1.balance AS a1balance, a2.id AS a2id, a2.balance AS a2balance"));
            assertSuccess(result);

            if (result.count() > 0) {
                return ImmutableMap.of(
                        "a1id", result.aliasAsLong("a1id"),
                        "a1balance", result.aliasAsLong("a1balance"),
                        "a2id", result.aliasAsLong("a2id"),
                        "a2balance", result.aliasAsLong("a2balance"));
            } else {
                return ImmutableMap.of();
            }
        } finally {
            tt.close();
        }
    }

}
