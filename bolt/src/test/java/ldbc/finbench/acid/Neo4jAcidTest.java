package ldbc.finbench.acid;

import ldbc.finbench.acid.bolt.BoltDriver;
import org.junit.BeforeClass;

public class Neo4jAcidTest extends AcidTest<BoltDriver> {

    public Neo4jAcidTest() {
        super(new BoltDriver("neo4j", 7687));
    }

    @BeforeClass
    public static void setUp() {
    }
}
