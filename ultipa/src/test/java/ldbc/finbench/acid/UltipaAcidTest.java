package ldbc.finbench.acid;

import ldbc.finbench.acid.ultipa.UltipaDriver;
import org.junit.BeforeClass;

public class UltipaAcidTest extends AcidTest<UltipaDriver> {


    public UltipaAcidTest() {
        super(new UltipaDriver("http://xx.xxx.x.xxx:xxxx", "xxx.xxx.x.xx", 666666, "xxxx", "xxxxxx").reset());
    }

    @BeforeClass
    public static void setUp() {
    }
}
