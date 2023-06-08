## Ultipa Graph @ LDBC FinBench ACID tests

This is the module for LDBC FinBench ACID tests on Ultipa Graph.

**Note: Cypher used in this module is only for the purpose of comparison and reference with UQL.

## Getting started
Modify test address

```
public UltipaAcidTest() {
//super(new UltipaDriver("http://xx.xxx.x.xxx:xxxx", "xxx.xxx.x.xx", 666666, "xxxx", "xxxxxx").reset());
}

httpServer :  ultipaServer gateway
host :    ultipaServer ip
port :    ultipaServer port
username : ultipaServer username
password : ultipaServer  password
```


Run tests:

```bash
cd ultipa
mvn -Dtest=UltipaAcidTest test
```

| Database  | C                  | RB                 | Isolation Level | G0                 | G1a                | G1c                | OTV                | FR                 | IMP                | PMP                | LU                 | WS  |
|-----------|--------------------|--------------------|-----------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|-----|
| Ultipa Graph | :white_check_mark: | :white_check_mark: | Serializable  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark:                | :white_check_mark:                | :white_check_mark:                | :white_check_mark: | :white_check_mark: |

> tip

* :white_check_mark: indicates passing the test
* :x: indicates failing the test
