# LDBC FinBench ACID tests

See the chapter on "ACID tests" in
the [LDBC FinBench specification](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf)

## Systems

* Neo4j

## Getting started

### Neo4j && Memgraph

Version 3.5.x:

```bash
docker run  --rm -p 7474:7474 -p 7687:7687 --env=NEO4J_AUTH=none neo4j:3.5.20
```

Version 4.1.x:

```bash
docker run  --rm -p 7474:7474 -p 7687:7687 --env=NEO4J_AUTH=none neo4j:4.1.1
```

Version 5.5.x:

```bash
docker run  --rm -p 7474:7474 -p 7687:7687 --env=NEO4J_AUTH=none neo4j:5.5.0
```

MemGraph

```bash
docker run -it --rm -p 7687:7687 memgraph/memgraph-platform:2.6.5-memgraph2.5.2-lab2.4.0-mage1.6
```

Run tests:

```bash
mvn -Dtest=Neo4jAcidTest test
```

| Database | C                  | RB                 | Isolation Level | G0                 | G1a                | G1c                | OTV                | FR                 | IMP                | PMP                | LU                 | WS  |
| -------- | ------------------ | ------------------ | --------------- | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | --- |
| Neo4j 3  | :white_check_mark: | :white_check_mark: | Read Committed  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :white_check_mark: | :x: |
| Neo4j 4  | :white_check_mark: | :white_check_mark: | Read Committed  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x:                | :x:                | :x:                | :x:                | :white_check_mark: | :x: |
| Memgraph | :white_check_mark: | :white_check_mark: | Snapshot  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :x: |
| Galaxybase  | :white_check_mark: | :white_check_mark: | Serializable    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

> tip

* :white_check_mark: indicates passing the test
* :x: indicates failing the test
