## Galaxybase @ LDBC FinBench ACID tests

See the chapter on "ACID tests" in
the [LDBC FinBench specification](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf)

## Galaxybase

- https://www.galaxybase.com/

## Getting started

Once Galaxybase, the graph database, has been pre-installed, you can run the following command to conduct an ACID test.

```shell
sh galaxybase-acid.sh
```

## Test results

| Database | C                  | RB                 | Isolation Level | G0                 | G1a                | G1c                | OTV                | FR                 | IMP                | PMP                | LU                 | WS                 |
|----------|--------------------|--------------------|-----------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|
| Galaxybase  | :white_check_mark: | :white_check_mark: | Serializable    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

> tip

* :white_check_mark: indicates passing the test
* :x: indicates failing the test
