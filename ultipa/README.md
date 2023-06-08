## Ultipa Graph @ LDBC FinBench ACID tests

This is the module for LDBC FinBench ACID tests on Ultipa Graph.

## Getting started

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
