## TuGraph @ LDBC FinBench ACID tests

See the chapter on "ACID tests" in
the [LDBC FinBench specification](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf)

## TuGraph

TuGraph is open-source at [TuGraph on Github](https://github.com/tugraph-family/tugraph-db)

You can also get our support via https://www.tugraph.org/

## Getting started

### Get TuGraph

TuGraph is accessible via Docker Hub. You can pull the image with:

```bash
docker pull tugraph/tugraph-runtime-centos7:${VERSION_TAG}
```

Take Version 3.4.0 for example, you can use:

```bash
docker pull tugraph/tugraph-runtime-centos7:3.4.0
docker run --rm -p 7070:7070 -p 9090:9090 -v {HOST_DIR}:{CONTAINER_DIR} tugraph/tugraph-runtime-centos7:3.4.0
```

### Compile the binary

Run tests:

```bash
cd tugraph
sh compile_embedded.sh acid
```

### Run tests

```bash
./acid
```


## Test results

| Database | C                  | RB                 | Isolation Level | G0                 | G1a                | G1c                | OTV                | FR                 | IMP                | PMP                | LU                 | WS                 |
|----------|--------------------|--------------------|-----------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|
| TuGraph  | :white_check_mark: | :white_check_mark: | Serializable    | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

> tip

* :white_check_mark: indicates passing the test
* :x: indicates failing the test
