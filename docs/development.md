# Development

## Building

```sh
VCPKG_TOOLCHAIN_PATH='<path to vcpkg>/scripts/buildsystems/vcpkg.cmake' GEN=ninja make relassert
ASAN_OPTIONS=detect_container_overflow=0 ./build/relassert/test/unittest test/sql/test_glue_attach.test
```

## Tests

The tests run against one of two `--test-config` files, which decide where the catalog and the storage live:

| Config | Catalog and storage |
|---|---|
| `test/configs/local_glue.json` | [moto](https://github.com/getmoto/moto) serves the Glue API and [SeaweedFS](https://github.com/seaweedfs/seaweedfs) serves S3, both started from `scripts/docker-compose.yml`. |
| `test/configs/cloud_glue.json` | A live AWS Glue Data Catalog, with credentials from the AWS credential chain. |

A config creates the S3 secret (`on_init`) and sets `GLUE_CATALOG_ID`, `GLUE_ENDPOINT` and `DEFAULT_S3_LOCATION`,
which the tests use in their `ATTACH`. Without a config, every test is skipped (`require-env GLUE_CATALOG_ID`). The
tests under `test/sql/cloud/` read tables of the live account that the tests do not create, so they only run with the
cloud config.

Every test creates the tables it needs and writes under its own `{TEST_DIR}` prefix, so runs do not interfere with
each other.

### Running them

```sh
make glue-fixture        # docker compose up: creates the bucket and the 'default' database
make test-local          # unittest --test-config test/configs/local_glue.json 'test/sql/*', with retries
make glue-fixture-down   # removes the containers and their data

AWS_PROFILE=... AWS_CONFIG_FILE=~/.aws/config AWS_SHARED_CREDENTIALS_FILE=~/.aws/credentials make test-cloud
```

`make glue-fixture` creates the bucket, the Glue database `default`, and the bucketed table
`default.fixture_bucketed_multi`. That table has bucket columns but no `NumberOfBuckets`, which the extension itself
never creates.

Both test targets set `AWS_EC2_METADATA_DISABLED=true`. The test runner hides `~/.aws`, and without a region from the
environment or a profile, the AWS SDK asks the EC2 instance metadata service for one, which off EC2 hangs for minutes
per client. A test config cannot export process environment variables, so the variable has to stay on the command.

`make test-local` runs the tests through DuckDB's `duckdb/scripts/ci/run_tests.py`, which needs Python 3.10 or later;
pick the interpreter with `PYTHON=python3.14`. It runs the tests one at a time, in batches of `TEST_BATCH_SIZE` (10)
tests per process, and reruns a failing batch up to twice, since against the local servers a read right after a write
occasionally returns no rows. Every retry is reported in the output. `TEST_BUILD=release` runs the `release` build
instead of `relassert`.

Each process first installs the loadable extensions from `build/<type>/repository`. On Linux, the debug info of
`relassert` makes them about 1.5 GB each, so CI tests a `make release EXT_RELEASE_FLAGS=-DFORCE_ASSERT=1` build
instead, which has assertions and sanitizers but no debug info.

### Fault proxies

The local config also starts two proxies in front of moto, for behavior that moto does not reproduce. Tests that
need a proxy are skipped without it.

`GLUE_FAULT_ENDPOINT` points at `scripts/glue_fault_proxy.py`, which injects Glue errors by database or table name:

| Name | Behavior |
|---|---|
| database `fault_access_denied*`, `fault_invalid_input*` | `GetTables` fails with that Glue error. |
| database `fault_already_exists*` | `CreateDatabase` and `CreateTable` are carried out, then answered with `AlreadyExistsException`. What the proxy creates differs from what was asked for. |
| database `fault_throttled*` | The first attempt of each request fails with `ThrottlingException`, and the SDK's retries go through. |
| table `fault_denied_*` | A table the caller may not describe, as Lake Formation leaves it. `GetTable` fails with `AccessDeniedException`, and `GetTables` leaves the table out. |

`GLUE_LINK_PROXY_ENDPOINT` points at `scripts/resource_link_proxy.py`, which answers for the database `link_db` the
way Glue does through a database resource link whose target is `target_db`.

### Cloud tests in CI

The `Cloud Glue tests` workflow (`.github/workflows/CloudGlueTests.yml`) runs the whole suite on a release build
against a live catalog.

**When it runs.** It runs on every push to `main`. When such a run fails, its job summary lists the failing tests and
the merges since the last passing run on `main`. On a pull request, it runs when a maintainer adds the `cloud-tests`
label, and again on every push to a labeled PR whose author is a member or collaborator of the repository. A
maintainer can also start it by hand, with `gh workflow run CloudGlueTests.yml -f pr_number=<n>`, or with
`gh workflow run CloudGlueTests.yml --ref main` for no PR. Anyone below the `maintain` role is refused, whether they
start it by hand or through the label.

**Outside contributors.** The PR's code runs with the AWS credentials, so read an outside contributor's PR before
labeling it. To test their later commits, remove the label and add it again.

**What it tests.** A run tests the PR's head commit, or the branch's, as it was when the run started. It reports the
result as a `Cloud Glue tests` status on that commit.

**One at a time.** Runs share the catalog, so they go one at a time. GitHub keeps only the newest waiting run, which
is why a failing run on `main` is compared with the last passing run rather than with the previous commit.

**State.** Every run first deletes all Glue databases of the account and creates an empty `default`. The catalog,
region and AWS credentials come from the variables and secrets of the `cloud-glue` environment, listed at the top of
the workflow. The tests write to the bucket `duckdb-glue-ci-<account id>-<region>`, which the workflow creates if
needed, under `duckdb-aws-glue-cloud-tests/ci-<run id>-<attempt>/`. The workflow deletes that prefix when the job
ends, whether the tests pass or not, and a lifecycle rule on `duckdb-aws-glue-cloud-tests/` expires anything a failed
run leaves behind after two days.

## Benchmarks

The benchmarks under `benchmark/` read from the same local servers, so run `make glue-fixture` first. The benchmark
runner needs a build with `BUILD_BENCHMARK=1`. The benchmarks build their tables in the `load` step, and they use
`debug_fs_delay_mean_ms` to add latency to every file open and read, which stands in for the S3 round trip that the
local SeaweedFS does not have.

```sh
AWS_EC2_METADATA_DISABLED=true ./build/relassert/benchmark/benchmark_runner benchmark/heavily_partitioned_table.benchmark
```

`benchmark_runner` runs the benchmarks below its own root, which is the repository, not the working directory.
DuckDB's own TPC-H and TPC-DS benchmarks on native tables, which are the reference for the ones below, run with
`--root-dir duckdb`; for TPC-DS at SF10, use `'benchmark/tpcds/sf1/.*' --sf 10`.

### TPC-H

`benchmark/tpch/<sf>/<format>/` runs the 22 TPC-H queries against Hive tables of that format and checks the answers.
The tables live in the Glue database `bench_tpch_<sf>_<format>`, whose name spells the scale factor as `sf1`,
`sf0_1`, and so on. `lineitem` and `orders` are partitioned by 10-day buckets of `l_shipdate` and `o_orderdate`.

| Directory | Scale factor |
|---|---|
| `sf1/parquet/` | SF1 |
| `sf0.1/csv/`, `sf0.1/json/`, `sf0.1/avro/` | SF0.1 |

The queries in `benchmark/tpch/queries/` are DuckDB's, with filters on the bucket columns added next to the date
filters. The first run generates the data with `dbgen` and writes it with `CREATE TABLE ... AS`, which takes a while.
Later runs reuse `duckdb_benchmark_data/glue_tpch_<sf>_<format>.duckdb`, which `make glue-fixture` removes.

```sh
AWS_EC2_METADATA_DISABLED=true ./build/relassert/benchmark/benchmark_runner 'benchmark/tpch/sf1/parquet/.*'
```

### TPC-DS

`benchmark/tpcds/<sf>/<format>/` does the same for the 99 TPC-DS queries, with `sf1/parquet/` at SF1 and the other
formats at SF0.01 (`sf0.01/<format>/`), in the Glue database `bench_tpcds_<sf>_<format>`. Its cache is
`duckdb_benchmark_data/glue_tpcds_<sf>_<format>.duckdb`.

The fact tables are partitioned by 10-day buckets of their date key, and the dimension tables are not. The bucket
columns are `ss_sold_date_bucket`, `sr_returned_date_bucket`, `cs_sold_date_bucket`, `cr_returned_date_bucket`,
`ws_sold_date_bucket`, `wr_returned_date_bucket` and `inv_date_bucket`, each computed as `(date_sk - 2440588) // 10`,
which gives the same buckets as the TPC-H tables. The queries in `benchmark/tpcds/queries/` are DuckDB's, with filters
on the bucket columns added wherever a fact table's date key is joined to a filtered `date_dim`.

```sh
AWS_EC2_METADATA_DISABLED=true ./build/release/benchmark/benchmark_runner 'benchmark/tpcds/sf1/parquet/.*'
```

The TPC-DS load needs a build without assertions (`BUILD_BENCHMARK=1 make release`). Its fact tables have rows with a
NULL date key, and writing such a partition trips a `D_ASSERT` in DuckDB's partitioned copy. The header of
`benchmark/tpcds/tpcds.benchmark.in` has a repro without Glue.

### Other scale factors

`benchmark/{tpch,tpcds}/sf10/parquet/` run the same queries at SF10, where the TPC-DS load writes about 4 GB to S3.
`benchmark/{tpch,tpcds}/sf0.01/parquet/` run them at SF0.01, which has as many partitions and files as SF1 but almost
no rows, so its timings are mostly the Glue calls, the S3 listings and the file opens. Neither runs in CI. TPC-H q17
fails at SF0.01, because DuckDB's answer file for it is empty while the query returns one NULL.

### Shared templates and caveats

Each suite has one template, `benchmark/tpch/tpch.benchmark.in` and `benchmark/tpcds/tpcds.benchmark.in`, which takes
`FORMAT`, `SF` and `SF_NAME`.

Both loads create their Glue tables with `CREATE TABLE IF NOT EXISTS ... AS`, so a change to a load has no effect
while the tables are still in Glue. Rebuild the fixture first with `make glue-fixture-down && make glue-fixture`.

### Regression checks in CI

`.github/workflows/Regression.yml` builds the benchmark runner once for a PR and once for its merge base, then
compares their timings over five runs each. It runs in two jobs of about three minutes, so it covers only a few
join-heavy TPC-H and TPC-DS queries:

- the Parquet job runs `benchmark/*.benchmark`, the Parquet benchmarks of `benchmark/pushdown/` and
  `benchmark/optimizer/`, and TPC-DS q64 at SF1;
- the CSV/JSON job runs the CSV and JSON benchmarks of `benchmark/pushdown/` and `benchmark/optimizer/`, TPC-H q09,
  q18 and q21 at SF0.1, and TPC-DS q14, q64, q72 and q78 at SF0.01 on CSV tables.

The other queries and the JSON and Avro TPC benchmarks only run locally.
