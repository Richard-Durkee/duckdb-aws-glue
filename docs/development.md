# Development

## Building

```sh
VCPKG_TOOLCHAIN_PATH='<path to vcpkg>/scripts/buildsystems/vcpkg.cmake' GEN=ninja make relassert
ASAN_OPTIONS=detect_container_overflow=0 ./build/relassert/test/unittest test/sql/test_glue_attach.test
```

## Testing

The tests are written against two `--test-config` files, which decide where the catalog and the storage are:

- `test/configs/local_glue.json`: [moto](https://github.com/getmoto/moto) serving the Glue API and
  [SeaweedFS](https://github.com/seaweedfs/seaweedfs) serving S3, both from `scripts/docker-compose.yml`, which also
  creates the bucket, the Glue database `default` and the bucketed table `default.fixture_bucketed_multi` (bucket
  columns without a NumberOfBuckets, which the extension does not create).
- `test/configs/cloud_glue.json`: a live AWS Glue Data Catalog, with credentials from the AWS credential chain.

A config creates the S3 secret (`on_init`) and sets `GLUE_CATALOG_ID`, `GLUE_ENDPOINT` and `DEFAULT_S3_LOCATION`,
which the tests use in their ATTACH; tests are skipped without a config (`require-env GLUE_CATALOG_ID`). Tests under
`test/sql/cloud/` read tables of the live account that the tests do not create and only run with the cloud config.
The local config also sets `GLUE_FAULT_ENDPOINT`: `scripts/glue_fault_proxy.py` in front of moto, which answers
GetTables on a database named `fault_access_denied*` / `fault_invalid_input*` with that Glue error, and carries out
CreateDatabase / CreateTable on a `fault_already_exists*` database before answering AlreadyExistsException (what it
creates differs from what was asked for), and refuses the first attempt of each request on a `fault_throttled*`
database with ThrottlingException, letting the SDK's retries through. A table named `fault_denied_*` is one the caller
may not describe, as Lake Formation leaves it: GetTable on it fails with AccessDeniedException and GetTables leaves it
out. Tests that need the proxy are skipped without it. It
also sets `GLUE_LINK_PROXY_ENDPOINT`: `scripts/resource_link_proxy.py` in front of moto, which answers for the database
`link_db` as Glue does through a database resource link whose target is `target_db`.

```sh
make glue-fixture        # docker compose up (creates the bucket and the 'default' database)
make test-local          # unittest --test-config test/configs/local_glue.json 'test/sql/*', with retries
make glue-fixture-down

AWS_PROFILE=... AWS_CONFIG_FILE=~/.aws/config AWS_SHARED_CREDENTIALS_FILE=~/.aws/credentials make test-cloud
```

Both targets set `AWS_EC2_METADATA_DISABLED=true`: the test runner hides `~/.aws`, and without a region from the
environment or a profile the AWS SDK asks the EC2 instance metadata service for one, which off EC2 hangs for
minutes per client. A test config can not export process environment variables, so this stays on the command.

`make test-local` runs the tests through DuckDB's `duckdb/scripts/ci/run_tests.py` (Python 3.10+; pick the
interpreter with `PYTHON=python3.14`), one at a time in batches of `TEST_BATCH_SIZE` (10) tests per process, and reruns
a failing batch up to twice: against the local servers a read right after a write occasionally comes back with no rows.
Every retry is reported in the output. `TEST_BUILD=release` runs the `release` build instead of `relassert`. Every
process first installs the loadable extensions from `build/<type>/repository`; on Linux the debug info of `relassert`
makes them ~1.5 GB each, so CI tests a `make release EXT_RELEASE_FLAGS=-DFORCE_ASSERT=1` build (assertions and
sanitizers, no debug info).

The `Cloud Glue tests` workflow (`.github/workflows/CloudGlueTests.yml`) runs the whole suite, on a release build,
against a live catalog. It runs on every push to `main`; when such a run fails, its job summary lists the failing tests
and the merges since the last passing run on `main`. On a pull request it runs when a maintainer adds the `cloud-tests`
label, and again on every push to a labeled PR whose author is a member or collaborator of the repository. Since the
PR's code runs with the AWS credentials, read an outside contributor's PR before labeling it, and remove and re-add the
label to test their later commits. A maintainer can also start it by hand (`gh workflow run CloudGlueTests.yml -f
pr_number=<n>`, or `gh workflow run CloudGlueTests.yml --ref main` without a PR). Starting it by hand or through the
label is refused for anyone below the `maintain` role. A run tests the PR's head commit (or the branch's) as it was when
the run started, and reports the result as a `Cloud Glue tests` status on that commit. Runs share the catalog, so they
go one at a time; GitHub keeps only the newest waiting run, which is why a failing run on `main` compares with the last
passing one rather than the previous commit. Every run first deletes all Glue databases of the account and creates an
empty `default`. The catalog, region and AWS credentials come from the variables and secrets of the `cloud-glue`
environment, listed at the top of the workflow. The tests write to the bucket `duckdb-glue-ci-<account id>-<region>`,
which the workflow creates if needed, under `duckdb-aws-glue-cloud-tests/ci-<run id>-<attempt>/`, which it deletes when
the job ends, whether the tests pass or not; a lifecycle rule on `duckdb-aws-glue-cloud-tests/` expires anything a
failed run leaves behind after 2 days.

Every test creates the tables it needs and writes under its own `{TEST_DIR}` prefix, so runs do not interfere with
each other; `make glue-fixture-down` throws the containers and their data away.

The benchmarks under `benchmark/` read from the same local servers. They build their tables in the `load` step and
use `debug_fs_delay_mean_ms` to add latency to every file open and read, standing in for the S3 round trip the local
SeaweedFS does not have (`make glue-fixture` first; the benchmark runner needs a build with `BUILD_BENCHMARK=1`):

```sh
AWS_EC2_METADATA_DISABLED=true ./build/relassert/benchmark/benchmark_runner benchmark/heavily_partitioned_table.benchmark
```

`benchmark/tpch/<sf>/<format>/` runs the 22 TPC-H queries against Hive tables of that format in the Glue database
`bench_tpch_<sf>_<format>` (`lineitem` and `orders` partitioned by 10-day buckets of `l_shipdate` and `o_orderdate`)
and checks the answers: `sf1/parquet/` at SF1, `sf0.1/csv/`, `sf0.1/json/` and `sf0.1/avro/` at SF0.1 (the Glue
database names spell the scale factor `sf1`, `sf0_1`, ...). The queries in `benchmark/tpch/queries/` are DuckDB's with
filters on the bucket columns added next to the date filters. The first run generates the data with `dbgen` and
writes it with CTAS, which takes a while; later runs reuse `duckdb_benchmark_data/glue_tpch_<sf>_<format>.duckdb`,
which `make glue-fixture` removes:

```sh
AWS_EC2_METADATA_DISABLED=true ./build/relassert/benchmark/benchmark_runner 'benchmark/tpch/sf1/parquet/.*'
```

`benchmark/tpcds/<sf>/<format>/` does the same for the 99 TPC-DS queries, `sf1/parquet/` at SF1 and the other formats
at SF0.01 (`sf0.01/<format>/`), with the Glue database `bench_tpcds_<sf>_<format>`: the fact tables are partitioned by
10-day buckets of their date key (`ss_sold_date_bucket`, `sr_returned_date_bucket`, `cs_sold_date_bucket`,
`cr_returned_date_bucket`, `ws_sold_date_bucket`, `wr_returned_date_bucket`, `inv_date_bucket`, each
`(date_sk - 2440588) // 10`, the same buckets as the TPC-H tables), the dimension tables are not. The queries in
`benchmark/tpcds/queries/` are DuckDB's with filters on the bucket columns added where a fact table's date key is
joined to a filtered `date_dim`. Its cache is `duckdb_benchmark_data/glue_tpcds_<sf>_<format>.duckdb`:

```sh
AWS_EC2_METADATA_DISABLED=true ./build/release/benchmark/benchmark_runner 'benchmark/tpcds/sf1/parquet/.*'
```

Both suites share a template per suite (`benchmark/tpch/tpch.benchmark.in`, `benchmark/tpcds/tpcds.benchmark.in`)
that takes `FORMAT`, `SF` and `SF_NAME`. The TPC-DS load needs a build without assertions
(`BUILD_BENCHMARK=1 make release`): its fact tables have rows with a NULL date key, and writing such a partition trips
a `D_ASSERT` in DuckDB's partitioned copy (see the header of `benchmark/tpcds/tpcds.benchmark.in` for a repro without
Glue).

`benchmark/{tpch,tpcds}/sf10/parquet/` run the same queries at SF10 (the TPC-DS load writes ~4 GB to S3), and
`benchmark/{tpch,tpcds}/sf0.01/parquet/` at SF0.01, which has as many partitions and files as SF1 but almost no rows,
so its timings are mostly the Glue calls, the S3 listings and the file opens. Neither runs in CI. (TPC-H q17 fails at
SF0.01: DuckDB's answer file for it is empty, the query returns one NULL.)

Both loads create their Glue tables with `CREATE TABLE IF NOT EXISTS ... AS`, so changing a load has no effect while
the tables are in Glue: rebuild the fixture with `make glue-fixture-down && make glue-fixture` first.

`benchmark_runner` runs the benchmarks below its own root (the repository), not the working directory. DuckDB's own
TPC-H and TPC-DS benchmarks on native tables, the reference for these, run with
`--root-dir duckdb` (`'benchmark/tpcds/sf1/.*' --sf 10` for TPC-DS at SF10).

`.github/workflows/Regression.yml` builds the benchmark runner for a PR and for its merge base once, then compares
their timings (5 runs each) in two jobs of about three minutes, so it runs only a few join-heavy TPC-H and TPC-DS
queries. The parquet job runs `benchmark/*.benchmark`, the parquet benchmarks of `benchmark/pushdown/` and
`benchmark/optimizer/` and TPC-DS q64 at SF1. The csv/json job runs the csv and json benchmarks of
`benchmark/pushdown/` and `benchmark/optimizer/`, TPC-H q09, q18 and q21 at SF0.1 and TPC-DS q14, q64, q72 and q78 at SF0.01 on csv tables. The
other queries and the json and avro TPC benchmarks only run locally.
