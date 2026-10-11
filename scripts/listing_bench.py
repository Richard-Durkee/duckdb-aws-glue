"""Times Hive scans that list partitions one directory each, with hive_partition_listing_parallelism 1 (one listing
at a time, as before) and 16, against the local Glue + S3 fixture with network latency added by tc netem."""
import os
import statistics
import subprocess
import sys

DUCKDB = "./build/release/duckdb"
# the extensions are loadable files in the build's repository, unsigned, as the test runner installs them
REPOSITORY = os.path.abspath("build/release/repository")
PREAMBLE = f"""
SET autoinstall_extension_repository = '{REPOSITORY}';
INSTALL httpfs FROM '{REPOSITORY}';
INSTALL glue FROM '{REPOSITORY}';
LOAD httpfs;
LOAD glue;
CREATE SECRET (TYPE S3, KEY_ID 'minio', SECRET 'minio123', REGION 'us-east-1', ENDPOINT 'localhost:9100',
    USE_SSL false, URL_STYLE 'path');
ATTACH '123456789012' AS lake (TYPE glue, ENDPOINT 'http://localhost:5555',
    DEFAULT_LOCATION 's3://glue-test/warehouse/bench');
"""
SETUP = PREAMBLE + """
CREATE TABLE lake.default.listing_bench (id INTEGER, dt VARCHAR) PARTITIONED BY (dt);
INSERT INTO lake.default.listing_bench SELECT i, 'd' || lpad((i % 200)::VARCHAR, 3, '0') FROM range(200000) t(i);
SELECT count(*) AS partitions FROM glue_partitions('lake.default.listing_bench');
"""
NINE = ", ".join(f"'d{i:03d}'" for i in range(9))
QUERIES = {
    "9 partitions (below the default threshold of 10)": f"SELECT count(*) FROM lake.default.listing_bench WHERE dt IN ({NINE});",
    "200 partitions, one listing each": "SET hive_partition_listing_threshold = 1000000;\n.timer on\nSELECT count(*) FROM lake.default.listing_bench;",
}
SETTINGS = [1, 16]
REPEATS = 7


def run(sql):
    result = subprocess.run([DUCKDB, "-batch", "-unsigned"], input=sql, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"duckdb failed:\n{sql}\n{result.stdout}\n{result.stderr}")
    return result.stdout


def time_query(parallelism, query):
    out = run(PREAMBLE + f"SET hive_partition_listing_parallelism = {parallelism};\n" +
              (query if query.startswith("SET") else ".timer on\n" + query))
    times = [float(line.split()[4]) for line in out.splitlines() if line.startswith("Run Time (s): real")]
    return times[-1]


def main():
    print(run(SETUP))
    results = {(name, p): [] for name in QUERIES for p in SETTINGS}
    for _ in range(REPEATS):
        for name, query in QUERIES.items():
            for p in SETTINGS:
                results[(name, p)].append(time_query(p, query))
    lines = ["| Query | parallelism 1 (before) | parallelism 16 | Speedup |", "|---|---|---|---|"]
    for name in QUERIES:
        before = statistics.median(results[(name, 1)])
        after = statistics.median(results[(name, 16)])
        lines.append(f"| {name} | {before:.3f} s | {after:.3f} s | {before / after:.1f}x |")
    table = "\n".join(lines)
    print(table)
    print({f"{k[0]} / {k[1]}": v for k, v in results.items()})
    with open(sys.argv[1], "a") as summary:
        summary.write(table + "\n")


main()
