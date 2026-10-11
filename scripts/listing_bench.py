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
# (label, threads, warm): warm runs the query once untimed in the same process with the parquet metadata cache on, so
# the timed run reads footers from memory and lists again (listings are cached per query only)
SCENARIOS = [
    ("cold, default threads (4)", None, False),
    ("cold, 32 threads", 32, False),
    ("warm caches, default threads (4)", None, True),
]
REPEATS = 5


def run(sql):
    result = subprocess.run([DUCKDB, "-batch", "-unsigned"], input=sql, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"duckdb failed:\n{sql}\n{result.stdout}\n{result.stderr}")
    return result.stdout


def time_query(parallelism, query, threads, warm):
    sql = PREAMBLE + f"SET hive_partition_listing_parallelism = {parallelism};\n"
    if threads:
        sql += f"SET threads = {threads};\n"
    if query.startswith("SET"):
        setting, select = query.split("\n.timer on\n")
        sql += setting + "\n"
    else:
        select = query
    if warm:
        sql += "SET parquet_metadata_cache = true;\n" + select + "\n"
    sql += ".timer on\n" + select + "\n"
    out = run(sql)
    times = [float(line.split()[4]) for line in out.splitlines() if line.startswith("Run Time (s): real")]
    return times[-1]


def main():
    print(run(SETUP))
    lines = ["| Scenario | Query | parallelism 1 (before) | parallelism 16 | Speedup |", "|---|---|---|---|---|"]
    raw = {}
    for label, threads, warm in SCENARIOS:
        results = {(name, p): [] for name in QUERIES for p in SETTINGS}
        for _ in range(REPEATS):
            for name, query in QUERIES.items():
                for p in SETTINGS:
                    results[(name, p)].append(time_query(p, query, threads, warm))
        for name in QUERIES:
            before = statistics.median(results[(name, 1)])
            after = statistics.median(results[(name, 16)])
            lines.append(f"| {label} | {name} | {before:.3f} s | {after:.3f} s | {before / after:.1f}x |")
        raw[label] = {f"{k[0]} / {k[1]}": v for k, v in results.items()}
    table = "\n".join(lines)
    print(table)
    print(raw)
    with open(sys.argv[1], "a") as summary:
        summary.write(table + "\n")


DIAGNOSE = """
SET redact_http_logs = false;
SET hive_partition_listing_threshold = 1000000;
CALL enable_logging('HTTP', storage := 'memory');
CALL truncate_duckdb_logs();
SELECT count(*) FROM lake.default.listing_bench;
CREATE TEMP TABLE r AS
SELECT CASE WHEN request.url LIKE '%list-type=2%' THEN 'list'
            WHEN request.url LIKE '%:5555%' THEN 'glue'
            ELSE request.type END AS kind,
       request.start_time AS s,
       request.start_time + to_milliseconds(request.duration_ms) AS e,
       request.duration_ms AS d
FROM duckdb_logs_parsed('HTTP');
SELECT kind, count(*) AS requests, sum(d) AS total_ms,
       epoch_ms(max(e)) - epoch_ms(min(s)) AS span_ms,
       epoch_ms(min(s)) - (SELECT epoch_ms(min(s)) FROM r) AS first_at_ms,
       (SELECT max(c) FROM (SELECT count(*) AS c FROM r a JOIN r b ON a.kind = b.kind AND b.s <= a.s AND b.e > a.s
                            WHERE a.kind = r.kind GROUP BY a.s)) AS max_concurrent
FROM r GROUP BY kind ORDER BY first_at_ms;
SELECT kind, epoch_ms(s) - (SELECT epoch_ms(min(s)) FROM r) AS at_ms, d AS ms FROM r ORDER BY s LIMIT 40;
"""


def diagnose():
    for p in SETTINGS:
        print(f"=== HTTP timeline, parallelism {p}, 32 threads, cold ===")
        print(run(PREAMBLE + f"SET hive_partition_listing_parallelism = {p};\nSET threads = 32;\n" + DIAGNOSE))


main()
diagnose()
