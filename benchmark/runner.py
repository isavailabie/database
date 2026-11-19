#!/usr/bin/env python3
"""Build a source snapshot and run every TSDB workload in a fresh process/db.

Layout of an output directory (never reused):

    <output>/source/            immutable copy of the submitted tree
    <output>/build/             Release build of the workload programs
    <output>/<case>-p<N>-t<T>-r<R>/run.log
    <output>/metrics.csv        one row per (case, phase, repeat)
    <output>/aggregate.json     median/min/max over repeats
    <output>/summary.json       overall status and every run record
"""
import argparse
import csv
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

PROFILES = {
    "smoke": dict(points=100_000, seconds=10, repeat=1),
    "quick": dict(points=1_000_000, seconds=60, repeat=1),
    "standard": dict(points=10_000_000, seconds=600, repeat=3),
    "full": dict(points=100_000_000, seconds=3600, repeat=1),
}

# case name -> workload program and its switches
CASES = {
    "perf_ingest": dict(kind="ingest", hot=False),
    "perf_ingest_hot": dict(kind="ingest", hot=True),
    "perf_mixed": dict(kind="mixed", hot=False),
    "perf_mixed_hot": dict(kind="mixed", hot=True),
    "perf_recover": dict(kind="recover"),
}

IGNORED = {".git", "build", "bench-results", "__pycache__", ".venv", "out"}

METRIC_FIELDS = [
    "case", "phase", "points", "threads", "seconds", "repeat", "ops",
    "ops_per_second", "p50_us", "p95_us", "p99_us", "rss_bytes", "disk_bytes",
    "wal_bytes",
]


def dump(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def copy_source(source, dest, extra_ignored=()):
    def ignore(_, names):
        return [n for n in names
                if n in IGNORED or n.startswith("build") or n in extra_ignored]
    shutil.copytree(source, dest, ignore=ignore)


def run(command, log, timeout, cwd):
    start = time.monotonic()
    with log.open("w") as out:
        out.write("COMMAND " + json.dumps([str(x) for x in command]) + "\n")
        out.flush()
        proc = subprocess.Popen(
            [str(x) for x in command], stdout=out, stderr=subprocess.STDOUT,
            start_new_session=True, cwd=cwd,
        )
        try:
            code = proc.wait(timeout=timeout)
            status = "pass" if code == 0 else "fail"
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            code, status = proc.returncode, "timeout"
    return dict(status=status, returncode=code,
                seconds=time.monotonic() - start, log=str(log))


def parse_kv(text):
    data = {}
    for token in text.split():
        if "=" in token:
            key, value = token.split("=", 1)
            data[key] = value
    return data


def number(data, key, default=0.0):
    try:
        return float(data.get(key, default))
    except (TypeError, ValueError):
        return float(default)


def rate(ops, seconds):
    return ops / seconds if seconds else 0.0


def metrics_for(case, data):
    """Translate one workload's key=value line into (phase, metric) rows."""
    rows = []
    common = dict(rss_bytes=number(data, "rss_bytes"),
                  disk_bytes=number(data, "disk_bytes"),
                  wal_bytes=number(data, "wal_bytes"))
    if case == "perf_recover":
        ops = number(data, "recovered_points")
        seconds = number(data, "open_seconds")
        rows.append(dict(phase="recover", ops=ops, seconds=seconds,
                         ops_per_second=rate(ops, seconds),
                         p50_us=0.0, p95_us=0.0, p99_us=0.0, **common))
        return rows
    if case.startswith("perf_ingest"):
        rows.append(dict(phase="write", ops=number(data, "points"),
                         seconds=number(data, "seconds"),
                         ops_per_second=number(data, "throughput"),
                         p50_us=0.0, p95_us=0.0, p99_us=0.0, **common))
        ops = number(data, "recovered_points")
        seconds = number(data, "recovery_seconds")
        rows.append(dict(phase="recovery", ops=ops, seconds=seconds,
                         ops_per_second=rate(ops, seconds),
                         p50_us=0.0, p95_us=0.0, p99_us=0.0, **common))
        return rows
    rows.append(dict(phase="write", ops=number(data, "written"),
                     seconds=number(data, "seconds"),
                     ops_per_second=number(data, "write_throughput"),
                     p50_us=0.0, p95_us=0.0, p99_us=0.0, **common))
    rows.append(dict(phase="scan", ops=number(data, "scans"),
                     seconds=number(data, "seconds"),
                     ops_per_second=number(data, "scan_throughput"),
                     p50_us=number(data, "scan_p50_us"),
                     p95_us=number(data, "scan_p95_us"),
                     p99_us=number(data, "scan_p99_us"), **common))
    return rows


def variants(case, cfg, matrix, threads):
    """Scale/thread variants for the matrix run; one variant otherwise."""
    sizes = sorted({max(1, cfg["points"] // 10), cfg["points"]}) if matrix else [cfg["points"]]
    thread_counts = sorted({1, threads}) if matrix and case.startswith("perf_mixed") else [threads]
    return [(size, count) for size in sizes for count in thread_counts]


def command_for(case, spec, binary, work, size, threads, seconds):
    db = work / "db"
    if spec["kind"] == "ingest":
        cmd = [binary["bench"], size, "--series", 10000, "--path", db]
        if spec["hot"]:
            cmd += ["--hot-series", 8, "--hot-fraction", 0.5]
        return cmd, 3600
    if spec["kind"] == "mixed":
        cmd = [binary["mixed"], seconds, "--writers", threads, "--readers", 16,
               "--path", db]
        if spec["hot"]:
            cmd += ["--hot"]
        return cmd, seconds + 600
    # recover: write without Close, then measure the reopen
    return [binary["bench"], size, "--crash", "--path", db], 3600


def execute(case, spec, binary, work, log, size, threads, seconds, timeout):
    if spec["kind"] != "recover":
        cmd, limit = command_for(case, spec, binary, work, size, threads, seconds)
        return run(cmd, log, min(timeout, limit), cwd=work)
    # Write without Close, then measure how long the reopen takes.
    crash_log = log.with_name("crash.log")
    failure = run([binary["bench"], size, "--crash", "--path", work / "db"],
                  crash_log, timeout, work)
    if failure["status"] != "pass":
        return failure
    return run([binary["recover"], work / "db"], log, timeout, work)


def summarize(records, out):
    rows = []
    for record in records:
        for metric in record.get("metrics", []):
            rows.append(dict(case=record["case"], phase=metric["phase"],
                             points=record["points"], threads=record["threads"],
                             seconds=metric["seconds"], repeat=record["repeat"],
                             ops=metric["ops"],
                             ops_per_second=metric["ops_per_second"],
                             p50_us=metric["p50_us"], p95_us=metric["p95_us"],
                             p99_us=metric["p99_us"],
                             rss_bytes=metric["rss_bytes"],
                             disk_bytes=metric["disk_bytes"],
                             wal_bytes=metric["wal_bytes"]))
    if rows:
        with (out / "metrics.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=METRIC_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
    groups = {}
    for row in rows:
        key = (row["case"], row["phase"], row["points"], row["threads"])
        groups.setdefault(key, []).append(row)
    aggregate = []
    for key, values in sorted(groups.items()):
        aggregate.append(dict(
            case=key[0], phase=key[1], points=key[2], threads=key[3],
            repeats=len(values),
            median_seconds=statistics.median(v["seconds"] for v in values),
            median_ops_per_second=statistics.median(v["ops_per_second"] for v in values),
            min_ops_per_second=min(v["ops_per_second"] for v in values),
            max_ops_per_second=max(v["ops_per_second"] for v in values),
            median_p99_us=statistics.median(v["p99_us"] for v in values),
            max_rss_bytes=max(v["rss_bytes"] for v in values),
            max_disk_bytes=max(v["disk_bytes"] for v in values),
        ))
    dump(out / "aggregate.json", aggregate)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", choices=sorted(PROFILES), default="quick")
    parser.add_argument("--cases", help="comma-separated case names; default all")
    parser.add_argument("--points", type=int)
    parser.add_argument("--seconds", type=int)
    parser.add_argument("--repeat", type=int)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--matrix", action="store_true",
                        help="run N/10 and N, and 1 vs T writers for mixed")
    parser.add_argument("--timeout", type=int, default=3600,
                        help="per-process seconds")
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()

    cfg = PROFILES[args.profile].copy()
    for key in ("points", "seconds", "repeat"):
        if getattr(args, key) is not None:
            cfg[key] = getattr(args, key)

    source = args.source.resolve()
    out = args.output.resolve()
    if not (source / "CMakeLists.txt").is_file():
        parser.error("--source must be the TSDB project root")
    if out.exists():
        parser.error("--output must be a new directory")
    out.mkdir(parents=True)

    summary = dict(schema_version=1, profile=args.profile, config=cfg,
                   source=str(source), status="running", records=[],
                   matrix=args.matrix, threads=args.threads)
    dump(out / "summary.json", summary)
    records = summary["records"]
    try:
        extra = set()
        try:
            extra = {out.relative_to(source).parts[0]}
        except ValueError:
            pass
        copy_source(source, out / "source", extra)
        build = out / "build"
        if not args.skip_build:
            summary["configure"] = run(
                ["cmake", "-S", out / "source", "-B", build,
                 "-DCMAKE_BUILD_TYPE=Release"],
                out / "configure.log", 600, out)
            if summary["configure"]["status"] != "pass":
                summary["status"] = "configure_failure"
                return 1
            summary["build"] = run(
                ["cmake", "--build", build, "--target", "tsdb_bench",
                 "tsdb_mixed_bench", "tsdb_recover", "-j", args.jobs],
                out / "build.log", 600, out)
            if summary["build"]["status"] != "pass":
                summary["status"] = "build_failure"
                return 1
        binary = dict(bench=build / "tsdb_bench",
                      mixed=build / "tsdb_mixed_bench",
                      recover=build / "tsdb_recover")
        for path in binary.values():
            if not path.is_file():
                parser.error(f"missing binary {path}")

        names = args.cases.split(",") if args.cases else list(CASES)
        unknown = [name for name in names if name not in CASES]
        if unknown:
            parser.error(f"unknown cases: {', '.join(unknown)}")
        summary["executed_cases"] = names
        summary["full_suite"] = set(names) == set(CASES)
        dump(out / "summary.json", summary)

        for name in names:
            spec = CASES[name]
            for size, threads in variants(name, cfg, args.matrix, args.threads):
                for repeat in range(cfg["repeat"]):
                    label = f"{name}-p{size}-t{threads}-r{repeat + 1}"
                    work = out / label
                    work.mkdir()
                    print(label, flush=True)
                    result = execute(name, spec, binary, work, work / "run.log",
                                     size, threads, cfg["seconds"], args.timeout)
                    result.update(case=name, points=size, threads=threads,
                                  repeat=repeat + 1)
                    text = (work / "run.log").read_text(errors="replace")
                    payload = parse_kv(text.splitlines()[-1] if text.splitlines() else "")
                    if result["status"] == "pass":
                        result["metrics"] = metrics_for(name, payload)
                        if not payload:
                            result["status"] = "protocol_failure"
                    records.append(result)
                    dump(out / "summary.json", summary)
        summary["status"] = ("pass" if all(r["status"] == "pass" for r in records)
                             else "fail")
        return 0 if summary["status"] == "pass" else 1
    except Exception as error:  # noqa: BLE001 - report, never hide
        summary.update(status="runner_error", error=str(error))
        return 2
    finally:
        summarize(records, out)
        dump(out / "summary.json", summary)
        print(json.dumps(dict(status=summary["status"], output=str(out),
                              runs=len(records),
                              passed=sum(r["status"] == "pass" for r in records))),
              flush=True)


if __name__ == "__main__":
    sys.exit(main())
