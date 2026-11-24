#!/usr/bin/env python3
"""Sequential, alternating baseline/candidate experiments; never reuse a DB.

Both binaries must be Release builds. Run no other workloads while measuring.
The same design.cpp source must be linked against both engine revisions.
"""
import argparse
import json
import math
import statistics
import subprocess
import time
from pathlib import Path


INGEST_METRICS = set("""
points series hot_series seconds throughput write_seconds write_throughput
 drain_seconds drain_compact_seconds rss_bytes disk_bytes sstables
 series_index_entries page_index_entries obsolete_sstables memtable_bytes
 sstable_bytes recovery_seconds flush_count flush_ms compaction_count
 compaction_ms wal_syncs wal_sync_ms wal_bytes_written wal_bytes_replayed
 recovered_points initial_points write_p50_us write_p95_us write_p99_us
""".split())
MIXED_METRICS = set("""
seconds writers readers series_per_writer written write_throughput scans
 scan_throughput scan_errors scan_p50_us scan_p95_us scan_p99_us scan_max_us
 rss_bytes disk_bytes flush_count compaction_count obsolete_sstables wal_bytes
 visible_version commits wal_syncs syncs_per_second points_per_commit
 engine_threads write_errors write_p50_us write_p95_us write_p99_us
 initial_points write_start_timestamp scanned_points historical_read_points
 current_rss_bytes peak_rss_bytes
""".split())
DESIGN_METRICS = set("""
points repeats append_seconds compact_seconds scan_seconds scan_points
 scan_points_per_second rss_bytes compaction_count flush_count recover_seconds
 recovered_points
""".split())


def metrics(text):
    result = {}
    for token in text.split():
        if "=" in token:
            key, value = token.split("=", 1)
            if not key or key in result or key == "wall_seconds":
                raise ValueError(f"duplicate or reserved metric: {key}")
            try:
                result[key] = int(value)
            except ValueError:
                try:
                    result[key] = float(value)
                except ValueError:
                    result[key] = value
    return result


def validate_metrics(data, case, points, repeats=3):
    required = (INGEST_METRICS if case in ("uniform", "hot") else
                MIXED_METRICS if case == "mixed" else DESIGN_METRICS)
    missing = required - data.keys()
    if missing:
        raise ValueError("missing metrics: " + ", ".join(sorted(missing)))
    for key, value in data.items():
        numeric = isinstance(value, (int, float)) and not isinstance(value, bool)
        if key in required and not numeric:
            raise ValueError(f"non-numeric metric: {key}")
        if numeric:
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"nonfinite or negative metric: {key}")
            if key in required and not key.endswith(("_seconds", "_ms", "_us")) and key not in {
                    "seconds", "throughput", "write_throughput", "scan_throughput",
                    "scan_points_per_second", "syncs_per_second", "points_per_commit"}:
                if value != int(value):
                    raise ValueError(f"non-integer counter: {key}")
    positive = {"rss_bytes"}
    if "wall_seconds" in data:
        positive.add("wall_seconds")
    if case in ("uniform", "hot"):
        positive.update(("seconds", "write_seconds", "drain_seconds", "recovery_seconds",
                         "throughput", "write_throughput"))
        expected = {"points": points, "recovered_points": points, "initial_points": 0,
                    "series": 10000, "hot_series": 8 if case == "hot" else 0}
    elif case == "mixed":
        positive.update(("seconds", "written", "scans", "write_throughput", "scan_throughput",
                         "historical_read_points", "scanned_points", "peak_rss_bytes"))
        expected = {"initial_points": points, "write_errors": 0, "scan_errors": 0,
                    "writers": 4, "readers": 16, "series_per_writer": 100}
        if data["historical_read_points"] > data["scanned_points"]:
            raise ValueError("historical reads exceed scanned points")
    else:
        positive.update(("append_seconds", "scan_seconds", "recover_seconds", "scan_points_per_second"))
        if case == "compact":
            positive.add("compact_seconds")
        elif data["compact_seconds"] != 0:
            raise ValueError("unexpected compaction timing")
        expected = {"points": points, "repeats": repeats, "recovered_points": points,
                    "scan_points": points * repeats, "mode": case}
        if "rss_scope" in data and data["rss_scope"] != "process_peak_bytes":
            raise ValueError("unexpected RSS scope")
    for key in positive:
        if data[key] <= 0:
            raise ValueError(f"metric must be positive: {key}")
    for key, value in expected.items():
        if data.get(key) != value:
            raise ValueError(f"unexpected {key}: {data.get(key)!r}, expected {value!r}")
    if case in ("uniform", "hot", "mixed"):
        for prefix in (("write", "scan") if case == "mixed" else ("write",)):
            p50, p95, p99 = (data[f"{prefix}_p{p}_us"] for p in (50, 95, 99))
            if not 0 < p50 <= p95 <= p99:
                raise ValueError(f"invalid {prefix} latency percentiles")
        if case == "mixed" and data["scan_max_us"] < data["scan_p99_us"]:
            raise ValueError("scan maximum is below p99")


def path_present(path):
    # exists() 不识别悬空链接；也不允许借悬空父链接创建它的目标。
    return path.exists() or any(p.is_symlink() and not p.exists() for p in (path, *path.parents))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-root", type=Path,
                        help="new DB root on the tested disk; defaults to output (not tmpfs)")
    parser.add_argument("--points", type=int, default=10000000)
    parser.add_argument("--hot-points", type=int, default=3000000)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=15)
    parser.add_argument("--cases", default="uniform,hot,compact,append,scan,mixed")
    args = parser.parse_args(argv)
    names = args.cases.split(",")
    if min(args.points, args.hot_points, args.repeat, args.seconds) <= 0:
        parser.error("sizes, repeats and duration must be positive")
    if (args.points > (1 << 63) - 1 or args.hot_points > ((1 << 64) - 1) // 3 or
            args.seconds > ((1 << 63) - 1) // 1000000000):
        parser.error("size or duration exceeds benchmark range")
    if any(not name for name in names) or len(names) != len(set(names)):
        parser.error("cases must be nonempty and unique")
    if set(names) - {"uniform", "hot", "compact", "append", "scan", "mixed"}:
        parser.error("unknown case")
    if path_present(args.output):
        parser.error("output must not exist")
    out = args.output.resolve()
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    for directory in binaries.values():
        for executable in ("tsdb_bench", "tsdb_mixed_bench", "tsdb_design_bench"):
            if not (directory / executable).is_file():
                parser.error(f"missing {directory / executable}")
    if args.data_root and path_present(args.data_root):
        parser.error("data-root must not exist")
    data_root = args.data_root.resolve() if args.data_root else out
    if data_root != out and (data_root in out.parents or out in data_root.parents):
        parser.error("output and data-root must not overlap")
    out.mkdir(parents=True)
    if data_root != out:
        data_root.mkdir(parents=True)
    result = {"data_root": str(data_root), "config": {"points": args.points, "hot_points": args.hot_points,
                         "repeat": args.repeat, "seconds": args.seconds},
              "baseline": str(binaries["baseline"]), "candidate": str(binaries["candidate"]),
              "records": [], "executions": [], "status": "running"}

    def save():
        (out / "results.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")

    def run(command, label, case, points):
        command = [str(x) for x in command]
        execution = {"label": label, "command": command, "status": "running",
                     "returncode": None, "log": str(out / (label + ".log"))}
        result["executions"].append(execution)
        save()
        started = time.monotonic()
        try:
            with Path(execution["log"]).open("x") as log:
                log.write("COMMAND " + json.dumps(command) + "\n")
                log.flush()
                try:
                    proc = subprocess.run(command, capture_output=True, text=True,
                                          timeout=max(600, args.seconds + 300))
                    execution["returncode"] = proc.returncode
                    log.write(proc.stdout + proc.stderr)
                    if proc.returncode:
                        execution["status"] = "nonzero_exit"
                        raise RuntimeError(f"{label} exited {proc.returncode}")
                    try:
                        data = metrics(proc.stdout)
                        data["wall_seconds"] = time.monotonic() - started
                        validate_metrics(data, case, points)
                    except (ValueError, OverflowError) as error:
                        execution["status"] = "protocol_failure"
                        raise RuntimeError(f"{label}: {error}") from error
                    execution.update(status="pass", metrics=data)
                    return data
                except subprocess.TimeoutExpired as error:
                    execution["status"] = "timeout"
                    for text in (error.stdout, error.stderr):
                        if text:
                            log.write(text.decode(errors="replace") if isinstance(text, bytes) else text)
                    raise RuntimeError(f"{label} timed out after {error.timeout}s") from error
                finally:
                    if execution["status"] == "running":
                        execution["status"] = "execution_failure"
                    log.write("\nSTATUS " + execution["status"] +
                              " RETURN_CODE " + str(execution["returncode"]) + "\n")
        except Exception as error:
            if execution["status"] == "running":
                execution["status"] = "execution_failure"
            execution["error"] = str(error)
            raise
        finally:
            execution["wall_seconds"] = time.monotonic() - started
            save()

    try:
        save()
        for name in names:
            for repeat in range(args.repeat):
                # AB/BA 顺序抵消文件缓存和温度漂移；每次使用新库。
                order = ["baseline", "candidate"] if repeat % 2 == 0 else ["candidate", "baseline"]
                for version in order:
                    label = f"{name}-{version}-{repeat + 1}"
                    binary = binaries[version]
                    db = data_root / (label + "-db")
                    if path_present(db):
                        raise RuntimeError(f"database must not exist: {db}")
                    if name in ("uniform", "hot", "mixed"):
                        command = [binary / "tsdb_bench", args.points, "--series", 10000, "--path", db]
                        if name == "hot":
                            command += ["--hot-series", 8, "--hot-fraction", 0.5]
                        data = run(command, label + ("-seed" if name == "mixed" else ""),
                                   "hot" if name == "hot" else "uniform", args.points)
                        if name == "mixed":
                            data = run([binary / "tsdb_mixed_bench", args.seconds,
                                        "--writers", 4, "--readers", 16, "--series", 10000,
                                        "--path", db, "--keep", "--expected-points", args.points],
                                       label, name, args.points)
                    else:
                        size = args.hot_points if name in ("compact", "scan") else min(args.hot_points, 1000000)
                        data = run([binary / "tsdb_design_bench", name, size, db, 3], label, name, size)
                    result["records"].append({"case": name, "version": version,
                                              "repeat": repeat + 1, "metrics": data})
                    save()
                    print(label, json.dumps(data), flush=True)
        summary = {}
        for name in names:
            by_version = {}
            for version in binaries:
                rows = [x["metrics"] for x in result["records"] if x["case"] == name and x["version"] == version]
                keys = set.intersection(*(set(x) for x in rows))
                by_version[version] = {key: statistics.median([x[key] for x in rows])
                                       for key in sorted(keys) if all(isinstance(x[key], (float, int)) for x in rows)}
            key = {"uniform": "seconds", "hot": "seconds", "compact": "compact_seconds",
                   "append": "append_seconds", "scan": "scan_seconds", "mixed": "write_throughput"}[name]
            base, candidate = by_version["baseline"][key], by_version["candidate"][key]
            gain = (candidate / base - 1) if name == "mixed" else (base / candidate - 1)
            rss_change = by_version["candidate"]["rss_bytes"] / by_version["baseline"]["rss_bytes"] - 1
            if not math.isfinite(gain * 100) or not math.isfinite(rss_change * 100):
                raise ValueError("nonfinite summary")
            summary[name] = {"medians": by_version, "primary_metric": key,
                             "speedup_percent": gain * 100,
                             "candidate_peak_rss_change_percent": rss_change * 100}
        result.update(status="pass", summary=summary)
        save()
    except Exception as error:
        result.update(status="fail", error=str(error))
        save()
        raise
    print(json.dumps(result["summary"], indent=2), flush=True)


if __name__ == "__main__":
    main()
