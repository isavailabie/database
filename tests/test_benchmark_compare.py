import argparse
import contextlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("benchmark_compare", ROOT / "benchmark" / "compare.py")
compare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compare)
DESIGN_BINARY = None
EXECUTABLES = {"tsdb_bench", "tsdb_mixed_bench", "tsdb_design_bench"}


def ingest_payload(points=101, hot=False):
    return dict(points=points, series=10000, hot_series=8 if hot else 0,
                seconds=2, throughput=points / 2, write_seconds=1, write_throughput=points,
                drain_seconds=1, drain_compact_seconds=0, rss_bytes=1000, disk_bytes=2000,
                sstables=1, series_index_entries=10, page_index_entries=1, obsolete_sstables=0,
                memtable_bytes=0, sstable_bytes=2000, recovery_seconds=0.1,
                flush_count=1, flush_ms=1, compaction_count=0, compaction_ms=0,
                wal_syncs=1, wal_sync_ms=1, wal_bytes_written=1000, wal_bytes_replayed=0,
                recovered_points=points, initial_points=0,
                write_p50_us=10, write_p95_us=20, write_p99_us=30)


def mixed_payload(points=101):
    return dict(seconds=1, writers=4, readers=16, series_per_writer=100,
                written=100, write_throughput=100, scans=10, scan_throughput=10,
                scan_errors=0, scan_p50_us=40, scan_p95_us=50, scan_p99_us=60,
                scan_max_us=70, rss_bytes=1000, disk_bytes=2000, flush_count=1,
                compaction_count=0, obsolete_sstables=0, wal_bytes=1000,
                visible_version=2, commits=1, wal_syncs=1, syncs_per_second=1,
                points_per_commit=100, engine_threads=1, write_errors=0,
                write_p50_us=10, write_p95_us=20, write_p99_us=30,
                initial_points=points, write_start_timestamp=1000,
                scanned_points=200, historical_read_points=20,
                current_rss_bytes=900, peak_rss_bytes=1000)


def design_payload(mode="scan", points=103, repeats=3):
    return dict(mode=mode, points=points, repeats=repeats, append_seconds=1,
                compact_seconds=0.1 if mode == "compact" else 0, scan_seconds=0.2,
                scan_points=points * repeats, scan_points_per_second=points * repeats / 0.2,
                rss_bytes=1000, compaction_count=1 if mode == "compact" else 0,
                flush_count=1, recover_seconds=0.1, recovered_points=points)


def completed(command, payload, returncode=0, stderr=""):
    stdout = " ".join(f"{key}={value}" for key, value in payload.items()) + "\n"
    return subprocess.CompletedProcess(command, returncode, stdout, stderr)


class BenchmarkCompareTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name).resolve()
        self.binaries = {name: self.work / name for name in ("baseline", "candidate")}
        self.commands = []
        self.run_number = 0

    def fake_run(self, command, **kwargs):
        self.commands.append(command)
        self.assertEqual(kwargs, dict(capture_output=True, text=True, timeout=600))
        name = Path(command[0]).name
        if name == "tsdb_bench":
            payload = ingest_payload(int(command[1]), "--hot-series" in command)
            Path(command[command.index("--path") + 1]).mkdir()
        elif name == "tsdb_mixed_bench":
            self.assertIn("--keep", command)
            self.assertTrue(Path(command[command.index("--path") + 1]).is_dir())
            payload = mixed_payload(int(command[command.index("--expected-points") + 1]))
        else:
            payload = design_payload(command[1], int(command[2]), int(command[4]))
            Path(command[3]).mkdir()
        return completed(command, payload)

    def invoke(self, options=(), effect=None, output=None, missing=None):
        self.run_number += 1
        self.output = output or self.work / f"output-{self.run_number}"
        argv = ["--baseline", str(self.binaries["baseline"]),
                "--candidate", str(self.binaries["candidate"]),
                "--output", str(self.output), "--points", "101", "--hot-points", "103",
                "--seconds", "1", "--repeat", "1", "--cases", "uniform", *options]
        # 不创建可执行文件，整个进程边界都使用 mock。
        with patch.object(Path, "is_file", autospec=True, side_effect=lambda path:
                          path.parent in self.binaries.values() and path.name in EXECUTABLES and
                          path.name != missing), \
                patch.object(compare.subprocess, "run", side_effect=effect or self.fake_run) as process, \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.process = process
            return compare.main(argv)

    def result(self):
        return json.loads((self.output / "results.json").read_text())

    def test_alternating_ab_ba_and_unique_databases(self):
        self.invoke(["--repeat", "3"])
        self.assertEqual([Path(c[0]).parent.name for c in self.commands],
                         ["baseline", "candidate", "candidate", "baseline", "baseline", "candidate"])
        databases = [c[c.index("--path") + 1] for c in self.commands]
        self.assertEqual(len(set(databases)), 6)
        self.assertEqual(self.result()["status"], "pass")
        self.assertEqual(len(self.result()["records"]), 6)
        self.assertTrue(all(x["status"] == "pass" for x in self.result()["executions"]))

    def test_case_scales_and_hot_distribution(self):
        self.invoke(["--cases", "uniform,hot,compact,append,scan", "--points", "987654",
                     "--hot-points", "2000001"])
        for command in self.commands:
            if Path(command[0]).name == "tsdb_bench":
                self.assertEqual(command[1], "987654")
                self.assertEqual(command[command.index("--series") + 1], "10000")
                if "hot-" in command[command.index("--path") + 1]:
                    self.assertEqual(command[command.index("--hot-series") + 1], "8")
                    self.assertEqual(command[command.index("--hot-fraction") + 1], "0.5")
                else:
                    self.assertNotIn("--hot-series", command)
            else:
                self.assertEqual(command[2], "1000000" if command[1] == "append" else "2000001")
                self.assertEqual(command[4], "3")
        self.assertEqual(len(self.result()["records"]), 10)

    def test_append_keeps_small_requested_scale(self):
        self.invoke(["--cases", "append"])
        self.assertEqual([c[2] for c in self.commands], ["103", "103"])

    def test_mixed_preloads_each_database_in_ab_ba_order(self):
        self.invoke(["--cases", "mixed", "--repeat", "2"])
        self.assertEqual(len(self.commands), 8)
        for seed, mixed in zip(self.commands[::2], self.commands[1::2]):
            self.assertEqual(Path(seed[0]).name, "tsdb_bench")
            self.assertEqual(Path(mixed[0]).name, "tsdb_mixed_bench")
            self.assertEqual(seed[seed.index("--path") + 1], mixed[mixed.index("--path") + 1])
            self.assertEqual(mixed[mixed.index("--expected-points") + 1], "101")
        self.assertEqual([Path(c[0]).parent.name for c in self.commands[::2]],
                         ["baseline", "candidate", "candidate", "baseline"])
        self.assertEqual(len(self.result()["records"]), 4)
        self.assertEqual(len(self.result()["executions"]), 8)

    def test_medians_and_latency_speedup_and_rss_change(self):
        seconds = {"baseline": iter([9, 1, 5]), "candidate": iter([4, 8, 2])}
        rss = {"baseline": iter([3000, 1000, 2000]), "candidate": iter([1000, 2000, 500])}

        def run(command, **kwargs):
            response = self.fake_run(command, **kwargs)
            data = compare.metrics(response.stdout)
            version = Path(command[0]).parent.name
            data.update(seconds=next(seconds[version]), rss_bytes=next(rss[version]))
            return completed(command, data)

        self.invoke(["--repeat", "3"], effect=run)
        summary = self.result()["summary"]["uniform"]
        self.assertEqual(summary["primary_metric"], "seconds")
        self.assertEqual(summary["medians"]["baseline"]["seconds"], 5)
        self.assertEqual(summary["medians"]["candidate"]["seconds"], 4)
        self.assertEqual(summary["medians"]["candidate"]["write_p99_us"], 30)
        self.assertAlmostEqual(summary["speedup_percent"], 25)
        self.assertAlmostEqual(summary["candidate_peak_rss_change_percent"], -50)

    def test_even_repeat_median_and_mixed_throughput_direction(self):
        rates = {"baseline": iter([100, 300]), "candidate": iter([400, 200])}

        def run(command, **kwargs):
            response = self.fake_run(command, **kwargs)
            if Path(command[0]).name == "tsdb_mixed_bench":
                data = compare.metrics(response.stdout)
                data["write_throughput"] = next(rates[Path(command[0]).parent.name])
                return completed(command, data)
            return response

        self.invoke(["--cases", "mixed", "--repeat", "2"], effect=run)
        summary = self.result()["summary"]["mixed"]
        self.assertEqual(summary["medians"]["baseline"]["write_throughput"], 200)
        self.assertEqual(summary["medians"]["candidate"]["write_throughput"], 300)
        self.assertAlmostEqual(summary["speedup_percent"], 50)

    def test_nonzero_exit_saves_command_status_and_both_streams(self):
        for returncode in (7, -9):
            with self.subTest(returncode=returncode):
                def run(command, **kwargs):
                    return completed(command, {"partial": 1}, returncode, "失败详情\n")

                with self.assertRaisesRegex(RuntimeError, f"exited {returncode}"):
                    self.invoke(effect=run)
                result = self.result()
                execution = result["executions"][0]
                self.assertEqual(result["status"], "fail")
                self.assertEqual(result["records"], [])
                self.assertEqual(execution["status"], "nonzero_exit")
                self.assertEqual(execution["returncode"], returncode)
                self.assertEqual(execution["command"], self.process.call_args.args[0])
                log = Path(execution["log"]).read_text()
                self.assertIn("COMMAND " + json.dumps(execution["command"]), log)
                self.assertIn("partial=1", log)
                self.assertIn("失败详情", log)
                self.assertIn(f"STATUS nonzero_exit RETURN_CODE {returncode}", log)
                self.assertEqual(self.process.call_count, 1)

    def test_timeout_saves_partial_bytes_or_text_and_status(self):
        for stdout, stderr in ((b"partial\xff\n", b"error\n"), ("partial\n", "error\n")):
            with self.subTest(stdout=stdout):
                def run(command, **kwargs):
                    raise subprocess.TimeoutExpired(command, kwargs["timeout"], stdout, stderr)

                with self.assertRaisesRegex(RuntimeError, "timed out after 600s"):
                    self.invoke(effect=run)
                result = self.result()
                execution = result["executions"][0]
                self.assertEqual(result["status"], "fail")
                self.assertEqual(execution["status"], "timeout")
                self.assertIsNone(execution["returncode"])
                self.assertEqual(execution["command"], self.process.call_args.args[0])
                log = Path(execution["log"]).read_text()
                self.assertIn("COMMAND", log)
                self.assertIn("partial", log)
                self.assertIn("error", log)
                self.assertIn("STATUS timeout", log)
                self.assertEqual(self.process.call_count, 1)

    def test_execution_error_is_persisted(self):
        with self.assertRaisesRegex(OSError, "cannot execute"):
            self.invoke(effect=OSError("cannot execute"))
        self.assertEqual(self.result()["status"], "fail")
        execution = self.result()["executions"][0]
        self.assertEqual(execution["status"], "execution_failure")
        self.assertIn("cannot execute", execution["error"])
        self.assertIn("STATUS execution_failure", Path(execution["log"]).read_text())

    def test_protocol_failure_stops_before_candidate(self):
        data = ingest_payload()
        del data["rss_bytes"]
        with self.assertRaisesRegex(RuntimeError, "missing metrics: rss_bytes"):
            self.invoke(effect=lambda command, **kwargs: completed(command, data))
        result = self.result()
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["executions"][0]["status"], "protocol_failure")
        self.assertEqual(result["records"], [])
        self.assertNotIn("summary", result)
        self.assertEqual(self.process.call_count, 1)

    def test_failed_seed_never_starts_mixed(self):
        data = ingest_payload()
        data["recovered_points"] -= 1
        with self.assertRaisesRegex(RuntimeError, "unexpected recovered_points"):
            self.invoke(["--cases", "mixed"], effect=lambda command, **kwargs: completed(command, data))
        self.assertEqual(self.process.call_count, 1)
        self.assertTrue(self.result()["executions"][0]["label"].endswith("-seed"))

    def test_successful_records_survive_later_failure(self):
        def run(command, **kwargs):
            if Path(command[0]).parent.name == "candidate":
                return completed(command, {}, 3)
            return self.fake_run(command, **kwargs)

        with self.assertRaisesRegex(RuntimeError, "exited 3"):
            self.invoke(effect=run)
        self.assertEqual(len(self.result()["records"]), 1)
        self.assertEqual([e["status"] for e in self.result()["executions"]], ["pass", "nonzero_exit"])

    def test_all_required_metrics_are_mandatory(self):
        for case, payload in (("uniform", ingest_payload()), ("hot", ingest_payload(hot=True)),
                              ("mixed", mixed_payload()), ("append", design_payload("append")),
                              ("compact", design_payload("compact")), ("scan", design_payload())):
            for key in payload:
                with self.subTest(case=case, missing=key):
                    data = dict(payload)
                    del data[key]
                    with self.assertRaises(ValueError):
                        compare.validate_metrics(data, case, payload.get("points", 101))

    def test_all_numeric_metrics_reject_nonfinite_and_negative(self):
        for case, payload in (("uniform", ingest_payload()), ("mixed", mixed_payload()),
                              ("compact", design_payload("compact"))):
            for key, value in payload.items():
                if isinstance(value, str):
                    continue
                for invalid in (float("nan"), float("inf"), float("-inf"), -1, "not-a-number"):
                    with self.subTest(case=case, key=key, invalid=invalid):
                        with self.assertRaises(ValueError):
                            compare.validate_metrics(dict(payload, **{key: invalid}), case,
                                                     payload.get("points", 101))
        with self.assertRaises(ValueError):
            compare.validate_metrics(dict(ingest_payload(), extra=float("nan")), "uniform", 101)

    def test_measured_durations_rates_and_rss_must_be_positive(self):
        examples = [("uniform", ingest_payload(), ("seconds", "write_seconds", "drain_seconds",
                     "recovery_seconds", "throughput", "write_throughput", "rss_bytes")),
                    ("mixed", mixed_payload(), ("seconds", "write_throughput", "scan_throughput", "rss_bytes")),
                    ("compact", design_payload("compact"), ("append_seconds", "compact_seconds",
                     "scan_seconds", "recover_seconds", "scan_points_per_second", "rss_bytes"))]
        for case, payload, keys in examples:
            for key in keys:
                with self.subTest(case=case, key=key), self.assertRaises(ValueError):
                    compare.validate_metrics(dict(payload, **{key: 0}), case, payload.get("points", 101))
        compare.validate_metrics(design_payload("append"), "append", 103)
        compare.validate_metrics(design_payload("scan"), "scan", 103)

    def test_design_recovery_scan_count_mode_and_repeats(self):
        for changes in ({"points": 102}, {"recovered_points": 102}, {"scan_points": 103},
                        {"repeats": 2}, {"mode": "append"}, {"compact_seconds": 1},
                        {"rss_scope": "current"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                compare.validate_metrics(dict(design_payload(), **changes), "scan", 103)
        compare.validate_metrics(dict(design_payload(), rss_scope="process_peak_bytes"), "scan", 103)

    def test_mixed_requires_zero_errors_and_actual_preloaded_reads(self):
        for changes in ({"write_errors": 1}, {"scan_errors": 1}, {"initial_points": 0},
                        {"initial_points": 102}, {"historical_read_points": 0},
                        {"historical_read_points": 201}, {"written": 0}, {"scans": 0},
                        {"scanned_points": 0}, {"writers": 1}, {"readers": 1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                compare.validate_metrics(dict(mixed_payload(), **changes), "mixed", 101)

    def test_percentiles_must_be_ordered_and_maximum_valid(self):
        for changes in ({"write_p50_us": 0}, {"write_p95_us": 5}, {"write_p99_us": 15},
                        {"scan_p95_us": 30}, {"scan_p99_us": 45}, {"scan_max_us": 59}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                compare.validate_metrics(dict(mixed_payload(), **changes), "mixed", 101)

    def test_fractional_counters_and_wrong_ingest_scale_are_rejected(self):
        for changes in ({"points": 1.5}, {"flush_count": 0.5}, {"recovered_points": 100},
                        {"initial_points": 1}, {"series": 1}, {"hot_series": 8}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                compare.validate_metrics(dict(ingest_payload(), **changes), "uniform", 101)

    def test_metric_parser_preserves_large_integer_and_rejects_duplicates(self):
        count = (1 << 53) + 1
        self.assertEqual(compare.metrics(f"points={count} seconds=1.25 mode=scan")["points"], count)
        for text in ("points=1 points=2", "mode=scan mode=append", "=1", "wall_seconds=1"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                compare.metrics(text)
        with self.assertRaisesRegex(RuntimeError, "nonfinite"):
            data = ingest_payload()
            data["write_p99_us"] = "NaN"
            self.invoke(effect=lambda command, **kwargs: completed(command, data))
        self.assertEqual(self.result()["executions"][0]["status"], "protocol_failure")

    def test_invalid_cli_refuses_empty_duplicate_cases_and_sizes(self):
        for option, value in (("--cases", ""), ("--cases", "uniform,"), ("--cases", ",scan"),
                              ("--cases", "uniform,,scan"), ("--cases", "scan,scan"),
                              ("--cases", "unknown"), ("--points", "0"), ("--points", "-1"),
                              ("--hot-points", "0"), ("--repeat", "0"), ("--seconds", "-1"),
                              ("--points", str(1 << 63)), ("--hot-points", str(1 << 63)),
                              ("--seconds", str(1 << 63)), ("--points", "1x")):
            with self.subTest(option=option, value=value), self.assertRaises(SystemExit) as caught:
                self.invoke([option, value])
            self.assertEqual(caught.exception.code, 2)
            self.assertFalse(self.output.exists())
            self.process.assert_not_called()

    def test_missing_binary_does_not_create_output(self):
        with self.assertRaises(SystemExit):
            self.invoke(missing="tsdb_design_bench")
        self.assertFalse(self.output.exists())
        self.process.assert_not_called()

    def test_existing_output_and_data_root_are_preserved(self):
        existing = self.work / "existing"
        existing.mkdir()
        marker = existing / "marker"
        marker.write_text("保留")
        for options, output in (([], existing), (["--data-root", str(existing)], None)):
            with self.subTest(options=options), self.assertRaises(SystemExit):
                self.invoke(options, output=output)
            self.assertEqual(marker.read_text(), "保留")
            self.process.assert_not_called()
            if output is None:
                self.assertFalse(self.output.exists())

    def test_dangling_output_or_data_symlinks_are_rejected(self):
        link = self.work / "dangling"
        target = self.work / "absent"
        link.symlink_to(target)
        for options, output in (([], link), ([], link / "child"),
                                (["--data-root", str(link)], None)):
            with self.subTest(output=output, options=options), self.assertRaises(SystemExit):
                self.invoke(options, output=output)
            self.assertTrue(link.is_symlink())
            self.assertFalse(target.exists())
            self.process.assert_not_called()

    def test_overlapping_output_and_data_roots_are_rejected(self):
        outer = self.work / "outer"
        inner = outer / "inner"
        for output, data in ((outer, inner), (inner, outer)):
            with self.subTest(output=output), self.assertRaises(SystemExit):
                self.invoke(["--data-root", str(data)], output=output)
            self.assertFalse(outer.exists())
            self.process.assert_not_called()

    def test_same_or_separate_new_data_root_is_supported(self):
        output = self.work / "same"
        self.invoke(["--data-root", str(output)], output=output)
        self.assertEqual(self.result()["data_root"], str(output))
        data = self.work / "data"
        self.invoke(["--data-root", str(data)])
        self.assertEqual(self.result()["data_root"], str(data))
        self.assertTrue(all(Path(c[c.index("--path") + 1]).parent == data for c in self.commands[-2:]))

    def test_existing_database_is_never_passed_to_destructive_ingest(self):
        marker = self.work / "marker"

        def run(command, **kwargs):
            response = self.fake_run(command, **kwargs)
            blocked = self.output / "uniform-candidate-1-db"
            blocked.symlink_to(marker)
            return response

        with self.assertRaisesRegex(RuntimeError, "database must not exist"):
            self.invoke(effect=run)
        self.assertEqual(self.process.call_count, 1)
        self.assertTrue((self.output / "uniform-candidate-1-db").is_symlink())
        self.assertFalse(marker.exists())
        self.assertEqual(self.result()["status"], "fail")

    def test_existing_log_is_not_overwritten_or_executed(self):
        def run(command, **kwargs):
            response = self.fake_run(command, **kwargs)
            (self.output / "uniform-candidate-1.log").write_text("已有日志")
            return response

        with self.assertRaises(FileExistsError):
            self.invoke(effect=run)
        self.assertEqual(self.process.call_count, 1)
        self.assertEqual((self.output / "uniform-candidate-1.log").read_text(), "已有日志")
        self.assertEqual(self.result()["executions"][-1]["status"], "execution_failure")


class DesignSmokeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name).resolve()

    def run_design(self, mode, points, path, repeats, timeout=15):
        return subprocess.run([str(DESIGN_BINARY), mode, str(points), str(path), str(repeats)],
                              capture_output=True, text=True, timeout=timeout)

    def check_mode(self, mode):
        result = self.run_design(mode, 1001, self.work / mode, 2)
        self.assertEqual(result.returncode, 0, result.stderr)
        data = compare.metrics(result.stdout)
        compare.validate_metrics(data, mode, 1001, repeats=2)
        self.assertEqual(data["rss_scope"], "process_peak_bytes")

    def test_append(self):
        self.check_mode("append")

    def test_compact(self):
        self.check_mode("compact")

    def test_scan(self):
        self.check_mode("scan")

    def test_invalid_arguments_do_not_create_database(self):
        values = ("-1", "+1", "1x", "1.0", " 1", "", "0", str(1 << 64))
        arguments = [("scan", value, "2") for value in values]
        arguments += [("scan", "2", value) for value in values]
        arguments += [("invalid", "2", "2"), ("scan", str(1 << 63), "1"),
                      ("scan", str((1 << 63) - 1), "3")]
        for mode, points, repeats in arguments:
            with self.subTest(mode=mode, points=points, repeats=repeats):
                path = self.work / "invalid-db"
                result = self.run_design(mode, points, path, repeats, timeout=5)
                self.assertNotEqual(result.returncode, 0)
                self.assertTrue(result.stderr)
                self.assertFalse(path.exists())
        result = self.run_design("scan", 2, "", 2, timeout=5)
        self.assertNotEqual(result.returncode, 0)

    def test_existing_paths_and_symlinks_are_preserved(self):
        directory = self.work / "existing-db"
        directory.mkdir()
        marker = directory / "marker"
        marker.write_text("保留")
        file_path = self.work / "existing-file"
        file_path.write_text("保留")
        link = self.work / "existing-link"
        link.symlink_to(directory, target_is_directory=True)
        missing = self.work / "absent"
        dangling = self.work / "dangling"
        dangling.symlink_to(missing)
        for path in (directory, file_path, link, dangling, dangling / "child"):
            with self.subTest(path=path):
                result = self.run_design("append", 2, path, 1, timeout=5)
                self.assertNotEqual(result.returncode, 0)
                self.assertTrue(result.stderr)
                self.assertEqual(marker.read_text(), "保留")
                self.assertEqual(file_path.read_text(), "保留")
                self.assertTrue(link.is_symlink())
                self.assertTrue(dangling.is_symlink())
                self.assertFalse(missing.exists())


def load_tests(loader, tests, pattern):
    return loader.loadTestsFromTestCase(DesignSmokeTests if DESIGN_BINARY else BenchmarkCompareTests)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--design-binary", type=Path)
    args = parser.parse_args()
    DESIGN_BINARY = args.design_binary
    unittest.main(argv=[sys.argv[0]], verbosity=2)
