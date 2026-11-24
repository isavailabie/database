import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("benchmark_runner", ROOT / "benchmark" / "runner.py")
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class BenchmarkRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.binary = {key: self.work / key for key in ("bench", "mixed", "recover")}

    def ingest_payload(self, points=100):
        return dict(points=str(points), seconds="1", throughput=str(points),
                    recovered_points=str(points), recovery_seconds="0.1",
                    rss_bytes="1000", disk_bytes="2000",
                    write_p50_us="10", write_p95_us="20", write_p99_us="30")

    def mixed_payload(self):
        return dict(written="100", seconds="1", write_throughput="100",
                    scans="10", scan_throughput="10", scan_errors="0", write_errors="0",
                    initial_points="100", historical_read_points="20",
                    rss_bytes="1000", disk_bytes="2000",
                    write_p50_us="10", write_p95_us="20", write_p99_us="30",
                    scan_p50_us="40", scan_p95_us="50", scan_p99_us="60")

    def fake_run(self, commands, bad_ingest=False):
        def invoke(command, log, timeout, cwd):
            commands.append([str(value) for value in command])
            if str(command[0]) == "cmake":
                if "--build" in command:
                    build = Path(command[2])
                    build.mkdir()
                    for name in ("tsdb_bench", "tsdb_mixed_bench", "tsdb_recover"):
                        (build / name).touch()
                payload = {}
            elif Path(command[0]).name in {"bench", "tsdb_bench"}:
                payload = self.ingest_payload(0 if bad_ingest else 100)
            else:
                payload = self.mixed_payload()
            Path(log).write_text(" ".join(f"{key}={value}" for key, value in payload.items()) + "\n")
            return dict(status="pass", returncode=0, seconds=0.01, log=str(log))
        return invoke

    def test_mixed_command_keeps_corresponding_database_and_samples(self):
        database = self.work / "existing-db"
        cmd, _ = runner.command_for("perf_mixed", runner.CASES["perf_mixed"], self.binary,
                                    self.work, 100, 4, 1, db=database, sample_interval=0.25)
        self.assertIn("--keep", cmd)
        self.assertEqual(cmd[cmd.index("--path") + 1], database)
        self.assertEqual(cmd[cmd.index("--expected-points") + 1], 100)
        self.assertEqual(cmd[cmd.index("--sample-interval") + 1], 0.25)
        self.assertEqual(cmd[cmd.index("--sample-csv") + 1], self.work / "samples.csv")

    def test_write_percentiles_are_preserved_in_metrics(self):
        for case, data in (("perf_mixed", self.mixed_payload()),
                           ("perf_ingest", self.ingest_payload())):
            row = runner.metrics_for(case, data)[0]
            self.assertEqual([row[f"p{p}_us"] for p in (50, 95, 99)], [10, 20, 30])

    def test_mixed_only_preloads_matching_hot_distribution(self):
        commands = []
        with patch.object(runner, "run", side_effect=self.fake_run(commands)):
            result = runner.execute("perf_mixed_hot", runner.CASES["perf_mixed_hot"],
                                    self.binary, self.work, self.work / "run.log", 100, 4, 1, 20)
        self.assertEqual(result["status"], "pass")
        self.assertEqual(len(commands), 2)
        self.assertIn("--hot-series", commands[0])
        self.assertIn("--hot-fraction", commands[0])
        self.assertIn("--keep", commands[1])
        self.assertEqual(commands[0][commands[0].index("--path") + 1],
                         commands[1][commands[1].index("--path") + 1])

    def test_mixed_with_existing_ingest_does_not_preload(self):
        commands = []
        with patch.object(runner, "run", side_effect=self.fake_run(commands)):
            runner.execute("perf_mixed", runner.CASES["perf_mixed"], self.binary,
                           self.work, self.work / "run.log", 100, 4, 1, 20,
                           ingest_db=self.work / "imported")
        self.assertEqual(len(commands), 1)
        self.assertIn(str(self.work / "imported"), commands[0])

    def test_bad_preload_stops_mixed(self):
        commands = []
        with patch.object(runner, "run", side_effect=self.fake_run(commands, bad_ingest=True)):
            result = runner.execute("perf_mixed", runner.CASES["perf_mixed"], self.binary,
                                    self.work, self.work / "run.log", 100, 4, 1, 20)
        self.assertEqual(result["status"], "protocol_failure")
        self.assertEqual(result["stage"], "preload")
        self.assertEqual(len(commands), 1)
        self.assertTrue(result["log"].endswith("ingest.log"))

    def test_protocol_rejects_empty_error_and_nonfinite_results(self):
        for changes in ({"initial_points": "0"}, {"historical_read_points": "0"},
                        {"write_errors": "1"}, {"scan_errors": "1"},
                        {"written": "0"}, {"seconds": "nan"},
                        {"write_p99_us": "inf"}, {"write_p95_us": "1"}):
            with self.subTest(changes=changes):
                data = dict(self.mixed_payload(), **changes)
                self.assertIsNotNone(runner.protocol_error("perf_mixed", data, 100))
        self.assertIsNotNone(runner.protocol_error("perf_mixed", {"foo": "1"}, 100))
        self.assertIsNone(runner.protocol_error("perf_mixed", self.mixed_payload(), 100))

    def test_recovery_requires_all_expected_points(self):
        data = dict(open_seconds="0.1", recovered_points="1", wal_bytes="100",
                    rss_bytes="1000", disk_bytes="2000")
        self.assertIsNotNone(runner.protocol_error("perf_recover", data, 100))
        data["recovered_points"] = "100"
        self.assertIsNone(runner.protocol_error("perf_recover", data, 100))

    def test_invalid_cli_is_rejected_before_creating_output(self):
        for option, value in (("--points", "0"), ("--seconds", "-1"),
                              ("--repeat", "0"), ("--threads", "0"),
                              ("--timeout", "-1"), ("--jobs", "0"),
                              ("--sample-interval", "nan"), ("--sample-interval", "0"),
                              ("--cases", "perf_mixed,perf_mixed"), ("--cases", "unknown")):
            output = self.work / "invalid"
            with self.subTest(option=option, value=value), patch("sys.argv", [
                "runner.py", "--output", str(output), option, value
            ]), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    runner.main()
                self.assertEqual(caught.exception.code, 2)
                self.assertFalse(output.exists())

    def test_main_reuses_ingest_even_when_cases_listed_in_reverse_order(self):
        commands = []
        output = self.work / "results"
        with patch("sys.argv", ["runner.py", "--output", str(output),
                                  "--cases", "perf_mixed,perf_ingest", "--points", "100",
                                  "--seconds", "1"]), patch.object(runner, "copy_source"), \
                patch.object(runner, "run", side_effect=self.fake_run(commands)), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(runner.main(), 0)
        summary = json.loads((output / "summary.json").read_text())
        ingest, mixed = summary["records"]
        self.assertEqual(ingest["case"], "perf_ingest")
        self.assertEqual(mixed["ingest_db"], ingest["db"])
        self.assertNotIn("preload", mixed)
        self.assertEqual(len([command for command in commands if "--series" in command]), 2)
        aggregate = json.loads((output / "aggregate.json").read_text())
        self.assertEqual(next(row for row in aggregate if row["case"] == "perf_mixed"
                              and row["phase"] == "write")["median_p99_us"], 30)

    def test_main_does_not_run_mixed_after_ingest_protocol_failure(self):
        commands = []
        output = self.work / "failed"
        with patch("sys.argv", ["runner.py", "--output", str(output),
                                  "--cases", "perf_ingest,perf_mixed", "--points", "100",
                                  "--seconds", "1"]), patch.object(runner, "copy_source"), \
                patch.object(runner, "run", side_effect=self.fake_run(commands, bad_ingest=True)), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(runner.main(), 1)
        summary = json.loads((output / "summary.json").read_text())
        self.assertEqual(summary["records"][1]["status"], "dependency_failure")
        self.assertFalse(any("--writers" in command for command in commands))


if __name__ == "__main__":
    unittest.main()
