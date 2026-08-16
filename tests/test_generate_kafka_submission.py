import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "generate-kafka-submission.py"


def run(*args: str, cwd: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args, cwd=cwd, env=env, check=False,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )


class GenerateKafkaSubmissionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()
        self._write_notebook()
        self._write(
            "evidence/kafka_log.csv",
            '{"city":"Pittsburgh","timestamp":"2026-08-15 12:00:00","temperature_f":64}\n'
            '{"city":"Chicago","timestamp":"2026-08-15 12:00:01","temperature_f":71}\n',
        )
        self._write(
            "evidence/client-config.json",
            json.dumps({
                "topic": "lab02-asmith",
                "producer": {
                    "bootstrap_servers": ["localhost:9092"],
                    "value_serializer": "JSON encoded as UTF-8 bytes",
                },
                "consumer": {
                    "bootstrap_servers": ["localhost:9092"],
                    "auto_offset_reset": "earliest",
                    "enable_auto_commit": True,
                },
            }, indent=2) + "\n",
        )
        self._write(
            "evidence/tunnel.txt",
            "ssh -o ExitOnForwardFailure=yes -L 9092:localhost:9092 student@broker.example.edu -NT\n",
        )
        self._write(
            "evidence/kcat.txt",
            '$ kcat -b localhost:9092 -t lab02-asmith -C -o earliest -c 2 -f "%o: %s\\n"\n'
            '12: {"city":"Pittsburgh","timestamp":"2026-08-15 12:00:00","temperature_f":64}\n'
            '13: {"city":"Chicago","timestamp":"2026-08-15 12:00:01","temperature_f":71}\n',
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write(self, relative: str, content: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def _write_notebook(self, execution_count: int | None = 1) -> None:
        notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": execution_count,
                    "metadata": {},
                    "outputs": [{
                        "name": "stdout", "output_type": "stream",
                        "text": ["Topic: lab02-asmith\n"],
                    }],
                    "source": [
                        "from kafka import KafkaProducer, KafkaConsumer\n",
                        "topic = 'lab02-asmith'\n",
                        "producer = KafkaProducer(bootstrap_servers=['localhost:9092'])\n",
                        "producer.send(topic, value=b'{}')\n",
                        "producer.flush()\n",
                        "consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])\n",
                        "open('kafka_log.csv', 'a').write('{}')\n",
                    ],
                },
                {
                    "cell_type": "markdown", "metadata": {},
                    "source": ["Run kcat in the terminal."],
                },
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 5,
        }
        self._write("KafkaDemo.ipynb", json.dumps(notebook))

    def generate(self, env: dict[str, str] | None = None, observed_offset: str = "12") -> subprocess.CompletedProcess[str]:
        return run(
            "python3", str(SCRIPT),
            "--learner", "Test Learner",
            "--notebook", str(self.root / "KafkaDemo.ipynb"),
            "--kafka-log", str(self.evidence / "kafka_log.csv"),
            "--client-config", str(self.evidence / "client-config.json"),
            "--tunnel-evidence", str(self.evidence / "tunnel.txt"),
            "--kcat-evidence", str(self.evidence / "kcat.txt"),
            "--local-port", "9092",
            "--observed-offset", observed_offset,
            cwd=self.root,
            env=env,
        )

    def test_generates_complete_report_and_manifest_without_contacting_kafka(self) -> None:
        fake_bin = self.root / "fake-bin"
        fake_bin.mkdir()
        markers = []
        for program in ("kcat", "ssh"):
            marker = self.root / f"{program}-was-called"
            markers.append(marker)
            executable = fake_bin / program
            executable.write_text(f"#!/bin/sh\ntouch '{marker}'\nexit 99\n", encoding="utf-8")
            executable.chmod(0o755)
        env = dict(os.environ)
        env["PATH"] = f"{fake_bin}:{env['PATH']}"

        result = self.generate(env)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(all(not marker.exists() for marker in markers))
        report = (self.root / "submission/kafka-report.html").read_text(encoding="utf-8")
        manifest = json.loads((self.root / "submission/kafka-manifest.json").read_text(encoding="utf-8"))
        self.assertTrue(manifest["complete"])
        self.assertTrue(all(item["status"] == "present" for item in manifest["checks"]))
        self.assertEqual(manifest["identifiers"]["observed_offset"], 12)
        self.assertEqual(manifest["manual_spot_checks"], ["offset", "start_position"])
        self.assertIn("lab02-asmith", report)
        self.assertIn("12: {&quot;city&quot;", report)

    def test_marks_unexecuted_notebook_and_uncited_offset_incomplete(self) -> None:
        self._write_notebook(execution_count=None)

        result = self.generate(observed_offset="99")

        self.assertEqual(result.returncode, 1)
        manifest = json.loads((self.root / "submission/kafka-manifest.json").read_text(encoding="utf-8"))
        checks = {item["name"]: item["status"] for item in manifest["checks"]}
        self.assertEqual(checks["executed_notebook"], "missing")
        self.assertEqual(checks["observed_offset_consistency"], "missing")
        self.assertEqual(checks["earliest_offset_kcat_evidence"], "missing")

    def test_marks_mismatched_config_and_tunnel_evidence_incomplete(self) -> None:
        config_path = self.evidence / "client-config.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))
        config["consumer"]["bootstrap_servers"] = ["localhost:19092"]
        config_path.write_text(json.dumps(config), encoding="utf-8")
        (self.evidence / "tunnel.txt").write_text("tunnel active\n", encoding="utf-8")

        result = self.generate()

        self.assertEqual(result.returncode, 1)
        manifest = json.loads((self.root / "submission/kafka-manifest.json").read_text(encoding="utf-8"))
        checks = {item["name"]: item["status"] for item in manifest["checks"]}
        self.assertEqual(checks["non_secret_client_configuration"], "missing")
        self.assertEqual(checks["tunnel_port_evidence"], "missing")

    def test_flags_and_withholds_plaintext_credentials(self) -> None:
        secret = "correct-horse-battery-staple"
        path = self.evidence / "kcat.txt"
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                " -C ", f" -X sasl.password={secret} -C "
            ),
            encoding="utf-8",
        )

        result = self.generate()

        self.assertEqual(result.returncode, 1)
        combined = (
            (self.root / "submission/kafka-report.html").read_text(encoding="utf-8")
            + (self.root / "submission/kafka-manifest.json").read_text(encoding="utf-8")
        )
        self.assertNotIn(secret, combined)
        self.assertIn("Content withheld", combined)
        manifest = json.loads((self.root / "submission/kafka-manifest.json").read_text(encoding="utf-8"))
        checks = {item["name"]: item["status"] for item in manifest["checks"]}
        self.assertEqual(checks["earliest_offset_kcat_evidence"], "missing")

    def test_rejects_missing_evidence_without_creating_outputs(self) -> None:
        (self.evidence / "kcat.txt").unlink()

        result = self.generate()

        self.assertEqual(result.returncode, 2)
        self.assertIn("Missing required saved evidence", result.stderr)
        self.assertFalse((self.root / "submission").exists())


if __name__ == "__main__":
    unittest.main()
