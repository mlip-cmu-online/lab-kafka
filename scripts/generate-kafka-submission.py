#!/usr/bin/env python3
"""Generate the Lab 3 Kafka report and manifest from saved evidence only."""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import io
import json
import re
import shlex
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPORT_NAME = "kafka-report.html"
MANIFEST_NAME = "kafka-manifest.json"
CHECKER_VERSION = "1.0"
SECRET_PATTERNS = (
    re.compile(
        r"(?i)(?:api[_-]?key|access[_-]?token|auth[_-]?token|password|passwd|secret)"
        r"\s*[:=]\s*['\"]?[^\s'\"]{4,}"
    ),
    re.compile(r"\b(?:ghp|github_pat|sk|xox[baprs])_[A-Za-z0-9_-]{16,}\b"),
    re.compile(r"(?i)-----BEGIN (?:OPENSSH|RSA|EC|DSA) PRIVATE KEY-----"),
    re.compile(r"https?://[^\s/@:]+:[^\s/@]+@", re.IGNORECASE),
)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def parse_json(path: Path) -> Any | None:
    try:
        return json.loads(read_text(path))
    except (json.JSONDecodeError, OSError):
        return None


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(65536), b""):
            digest.update(block)
    return digest.hexdigest()


def display_path(root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return path.name


def check(name: str, present: bool, location: str, identifier: str, detail: str) -> dict[str, str]:
    return {
        "name": name,
        "status": "present" if present else "missing",
        "location": location,
        "identifier": identifier,
        "detail": detail,
    }


def contains_secret(text: str) -> bool:
    return any(pattern.search(text) for pattern in SECRET_PATTERNS)


def notebook_text(notebook: dict[str, Any]) -> str:
    pieces: list[str] = []
    for cell in notebook.get("cells", []):
        if not isinstance(cell, dict):
            continue
        pieces.append("".join(cell.get("source", [])))
        for output in cell.get("outputs", []):
            if not isinstance(output, dict):
                continue
            text = output.get("text", "")
            pieces.append("".join(text) if isinstance(text, list) else str(text))
            data = output.get("data", {})
            if isinstance(data, dict):
                for value in data.values():
                    pieces.append("".join(value) if isinstance(value, list) else str(value))
    return "\n".join(pieces)


def executable_source(cell: dict[str, Any]) -> str:
    source = "".join(cell.get("source", []))
    return "\n".join(
        line for line in source.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


def validate_notebook(value: Any, topic: str) -> tuple[bool, str, str]:
    if not isinstance(value, dict) or not isinstance(value.get("cells"), list):
        return False, "not parsed", "Save KafkaDemo.ipynb as a valid Jupyter notebook."

    code_cells = [
        cell for cell in value["cells"]
        if isinstance(cell, dict)
        and cell.get("cell_type") == "code"
        and executable_source(cell)
    ]
    executed = [cell for cell in code_cells if isinstance(cell.get("execution_count"), int)]
    source = "\n".join(executable_source(cell) for cell in code_cells)
    required_patterns = (
        r"KafkaProducer\s*\(",
        r"\.send\s*\(",
        r"\.flush\s*\(",
        r"KafkaConsumer\s*\(",
        r"kafka_log\.csv",
    )
    structure_ok = all(re.search(pattern, source) for pattern in required_patterns)
    placeholders_ok = not re.search(r"(?:=|\[|\(|,)\s*\.\.\.(?:\s|,|\]|\)|$)", source)
    topic_ok = bool(topic and topic in notebook_text(value))

    errors: list[str] = []
    for cell in code_cells:
        cell_source = executable_source(cell)
        for output in cell.get("outputs", []):
            if not isinstance(output, dict) or output.get("output_type") != "error":
                continue
            # An unbounded Kafka consumer is normally stopped by the learner.
            expected_interrupt = output.get("ename") == "KeyboardInterrupt" and "KafkaConsumer" in cell_source
            if not expected_interrupt:
                errors.append(str(output.get("ename", "notebook error")))

    ok = (
        bool(code_cells)
        and len(executed) == len(code_cells)
        and structure_ok
        and placeholders_ok
        and topic_ok
        and not errors
    )
    identifier = f"{len(executed)}/{len(code_cells)} executable cells; topic {topic or 'missing'}"
    if ok:
        detail = "All executable cells were run; producer, consumer, log-writing structure, and the configured topic are present."
    else:
        reasons = []
        if len(executed) != len(code_cells):
            reasons.append("run every executable cell")
        if not structure_ok or not placeholders_ok:
            reasons.append("complete the producer, consumer, and log-writing placeholders")
        if not topic_ok:
            reasons.append("save notebook output identifying the configured topic")
        if errors:
            reasons.append("resolve saved execution errors")
        detail = "; ".join(reasons) or "Save a valid executed Kafka notebook."
    return ok, identifier, detail


def parse_kafka_records(text: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    nonempty = [line.strip() for line in text.splitlines() if line.strip()]
    for line in nonempty:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    if records:
        return records

    try:
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader if row]
    except (csv.Error, TypeError):
        return []


def validate_log(text: str) -> tuple[bool, list[dict[str, Any]], str]:
    records = parse_kafka_records(text)
    required = {"city", "timestamp", "temperature_f"}
    ok = bool(records) and all(required.issubset(record) for record in records)
    detail = (
        f"Parsed {len(records)} consumed record(s) with city, timestamp, and temperature_f fields."
        if ok else
        "Save at least one consumed JSON-line or CSV record with city, timestamp, and temperature_f fields."
    )
    return ok, records, detail


def string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    return []


def loopback_broker(value: str, port: int) -> bool:
    return bool(re.fullmatch(rf"(?:localhost|127\.0\.0\.1):{port}", value.strip(), re.IGNORECASE))


def validate_config(value: Any, local_port: int) -> tuple[bool, str, str, str]:
    if not isinstance(value, dict):
        return False, "", "", "Save the client configuration as a JSON object."
    topic = value.get("topic")
    producer = value.get("producer")
    consumer = value.get("consumer")
    if not isinstance(topic, str) or not isinstance(producer, dict) or not isinstance(consumer, dict):
        return False, str(topic or ""), "", "Include topic, producer, and consumer objects in the client configuration."

    producer_brokers = string_list(producer.get("bootstrap_servers"))
    consumer_brokers = string_list(consumer.get("bootstrap_servers"))
    serializer = producer.get("value_serializer")
    offset_reset = consumer.get("auto_offset_reset")
    commit = consumer.get("enable_auto_commit")
    brokers_ok = (
        bool(producer_brokers)
        and producer_brokers == consumer_brokers
        and all(loopback_broker(broker, local_port) for broker in producer_brokers)
    )
    serializer_ok = isinstance(serializer, str) and "json" in serializer.casefold() and "utf" in serializer.casefold()
    offset_ok = isinstance(offset_reset, str) and offset_reset.casefold() in {"earliest", "latest", "none"}
    ok = bool(topic.strip()) and brokers_ok and serializer_ok and offset_ok and isinstance(commit, bool)
    identifier = f"{topic}; {','.join(producer_brokers) or 'broker missing'}; {offset_reset or 'offset reset missing'}"
    detail = (
        "Producer and consumer share the loopback tunnel broker; JSON/UTF-8 serialization and consumer offset settings are recorded."
        if ok else
        "Record a non-empty topic, matching localhost tunnel brokers, JSON/UTF-8 value serialization, auto_offset_reset, and enable_auto_commit."
    )
    return ok, topic.strip(), str(offset_reset or ""), detail


def validate_tunnel(text: str, local_port: int) -> tuple[bool, str]:
    port_present = bool(re.search(rf"(?<!\d){local_port}(?!\d)", text))
    evidence_marker = bool(re.search(r"(?:\bssh\b|\blsof\b|\bLISTEN\b|-L\s*)", text, re.IGNORECASE))
    ok = port_present and evidence_marker
    return ok, (
        f"Saved tunnel evidence identifies local port {local_port}."
        if ok else
        f"Save the SSH tunnel command or listening-port output that identifies local port {local_port}."
    )


def option_value(tokens: list[str], option: str) -> str:
    try:
        index = tokens.index(option)
    except ValueError:
        return ""
    return tokens[index + 1] if index + 1 < len(tokens) else ""


def kcat_command(text: str) -> tuple[str, list[str]]:
    for line in text.splitlines():
        if re.search(r"(?:^|\s)kcat(?:\s|$)", line):
            command = re.sub(r"^\s*(?:\$|>)\s*", "", line.strip())
            try:
                return command, shlex.split(command)
            except ValueError:
                return command, []
    return "", []


def output_has_offset(text: str, command: str, observed_offset: int) -> bool:
    for line in text.splitlines():
        if line.strip() == command.strip():
            continue
        if re.match(rf"^\s*{observed_offset}\s*(?::|\||\t)", line):
            return True
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("offset") == observed_offset:
            return True
    return False


def validate_kcat(
    text: str, topic: str, local_port: int, observed_offset: int
) -> tuple[bool, bool, str, str]:
    command, tokens = kcat_command(text)
    broker = option_value(tokens, "-b")
    command_topic = option_value(tokens, "-t")
    start = option_value(tokens, "-o").casefold()
    format_value = option_value(tokens, "-f")
    includes_offset = "%o" in format_value or "-J" in tokens
    command_ok = (
        bool(tokens)
        and tokens[0].endswith("kcat")
        and loopback_broker(broker, local_port)
        and command_topic == topic
        and "-C" in tokens
        and start in {"earliest", "beginning"}
        and includes_offset
    )
    offset_ok = output_has_offset(text, command, observed_offset)
    ok = command_ok and offset_ok
    detail = (
        "The saved kcat command consumes the configured topic through the tunnel from the earliest offset and its output includes offsets."
        if ok else
        "Save a kcat consumer command using the configured broker/topic, -o earliest, and %o (or -J), followed by its output."
    )
    return ok, offset_ok, command, detail


def output_text(output: dict[str, Any]) -> str:
    if "text" in output:
        value = output["text"]
        return "".join(value) if isinstance(value, list) else str(value)
    if output.get("output_type") == "error":
        traceback = output.get("traceback", [])
        return "\n".join(traceback) if isinstance(traceback, list) else str(traceback)
    data = output.get("data", {})
    if isinstance(data, dict):
        value = data.get("text/plain", "")
        return "".join(value) if isinstance(value, list) else str(value)
    return ""


def render_notebook(notebook: dict[str, Any]) -> str:
    sections: list[str] = []
    for number, cell in enumerate(notebook.get("cells", []), start=1):
        if not isinstance(cell, dict):
            continue
        source = "".join(cell.get("source", []))
        outputs = "\n".join(
            output_text(output) for output in cell.get("outputs", []) if isinstance(output, dict)
        )
        execution = cell.get("execution_count")
        heading = f"Cell {number}: {cell.get('cell_type', 'unknown')}"
        if execution is not None:
            heading += f" [execution {execution}]"
        output_html = f"<h5>Saved output</h5><pre>{html.escape(outputs)}</pre>" if outputs else ""
        sections.append(f"<section><h4>{html.escape(heading)}</h4><pre>{html.escape(source)}</pre>{output_html}</section>")
    return "\n".join(sections)


def evidence_section(title: str, location: str, content: str, withheld: bool) -> str:
    body = "Content withheld because an obvious credential pattern was detected." if withheld else content
    return (
        f"<section><h3>{html.escape(title)}</h3><p><code>{html.escape(location)}</code></p>"
        f"<pre>{html.escape(body)}</pre></section>"
    )


def render_report(
    learner: str,
    generated_at: str,
    local_port: int,
    topic: str,
    observed_offset: int,
    checks: list[dict[str, str]],
    notebook: dict[str, Any],
    notebook_location: str,
    evidence: list[tuple[str, str, str]],
    secret_locations: set[str],
) -> str:
    rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(item['name'].replace('_', ' '))}</td>"
        f"<td class=\"{item['status']}\">{html.escape(item['status'])}</td>"
        f"<td>{html.escape(item['location'])}</td>"
        f"<td>{html.escape(item['identifier'])}</td>"
        f"<td>{html.escape(item['detail'])}</td>"
        "</tr>"
        for item in checks
    )
    notebook_body = (
        evidence_section("Executed notebook", notebook_location, "", True)
        if notebook_location in secret_locations else
        f"<section><h3>Executed notebook</h3><p><code>{html.escape(notebook_location)}</code></p>{render_notebook(notebook)}</section>"
    )
    evidence_body = "\n".join(
        evidence_section(title, location, content, location in secret_locations)
        for title, location, content in evidence
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Lab 3 Kafka Submission Report</title>
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 1100px; margin: 2rem auto; padding: 0 1rem; color: #172033; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #cbd5e1; padding: .55rem; text-align: left; vertical-align: top; }}
    th {{ background: #e2e8f0; }} .present {{ color: #166534; font-weight: 700; }} .missing {{ color: #991b1b; font-weight: 700; }}
    pre {{ white-space: pre-wrap; overflow-wrap: anywhere; background: #f8fafc; border: 1px solid #e2e8f0; padding: .75rem; }}
    .note {{ background: #fff7ed; border-left: .25rem solid #f97316; padding: .75rem; }}
  </style>
</head>
<body>
  <h1>Lab 3: Kafka Submission Report</h1>
  <dl>
    <dt>Learner</dt><dd>{html.escape(learner)}</dd>
    <dt>Generated</dt><dd>{html.escape(generated_at)}</dd>
    <dt>Topic</dt><dd><code>{html.escape(topic or 'not parsed')}</code></dd>
    <dt>Local tunnel port</dt><dd><code>{local_port}</code></dd>
    <dt>Observed offset cited for the spot check</dt><dd><code>{observed_offset}</code></dd>
  </dl>

  <h2>Completeness</h2>
  <table><thead><tr><th>Check</th><th>Status</th><th>Location</th><th>Identifier</th><th>Detail</th></tr></thead><tbody>{rows}</tbody></table>

  <h2>Durable evidence</h2>
  {notebook_body}
  {evidence_body}

  <h2>Staff spot checks</h2>
  <p class="note">This checker confirms saved evidence and internal consistency only. Answer these separately in Canvas; it does not judge either interpretation.</p>
  <ol>
    <li><strong>Offset:</strong> identify topic <code>{html.escape(topic or 'not parsed')}</code> and observed offset <code>{observed_offset}</code>, explain what that offset identifies, and cite the corresponding saved output line.</li>
    <li><strong>Start position:</strong> explain the saved <code>auto_offset_reset</code> choice and how the broker, topic, and offset option in the saved kcat command determine what it reads.</li>
  </ol>

  <h2>Safety check</h2>
  <p>The checker scans included text for common plaintext credential patterns. Review the finished report yourself before uploading it.</p>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the Lab 3 Kafka HTML report and JSON manifest using saved evidence only.")
    parser.add_argument("--learner", required=True)
    parser.add_argument("--notebook", required=True, type=Path)
    parser.add_argument("--kafka-log", required=True, type=Path)
    parser.add_argument("--client-config", required=True, type=Path)
    parser.add_argument("--tunnel-evidence", required=True, type=Path)
    parser.add_argument("--kcat-evidence", required=True, type=Path)
    parser.add_argument("--local-port", required=True, type=int)
    parser.add_argument("--observed-offset", required=True, type=int)
    parser.add_argument("--output-dir", default=Path("submission"), type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not 1 <= args.local_port <= 65535:
        print("--local-port must be between 1 and 65535.", file=sys.stderr)
        return 2
    if args.observed_offset < 0:
        print("--observed-offset must be zero or greater.", file=sys.stderr)
        return 2

    paths = {
        "executed notebook": args.notebook,
        "kafka_log.csv": args.kafka_log,
        "client configuration": args.client_config,
        "tunnel-port evidence": args.tunnel_evidence,
        "kcat command and output": args.kcat_evidence,
    }
    missing = [f"{label}: {path}" for label, path in paths.items() if not path.is_file()]
    if missing:
        print("Missing required saved evidence:\n  " + "\n  ".join(missing), file=sys.stderr)
        return 2

    root = Path.cwd()
    locations = {label: display_path(root, path) for label, path in paths.items()}
    texts = {label: read_text(path) for label, path in paths.items()}
    notebook = parse_json(args.notebook)
    config = parse_json(args.client_config)
    config_ok, topic, offset_reset, config_detail = validate_config(config, args.local_port)
    notebook_ok, notebook_identifier, notebook_detail = validate_notebook(notebook, topic)
    log_ok, records, log_detail = validate_log(texts["kafka_log.csv"])
    tunnel_ok, tunnel_detail = validate_tunnel(texts["tunnel-port evidence"], args.local_port)
    kcat_ok, offset_ok, command, kcat_detail = validate_kcat(
        texts["kcat command and output"], topic, args.local_port, args.observed_offset
    )

    secret_locations = {
        locations[label] for label, content in texts.items() if contains_secret(content)
    }
    secrets_ok = not secret_locations
    kcat_has_secret = locations["kcat command and output"] in secret_locations
    structure_ok = (
        isinstance(notebook, dict)
        and isinstance(config, dict)
        and bool(texts["tunnel-port evidence"].strip())
        and bool(texts["kcat command and output"].strip())
        and bool(records)
    )
    artifact_digest = hashlib.sha256(
        "".join(sha256(path) for path in paths.values()).encode("ascii")
    ).hexdigest()[:16]

    report_location = REPORT_NAME
    checks = [
        check(
            "required_artifact_structure", structure_ok, report_location, artifact_digest,
            "Notebook and configuration parse, the saved text evidence is non-empty, and kafka_log.csv contains records."
            if structure_ok else
            "Provide a parseable notebook and JSON configuration, non-empty tunnel/kcat evidence, and a parseable kafka_log.csv.",
        ),
        check("executed_notebook", notebook_ok, locations["executed notebook"], notebook_identifier, notebook_detail),
        check(
            "consumed_kafka_log", log_ok, locations["kafka_log.csv"], f"{len(records)} record(s)", log_detail,
        ),
        check(
            "non_secret_client_configuration", config_ok and locations["client configuration"] not in secret_locations,
            locations["client configuration"],
            f"{topic or 'topic missing'}; auto_offset_reset={offset_reset or 'missing'}",
            config_detail if locations["client configuration"] not in secret_locations else "Remove credentials from the saved client configuration.",
        ),
        check(
            "tunnel_port_evidence", tunnel_ok, locations["tunnel-port evidence"], str(args.local_port), tunnel_detail,
        ),
        check(
            "earliest_offset_kcat_evidence", kcat_ok and not kcat_has_secret,
            locations["kcat command and output"],
            "content withheld" if kcat_has_secret else (command or "command not parsed"),
            "Remove credentials from the saved kcat evidence."
            if kcat_has_secret else kcat_detail,
        ),
        check(
            "observed_offset_consistency", offset_ok, report_location, str(args.observed_offset),
            "The report's cited observed offset appears as an offset in the saved kcat output."
            if offset_ok else
            "Choose an --observed-offset that appears as an offset in the saved kcat output.",
        ),
        check(
            "obvious_credential_leakage", secrets_ok, report_location,
            "none detected" if secrets_ok else "content withheld",
            "No common plaintext credential pattern was found."
            if secrets_ok else
            "Remove and revoke plaintext credentials before regenerating; affected content was withheld.",
        ),
    ]

    notebook_value = notebook if isinstance(notebook, dict) else {"cells": []}
    evidence = [
        ("Consumed Kafka log", locations["kafka_log.csv"], texts["kafka_log.csv"]),
        ("Non-secret client configuration", locations["client configuration"], texts["client configuration"]),
        ("Tunnel-port evidence", locations["tunnel-port evidence"], texts["tunnel-port evidence"]),
        ("kcat command and output", locations["kcat command and output"], texts["kcat command and output"]),
    ]
    generated_at = datetime.now(timezone.utc).isoformat()
    report = render_report(
        args.learner, generated_at, args.local_port, topic, args.observed_offset,
        checks, notebook_value, locations["executed notebook"], evidence, secret_locations,
    )
    complete = all(item["status"] == "present" for item in checks)
    manifest = {
        "schema_version": "1.0",
        "lab": "Lab 3: Kafka",
        "learner": args.learner,
        "generated_at": generated_at,
        "checker_version": CHECKER_VERSION,
        "complete": complete,
        "report": REPORT_NAME,
        "identifiers": {
            "topic": topic,
            "local_port": args.local_port,
            "auto_offset_reset": offset_reset,
            "observed_offset": args.observed_offset,
        },
        "artifacts": {
            label: {"path": locations[label], "sha256": sha256(path)}
            for label, path in paths.items()
        },
        "checks": checks,
        "manual_spot_checks": ["offset", "start_position"],
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / REPORT_NAME).write_text(report, encoding="utf-8")
    (args.output_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.output_dir / REPORT_NAME}")
    print(f"Wrote {args.output_dir / MANIFEST_NAME}")
    print("Submission evidence is complete." if complete else "Submission evidence is incomplete; open the report for details.")
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
