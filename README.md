# Lab 2: Kafka for Data Streaming

In this lab, you will gain hands-on experience with Apache Kafka, a distributed streaming platform that plays a key role in processing large-scale real-time data.
You will establish a connection to a Kafka broker, produce and consume messages, and explore Kafka command-line tools.
This lab will prepare you for your group project, where you will work with Kafka streams.

Complete each deliverable and save the requested outputs and explanations as evidence of your work.

## Deliverables

- [ ] Establish a secure SSH tunnel to the Kafka server and save the connection evidence.
      Explain in your lab notes how topics and offsets support message continuity when a consumer disconnects.
- [ ] Modify starter code to implement producer and consumer modes for a Kafka topic.
      Explain the tradeoffs of the different *auto_offset_reset* values.
- [ ] Demonstrate using Kafka's CLI tool *kcat* (or alternatives) to manage and monitor Kafka topics and messages.

## Generate the Submission Report

Run every executable notebook cell and save `KafkaDemo.ipynb` with its outputs before closing the broker connection.
Keep the complete `kafka_log.csv`, the SSH tunnel command or listening-port output, and the exact `kcat` command followed by its output in an `evidence/` directory.
The `kcat` command must consume from the earliest offset and print offsets, for example:

```bash
mkdir -p evidence
lsof -i :9092 | tee evidence/tunnel.txt
{
  printf '%s\n' 'kcat -F ~/.config/mlip-kafka.conf -b localhost:9092 -t lab02-asmith -C -o earliest -c 5 -f "%o: %s\n"'
  kcat -F ~/.config/mlip-kafka.conf -b localhost:9092 -t lab02-asmith -C -o earliest -c 5 -f "%o: %s\n"
} | tee evidence/kcat.txt
```

Record the non-secret client settings in `evidence/client-config.json`.
Use the same local tunnel broker in both client sections, describe the producer serialization, and do not include the SSH password or other credentials.

```json
{
  "topic": "lab02-asmith",
  "producer": {
    "bootstrap_servers": ["localhost:9092"],
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_username": "students",
    "value_serializer": "JSON encoded as UTF-8 bytes"
  },
  "consumer": {
    "bootstrap_servers": ["localhost:9092"],
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_username": "students",
    "auto_offset_reset": "earliest",
    "enable_auto_commit": true
  }
}
```

Generate the report from the repository root, replacing the example name, port, and observed offset with values from your saved evidence:

```bash
python3 scripts/generate-kafka-submission.py \
  --learner "Your name" \
  --notebook KafkaDemo.ipynb \
  --kafka-log kafka_log.csv \
  --client-config evidence/client-config.json \
  --tunnel-evidence evidence/tunnel.txt \
  --kcat-evidence evidence/kcat.txt \
  --local-port 9092 \
  --observed-offset 12
```

Open `submission/kafka-report.html` and correct every item marked `missing` before uploading it to Canvas.
Keep `submission/kafka-manifest.json` with the raw evidence.
The command reads saved files only: it does not open an SSH tunnel, reconnect to Kafka, or rerun the notebook.
It checks notebook execution, the required artifact structure, matching topic/broker/port settings, an earliest-offset `kcat` command, consistency of the cited observed offset, and obvious plaintext credential leakage.
Answer the offset and start-position interpretation questions separately in Canvas; the checker does not decide whether those explanations are correct.

## Getting started

The recommended environment is the provided Linux DevContainer, which includes Python, the notebook dependencies, SSH tools, and `kcat`.

1. Select [Open in GitHub Codespaces](https://codespaces.new/mlip-cmu-online/lab-kafka?quickstart=1).
2. Wait for the post-create command to finish before opening `KafkaDemo.ipynb`.
3. Run the following checks in the Codespace terminal.

   ```bash
   python -c "from kafka import KafkaProducer; print('kafka-python ready')"
   kcat -V
   ssh -V
   ```

If you prefer to work locally, clone this repository and reopen it in its DevContainer.
You can instead create a local Python virtual environment, although you will need to install `kcat` and an SSH client separately.

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Check the [bug list and solutions](./bug_list.md) if you encounter common environment, connection, or code problems.

## Connecting to Kafka server

1. Retrieve the SSH tunnel details and shared Kafka credential from the Canvas entry for this lab.
   Do not save either credential in the repository or notebook.
   Use SSH to create a foreground tunnel to the Kafka server.

   ```bash
   ssh -o ExitOnForwardFailure=yes -o ServerAliveInterval=60 -L 9092:localhost:<remote_port> <user>@<remote_server> -NT
   ```

   Kafka advertises `localhost:9092`, so the local end of the tunnel must be
   port `9092`. This will be your `bootstrap_servers` address.
   Keep this terminal open while you use Kafka, and press <kbd>Ctrl</kbd>+<kbd>C</kbd> in it when you finish the lab to close the tunnel.

2. In a second terminal, create a private `kcat` configuration outside the repository, then test broker reachability through the tunnel.

   ```bash
   mkdir -p ~/.config && chmod 700 ~/.config
   read -rsp "Kafka credential: " KAFKA_CREDENTIAL
   printf '\nsecurity.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.username=students\nsasl.password=%s\n' \
     "${KAFKA_CREDENTIAL}" > ~/.config/mlip-kafka.conf
   chmod 600 ~/.config/mlip-kafka.conf
   unset KAFKA_CREDENTIAL
   kcat -F ~/.config/mlip-kafka.conf -b localhost:9092 -L
   ```

   Continue only when the command returns broker and topic metadata.
   If it fails, confirm that the tunnel terminal is still open and that the local port matches the value used in the command.

## Implementing Producer-Consumer Mode

### 1. Producer Mode: Writes Data to Broker

Refer to the TODO sections in the notebook.
Edit the bootstrap servers and add two or three cities of your choice.
Run the code to write to the Kafka stream.

### 2. Consumer Mode: Reads Data from Broker

Modify the TODO section by filling appropriate parameters/arguments in the starter code.
Verify `kafka_log.csv`.

References:

- [KafkaProducer Documentation](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html)
- [KafkaConsumer Documentation](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html)

## Using Kafka's CLI tools

`kcat` is a command-line interface that was previously known as kafkacat.
Install with your package installer such as:

- macOS: `brew install kcat`
- Ubuntu: `apt-get install kcat`
- Windows: Use the provided Codespace or DevContainer because native Windows setup is complex.

Using the kcat documentation, write a command that uses `-F ~/.config/mlip-kafka.conf`, connects to the local Kafka broker, specifies a topic, and consumes messages from the earliest offset.

References:

- [kcat usage](https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html)
- [kcat GitHub](https://github.com/edenhill/kcat)

## Optional but Recommended

For your group project you will be reading movies from the Kafka stream.
Try finding the list of all topics and then read some movielog streams to get an idea of what the data looks like.

`kcat -F ~/.config/mlip-kafka.conf -b localhost:9092 -L`

## Additional resources

- [Kafka Introduction Video 1](https://www.youtube.com/watch?v=PzPXRmVHMxI) <- Recommended video for a quick 5-min introduction to Kafka
- [Kafka Introduction Video 2](https://www.youtube.com/watch?v=JalUUBKdcA0)
- [Apache Kafka](https://kafka.apache.org/)
- [Kafka for Beginners](https://www.cloudkarafka.com/blog/2016-11-30-part1-kafka-for-beginners-what-is-apache-kafka.html)
- [What is Apache Kafka? - TIBCO](https://www.tibco.com/reference-center/what-is-apache-kafka)
- [Common bugs and solutions](./bug_list.md) - Troubleshooting guide for connection issues, code errors, and environment setup
