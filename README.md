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

1. Retrieve the `remote_server`, `remote_port`, `user`, and password from the Canvas entry for this lab.
   Enter the password only when SSH prompts for it, and do not save these credentials in the repository or notebook.
   Use SSH to create a foreground tunnel to the Kafka server.

   ```bash
   ssh -o ExitOnForwardFailure=yes -o ServerAliveInterval=60 -L <local_port>:localhost:<remote_port> <user>@<remote_server> -NT
   ```

   Use the same `<local_port>` throughout the lab, such as `9092`.
   This port will be your `bootstrap_servers` address.
   Keep this terminal open while you use Kafka, and press <kbd>Ctrl</kbd>+<kbd>C</kbd> in it when you finish the lab to close the tunnel.

2. Open a second terminal and test broker reachability through the tunnel.

   ```bash
   kcat -b localhost:<local_port> -L
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

Using the kcat documentation, write a command that connects to the local Kafka broker, specifies a topic, and consumes messages from the earliest offset.

References:

- [kcat usage](https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html)
- [kcat GitHub](https://github.com/edenhill/kcat)

## Optional but Recommended

For your group project you will be reading movies from the Kafka stream.
Try finding the list of all topics and then read some movielog streams to get an idea of what the data looks like.

`kcat -b localhost:9092 -L`

## Additional resources

- [Kafka Introduction Video 1](https://www.youtube.com/watch?v=PzPXRmVHMxI) <- Recommended video for a quick 5-min introduction to Kafka
- [Kafka Introduction Video 2](https://www.youtube.com/watch?v=JalUUBKdcA0)
- [Apache Kafka](https://kafka.apache.org/)
- [Kafka for Beginners](https://www.cloudkarafka.com/blog/2016-11-30-part1-kafka-for-beginners-what-is-apache-kafka.html)
- [What is Apache Kafka? - TIBCO](https://www.tibco.com/reference-center/what-is-apache-kafka)
- [Common bugs and solutions](./bug_list.md) - Troubleshooting guide for connection issues, code errors, and environment setup
