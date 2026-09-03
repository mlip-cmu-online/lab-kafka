# Lab: Kafka for Data Streaming

In this lab, you will gain hands-on experience with Apache Kafka, a distributed streaming platform that plays a key role in processing large-scale real-time data.
You will establish a connection to a Kafka broker, produce and consume messages, and explore Kafka command-line tools.
This lab will prepare you for your group project, where you will work with Kafka streams.

Complete each deliverable in `KafkaDemo.ipynb`.

## Deliverables

- [ ] Establish a secure SSH tunnel to the Kafka server.
- [ ] Modify starter code to implement producer and consumer modes for a Kafka topic.
- [ ] Demonstrate using Kafka's CLI tool *kcat* (or alternatives) to manage and monitor Kafka topics and messages.
- [ ] Answer the two reflection prompts at the end of the notebook.

### Submission

Submit one file: `KafkaDemo.ipynb` with your code, answers, and cell outputs saved.
Before submitting:

1. Run the producer, consumer, and `kcat` cells successfully.
2. Answer both reflection prompts in the final notebook cell.
3. Save the notebook so its outputs are included.
4. Confirm that no password or other credential appears in the notebook.

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

Retrieve the SSH tunnel details and the shared Kafka credential from the Canvas entry for this lab.
These are two separate credentials: an SSH password that opens the tunnel, and a Kafka password that the client authenticates with.
From the lab directory, run the following command using the non-secret values supplied in Canvas:

```bash
./connect-kafka <user>@<remote_server> <remote_port>
```

Enter each password only when prompted.
The helper creates a private Kafka configuration outside the repository, opens the SSH tunnel in the background, and tests the connection.
Continue when it prints `Connected to Kafka on localhost:9092`.

Kafka advertises `localhost:9092`, so the helper always uses that local port.
The notebook and `kcat` both reuse the private configuration at `~/.config/mlip-kafka.conf`; neither will ask for the Kafka password again.
Do not save either credential in the repository or notebook.

When you finish the lab, close the tunnel:

```bash
./disconnect-kafka
```

## Implementing Producer-Consumer Mode

### 1. Producer Mode: Writes Data to Broker

Refer to the TODO sections in the notebook.
Add two or three cities of your choice and complete the producer serializer TODO.
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

Run the provided notebook command, which loads `~/.config/mlip-kafka.conf`, specifies your topic, and consumes messages from the earliest offset.

References:

- [kcat usage](https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html)
- [kcat GitHub](https://github.com/edenhill/kcat)

## Optional but Recommended

For your group project you will be reading movies from the Kafka stream.
Try finding the list of all topics and then read some movielog streams to get an idea of what the data looks like.

`kcat -F ~/.config/mlip-kafka.conf -L`

## Additional resources

- [Kafka Introduction Video 1](https://www.youtube.com/watch?v=PzPXRmVHMxI) <- Recommended video for a quick 5-min introduction to Kafka
- [Kafka Introduction Video 2](https://www.youtube.com/watch?v=JalUUBKdcA0)
- [Apache Kafka](https://kafka.apache.org/)
- [Kafka for Beginners](https://www.cloudkarafka.com/blog/2016-11-30-part1-kafka-for-beginners-what-is-apache-kafka.html)
- [What is Apache Kafka? - TIBCO](https://www.tibco.com/reference-center/what-is-apache-kafka)
- [Common bugs and solutions](./bug_list.md) - Troubleshooting guide for connection issues, code errors, and environment setup
