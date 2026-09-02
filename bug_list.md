# Common Bugs and Solutions

## Connection Issues

### Error: `NoBrokersAvailable: NoBrokersAvailable`

The Python client cannot reach the Kafka broker through the forwarded local port.
The broker advertises itself as `localhost:9092`, so the local end of the tunnel must be port `9092` and `bootstrap_servers` must be `localhost:9092`.

1. Confirm that the foreground SSH tunnel is still running in its terminal.
2. Confirm that the notebook uses `localhost:9092`.
3. Check the tunnel and broker metadata from a second terminal.

   ```bash
   lsof -i :9092
   kcat -F ~/.config/mlip-kafka.conf -b localhost:9092 -L
   ```

Recreate the tunnel with the Canvas-supplied connection details if no SSH process is listening.

```bash
ssh -o ExitOnForwardFailure=yes -o ServerAliveInterval=60 -L 9092:localhost:<remote_port> <user>@<remote_server> -NT
```

Do not save the SSH or shared Kafka credentials in the repository or notebook.

### Error: `kcat` Broker Transport Failure

This error usually means that `kcat` cannot reach the forwarded local port.

1. Confirm that the SSH tunnel terminal is still open.
2. Use `localhost:9092` as the broker address.
3. Run the metadata check before trying to consume messages.

   ```bash
   kcat -F ~/.config/mlip-kafka.conf -b localhost:9092 -L
   ```

### Error: `Port already in use` or `Address already in use`

Another process or an earlier SSH tunnel is already using port `9092`.
Free that port rather than choosing a different one: the broker advertises `localhost:9092`, so a tunnel on any other local port will connect and then fail to reach the broker.

1. Run `lsof -i :9092` to identify the process.
2. Press <kbd>Ctrl</kbd>+<kbd>C</kbd> in the earlier tunnel terminal if it is still open.
3. Otherwise stop the process that holds the port, then start the tunnel again.
   In a Codespace, also check that port `9092` is not forwarded in the **Ports** tab.

### Error: `SASL authentication failed`

Re-enter the shared Kafka credential from Canvas.
In the notebook, rerun the setup cell so it prompts again.
For `kcat`, recreate `~/.config/mlip-kafka.conf` using the README instructions.
Never paste the credential into the notebook or saved evidence.

## Code Issues

### Error: `TypeError: a bytes-like object is required, not 'str'`

Kafka messages must be serialized to bytes before the producer sends them.
Use `value_serializer=lambda value: dumps(value).encode("utf-8")` in the producer.
Decode and parse the bytes in the consumer only when a `value_deserializer` has not already done so.

### Consumer Reads No Messages or Unexpected Old Messages

The result depends on the consumer group and its committed offset.
Use `auto_offset_reset="earliest"` to start at the beginning only when the consumer group has no committed offset.
Use `auto_offset_reset="latest"` to wait for messages produced after that new consumer starts.
Use a new `group_id` or disable automatic commits when you need a repeatable experiment with reset behavior.

### Error: `Topic does not exist` or `UnknownTopicOrPartitionException`

The producer may not have created the topic yet, or the producer and consumer topic names may differ.

1. Run the producer before the consumer.
2. Confirm that your Andrew ID or other unique identifier appears in the topic name.
3. Run `kcat -F ~/.config/mlip-kafka.conf -b localhost:9092 -L` and check the topic spelling exactly.

### Error: `'dict' object has no attribute 'decode'`

A configured `value_deserializer` has already converted `message.value` into a dictionary.
Use the dictionary directly instead of decoding and parsing it again.

## Environment Issues

### Error: `ModuleNotFoundError: No module named 'kafka'`

In a Codespace or DevContainer, wait for the post-create command to finish and select `.venv/bin/python` as the notebook kernel.
In a local environment, activate the virtual environment and install the requirements.

```bash
source <environment_name>/bin/activate
python -m pip install -r requirements.txt
python -c "from kafka import KafkaProducer; print('kafka-python ready')"
```

### Error: `kcat: command not found`

Rebuild the Codespace or DevContainer because the provided Linux image installs `kcat` automatically.
For an unsupported local environment, install `kcat` with the operating system package manager.

```bash
# Ubuntu or Debian
sudo apt-get update
sudo apt-get install kcat
```

## Final Checks

1. Confirm that the tunnel terminal is still open.
2. Confirm that SSH, Python, and `kcat` all use port `9092`.
3. Confirm that the producer and consumer use the same unique topic name.
4. Stop a consumer that is waiting indefinitely with <kbd>Ctrl</kbd>+<kbd>C</kbd> and recheck its group and reset settings.
5. Print `message.value` before processing it when the serialized data format is unclear.
