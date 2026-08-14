# Common Bugs and Solutions

## Connection Issues

### Error: `NoBrokersAvailable: NoBrokersAvailable`
**Problem**: Not connected to the Kafka server via SSH tunnel.

**Solution**: 
1. Make sure you've established the SSH tunnel first:
   ```bash
   ssh -L <local_port>:localhost:<remote_port> <user>@<remote_server> -NTf
   ```
2. Verify the tunnel is active: `lsof -i :<local_port>` (should show ssh process)
3. Use the same port number in your `bootstrap_servers` parameter

---

### Error: `kcat` connection failures
```
% ERROR: Failed to query metadata for topic <topic_name>: Local: Broker transport failure
Connect to ipv6#[::1]:9092 failed: Connection refused
```

**Problem**: SSH tunnel not established before running kcat.

**Solution**: 
- Always establish SSH tunnel BEFORE running kcat commands
- Use: `ssh -o ServerAliveInterval=60 -L 9092:localhost:9092 <user>@<remote_server> -NTf`
- Verify connection with: `kcat -b localhost:9092 -L` (should list topics)

---

### Error: `Port already in use` or `Address already in use`
**Problem**: Another SSH tunnel is already using that port, or previous tunnel wasn't killed.

**Solution**:
1. Find and kill existing tunnel:
   ```bash
   lsof -ti:<local_port> | xargs kill -9
   ```
2. Or use a different local port in your SSH command

---

## Code Issues

### Error: `TypeError: a bytes-like object is required, not 'str'`
**Problem**: Trying to send string directly instead of bytes, or consumer trying to decode already-decoded data.

**Solution**:
- For Producer: Use `value_serializer=lambda v: json.dumps(v).encode('utf-8')` or `value_serializer=lambda m: dumps(m).encode('utf-8')`
- For Consumer: If producer used serializer, consumer needs `value_deserializer=lambda m: loads(m.decode('utf-8'))`. Otherwise, manually decode: `message.value.decode('utf-8')`

---

### Error: Consumer not reading messages / Consumer reads old messages
**Problem**: `auto_offset_reset` setting or consumer group behavior.

**Solution**:
- Use `auto_offset_reset='earliest'` to read from beginning
- Use `auto_offset_reset='latest'` to read only new messages
- If using same consumer group, Kafka remembers your offset. Either:
  - Use a different `group_id` each time, OR
  - Set `auto_offset_reset='earliest'` and `enable_auto_commit=False` for testing

---

### Error: `Topic does not exist` or `UnknownTopicOrPartitionException`
**Problem**: Topic hasn't been created yet, or wrong topic name.

**Solution**:
1. Make sure you ran the producer code first to create the topic
2. Verify topic exists: `kcat -b localhost:9092 -L` (lists all topics)
3. Check topic name spelling matches exactly (case-sensitive)

---

### Error: `AttributeError: 'dict' object has no attribute 'decode'`
**Problem**: Consumer code trying to decode when value_deserializer already decoded the message.

**Solution**: 
- If using `value_deserializer` in consumer, `message.value` is already a dict, no need to decode/loads
- If NOT using deserializer, then decode: `message.value.decode('utf-8')` then `loads(...)`

---

## Environment Issues

### Error: `ModuleNotFoundError: No module named 'kafka'`
**Problem**: kafka-python not installed or wrong Python environment.

**Solution**:
1. Activate your virtual environment: `source <env_name>/bin/activate`
2. Install: `pip install kafka-python` or `pip install -r requirements.txt`
3. Verify: `python -c "from kafka import KafkaProducer; print('OK')"`

---

### Error: `kcat: command not found`
**Problem**: kcat not installed.

**Solution**:
- macOS: `brew install kcat`
- Ubuntu/Debian: `sudo apt-get install kcat`
- Windows: Use WSL or another Linux environment for this deliverable

---

## General Troubleshooting Tips

1. **Always check SSH tunnel first**: `lsof -i :<your_port>` should show an ssh process
2. **Test connection**: Try `kcat -b localhost:<port> -L` to list topics before running Python code
3. **Check topic name**: Make sure producer and consumer use the exact same topic name
4. **Restart consumer**: If consumer seems stuck, stop it (Ctrl+C) and restart with a new group_id
5. **Verify data format**: Print `message.value` before processing to see what format you're getting
