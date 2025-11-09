# Image-Processing-Pipeline-Using-Kafka
#  Distributed Image Processing Pipeline with Kafka

##  Overview

This project implements a **Distributed Image Processing Pipeline** that leverages **Apache Kafka** for asynchronous communication between a **Master Node** and multiple **Worker Nodes**.  

It enables **parallel**, **scalable**, and **fault-tolerant** image processing by splitting a large image into smaller tiles, distributing them to multiple workers for transformation (e.g., grayscale, blur), and then reconstructing the processed tiles back into a final image.

The architecture follows a **producer–broker–consumer** pattern, where the master node acts as a producer, the Kafka broker manages message flow, and worker nodes act as consumers.

---

##  System Architecture

The system consists of four key components:

1. **Client & Master Node**
   - The client (Flask or FastAPI web interface) allows users to upload images and view results.  
   - The master node:
     - Splits uploaded images into smaller tiles.
     - Publishes each tile to the Kafka **`tasks`** topic.
     - Subscribes to the **`results`** topic to collect processed tiles.
     - Reconstructs the processed image and returns it to the user.
     - Listens to **`heartbeats`** to monitor worker health.

2. **Kafka Broker**
   - Acts as the central message hub for communication.
   - Manages three main topics:
     - **`tasks`** → distributes image tiles from master to workers  
     - **`results`** → collects processed tiles from workers  
     - **`heartbeats`** → receives worker health signals  
   - Handles **partitioning**, **message persistence**, and **consumer group balancing**.

3. **Worker Nodes (2)**
   - Each worker subscribes to the **`tasks`** topic as part of the same consumer group.
   - Processes assigned image tiles independently using OpenCV or Pillow (e.g., grayscale, blur).
   - Publishes processed tiles to the **`results`** topic.
   - Sends periodic heartbeat messages to **`heartbeats`** topic to report status.

4. **Monitoring Dashboard**
   - Displays:
     - Number of active workers
     - Task progress and job completion status
     - Live heartbeat data

---

##  Key Features

- **Parallel Processing:** Image split into multiple tiles processed concurrently by different workers.  
- **Load Balancing:** Kafka consumer groups ensure fair distribution of work.  
- **Scalability:** Easily add more worker nodes for faster processing.  
- **Fault Tolerance:** Heartbeat monitoring detects inactive workers.  
- **Asynchronous Communication:** Kafka enables loose coupling between master and workers.  
- **Persistence:** Kafka retains messages for a fixed duration, ensuring reliability.  

---

## ⚙️ Technology Stack

| Component | Technology Used |
|------------|-----------------|
| Programming Language | Python |
| Message Broker | Apache Kafka |
| Kafka API | Confluent Kafka |
| Image Processing | OpenCV / Pillow / NumPy |
| Web Framework | Flask or FastAPI |
| Database (optional) | SQLite or Redis |
| Dashboard | HTML/CSS + Flask Templates |

---

##  Execution Guide

Follow these steps **in order** to run the full pipeline.

---

### **Step 1: Start Zookeeper**
```bash
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Zookeeper manages the Kafka cluster coordination.

---

### **Step 2: Start Kafka Broker**
In a new terminal:
```bash
cd /opt/kafka
bin/kafka-server-start.sh config/server.properties
```
This launches the Kafka broker which handles message flow between master and workers.

---

### **Step 3: Create Required Kafka Topics**
```bash
cd /opt/kafka

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic tasks --partitions 2 --replication-factor 1 --config retention.ms=3600000

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic results --partitions 2 --replication-factor 1 --config retention.ms=3600000

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic heartbeats --partitions 1 --replication-factor 1 --config retention.ms=300000
```
Creates 3 topics with appropriate partitions and message retention time.

---

### **Step 4: Start the Worker Nodes**
Run each worker on separate terminals or machines:
```bash
python3 worker1.py
python3 worker2.py
```
Each worker will consume image tiles, process them, and send results back.

---

### **Step 5: Start the Master Node**
```bash
python3 app.py
```
The master node handles:
- Receiving images from client  
- Splitting them into tiles  
- Publishing tasks to Kafka  
- Collecting processed tiles from results topic  
- Reconstructing the final output image  

---

### **Step 6: Start the Client Web Interface**
```bash
python3 app.py
```
Open your browser and visit:
```
http://localhost:5000
```
You can now upload an image, monitor processing progress, and download the final result.

---

### **Step 7: Monitor Worker Heartbeats**
(Optional – for debugging or monitoring)
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic heartbeats --from-beginning
```
Displays live worker heartbeat messages, showing which workers are active.

---

### **Step 8: Retrieve Processed Image**
Once processing is complete, the reconstructed image will be available for download from the client web interface.

---

### **Step 9: (Optional) Delete Topics**
If you want to clean up topics after testing:
```bash
cd /opt/kafka

bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic tasks
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic results
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic heartbeats
```

---

## 📊 Expected Output

- Uploaded image is split into tiles.
- Tiles are distributed to workers via Kafka.
- Each worker processes assigned tiles (e.g., grayscale, blur).
- Master node collects processed tiles and reconstructs the image.
- Final processed image available for download in the web UI.
- Dashboard shows worker heartbeat and progress.

---

##  Summary

This project demonstrates the principles of **distributed systems**, **asynchronous communication**, and **parallel computing** using **Apache Kafka** as a message broker.  
It showcases:
- End-to-end data flow
- Real-time coordination between components
- Practical application of message queues for scalable image processing.
