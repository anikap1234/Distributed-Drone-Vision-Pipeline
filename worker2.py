import cv2
import numpy as np
import json
import base64
from confluent_kafka import Consumer, Producer
import time
from datetime import datetime
import threading
import sys
import uuid

class WorkerNode:
    def __init__(self, kafka_broker, worker_id=None):
        self.kafka_broker = kafka_broker
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        
        self.consumer_config = {
            'bootstrap.servers': kafka_broker,
            'group.id': 'image-processing-workers',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'client.id': self.worker_id
        }
        
        self.producer_config = {
            'bootstrap.servers': kafka_broker,
            'client.id': self.worker_id
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.producer = Producer(self.producer_config)
        
        self.is_running = True
        self.processed_count = 0
        
    def decode_tile(self, encoded_tile):
        """Decode base64 string to image"""
        img_data = base64.b64decode(encoded_tile)
        nparr = np.frombuffer(img_data, np.uint8)
        return cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    
    def encode_tile(self, tile):
        """Encode image as base64 string"""
        _, buffer = cv2.imencode('.png', tile)
        return base64.b64encode(buffer).decode('utf-8')
    
    def apply_transformation(self, tile, transformation):
        """Apply image transformation"""
        if transformation == 'grayscale':
            gray = cv2.cvtColor(tile, cv2.COLOR_BGR2GRAY)
            return cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
        
        elif transformation == 'blur':
            return cv2.GaussianBlur(tile, (15, 15), 0)
        
        elif transformation == 'edge':
            gray = cv2.cvtColor(tile, cv2.COLOR_BGR2GRAY)
            edges = cv2.Canny(gray, 100, 200)
            return cv2.cvtColor(edges, cv2.COLOR_GRAY2BGR)
        
        elif transformation == 'sharpen':
            kernel = np.array([[-1,-1,-1],
                              [-1, 9,-1],
                              [-1,-1,-1]])
            return cv2.filter2D(tile, -1, kernel)
        
        elif transformation == 'sepia':
            kernel = np.array([[0.272, 0.534, 0.131],
                              [0.349, 0.686, 0.168],
                              [0.393, 0.769, 0.189]])
            return cv2.transform(tile, kernel)
        
        elif transformation == 'invert':
            return cv2.bitwise_not(tile)
        
        elif transformation == 'brightness':
            return cv2.convertScaleAbs(tile, alpha=1.2, beta=30)
        
        else:
            # Default: return original
            return tile
    
    def process_tile(self, task):
        """Process a single tile"""
        job_id = task['job_id']
        tile_id = task['tile_id']
        tile_data = task['tile_data']
        transformation = task['transformation']
        
        print(f"[{self.worker_id}] Processing job {job_id}, tile {tile_id} - {transformation}")
        
        # Decode tile
        tile = self.decode_tile(tile_data)
        
        # Apply transformation
        processed_tile = self.apply_transformation(tile, transformation)
        
        # Encode result
        encoded_result = self.encode_tile(processed_tile)
        
        # Prepare result message
        result = {
            'job_id': job_id,
            'tile_id': tile_id,
            'tile_data': encoded_result,
            'worker_id': self.worker_id,
            'processed_at': datetime.now().isoformat()
        }
        
        # Publish result
        self.producer.produce(
            'results',
            key=str(job_id),
            value=json.dumps(result),
            callback=self.delivery_report
        )
        self.producer.flush()
        
        self.processed_count += 1
        print(f"[{self.worker_id}] Completed tile {tile_id} (Total: {self.processed_count})")
    
    def delivery_report(self, err, msg):
        """Callback for message delivery"""
        if err is not None:
            print(f'[{self.worker_id}] Delivery failed: {err}')
    
    def send_heartbeat(self):
        """Send periodic heartbeat messages"""
        while self.is_running:
            heartbeat = {
                'worker_id': self.worker_id,
                'timestamp': datetime.now().isoformat(),
                'status': 'active',
                'processed_count': self.processed_count
            }
            
            self.producer.produce(
                'heartbeats',
                key=self.worker_id,
                value=json.dumps(heartbeat)
            )
            self.producer.flush()
            
            print(f"[{self.worker_id}] Heartbeat sent")
            time.sleep(5)  # Send heartbeat every 5 seconds
    
    def start(self):
        """Start worker node"""
        print(f"[{self.worker_id}] Starting worker node...")
        print(f"[{self.worker_id}] Connecting to Kafka broker: {self.kafka_broker}")
        
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        heartbeat_thread.start()
        
        # Subscribe to tasks topic
        self.consumer.subscribe(['tasks'])
        print(f"[{self.worker_id}] Subscribed to 'tasks' topic")
        
        try:
            while self.is_running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    print(f"[{self.worker_id}] Consumer error: {msg.error()}")
                    continue
                
                # Process message
                try:
                    task = json.loads(msg.value().decode('utf-8'))
                    self.process_tile(task)
                except Exception as e:
                    print(f"[{self.worker_id}] Error processing task: {e}")
                    
        except KeyboardInterrupt:
            print(f"\n[{self.worker_id}] Shutting down...")
        finally:
            self.is_running = False
            self.consumer.close()
            print(f"[{self.worker_id}] Worker stopped. Total processed: {self.processed_count}")

if __name__ == '__main__':
    # Get Kafka broker from command line or use default
    kafka_broker = "172.27.173.202:9092"
    worker_id = "worker-2"

    worker = WorkerNode(kafka_broker, worker_id)
    worker.start()
