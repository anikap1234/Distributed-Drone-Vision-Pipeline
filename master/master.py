import cv2
import numpy as np
import json
import base64
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import sqlite3
from datetime import datetime
import time
import threading
import pickle
import os

class MasterNode:
    def __init__(self, kafka_broker, num_partitions=8):
        self.kafka_broker = kafka_broker
        self.num_partitions = num_partitions
        
        self.producer_config = {
            'bootstrap.servers': kafka_broker,
            'client.id': 'master-producer',
            'acks': 'all',
            'retries': 3
        }
        self.consumer_config = {
            'bootstrap.servers': kafka_broker,
            'group.id': 'master-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        self.producer = Producer(self.producer_config)
        self.tile_size = 512
        
        # Create directory for job metadata
        os.makedirs('job_metadata', exist_ok=True)
        
        # Initialize Kafka topics with multiple partitions
        self.init_kafka_topics()
        
    def init_kafka_topics(self):
        """Create Kafka topics with multiple partitions for load balancing"""
        admin_client = AdminClient({'bootstrap.servers': self.kafka_broker})
        
        topics = [
            NewTopic('tasks', num_partitions=self.num_partitions, replication_factor=1),
            NewTopic('results', num_partitions=self.num_partitions, replication_factor=1),
            NewTopic('heartbeats', num_partitions=1, replication_factor=1)
        ]
        
        # Create topics (ignore if they already exist)
        fs = admin_client.create_topics(topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic {topic} created with {self.num_partitions} partitions")
            except Exception as e:
                if 'TOPIC_ALREADY_EXISTS' in str(e):
                    print(f"Topic {topic} already exists")
                else:
                    print(f"Failed to create topic {topic}: {e}")
    
    def save_job_metadata(self, job_id, metadata):
        """Save job metadata to disk"""
        filepath = f'job_metadata/{job_id}.pkl'
        with open(filepath, 'wb') as f:
            pickle.dump(metadata, f)
    
    def load_job_metadata(self, job_id):
        """Load job metadata from disk"""
        filepath = f'job_metadata/{job_id}.pkl'
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                return pickle.load(f)
        return None
    
    def delete_job_metadata(self, job_id):
        """Delete job metadata file"""
        filepath = f'job_metadata/{job_id}.pkl'
        if os.path.exists(filepath):
            os.remove(filepath)
        
    def split_image(self, image_path):
        """Split image into tiles of 512x512"""
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError("Failed to load image")
        
        height, width = img.shape[:2]
        
        # Ensure minimum size
        if height < 1024 or width < 1024:
            raise ValueError(f"Image must be at least 1024x1024. Got {width}x{height}")
        
        tiles = []
        tile_positions = []
        
        for y in range(0, height, self.tile_size):
            for x in range(0, width, self.tile_size):
                # Extract tile
                tile = img[y:y+self.tile_size, x:x+self.tile_size]
                
                # Pad if necessary (for edge tiles)
                if tile.shape[0] < self.tile_size or tile.shape[1] < self.tile_size:
                    padded = np.zeros((self.tile_size, self.tile_size, 3), dtype=np.uint8)
                    padded[:tile.shape[0], :tile.shape[1]] = tile
                    tile = padded
                
                tiles.append(tile)
                tile_positions.append((x, y, tile.shape[1], tile.shape[0]))
        
        return tiles, tile_positions, (width, height)
    
    def encode_tile(self, tile):
        """Encode tile as base64 string"""
        _, buffer = cv2.imencode('.png', tile)
        return base64.b64encode(buffer).decode('utf-8')
    
    def decode_tile(self, encoded_tile):
        """Decode base64 string to tile"""
        img_data = base64.b64decode(encoded_tile)
        nparr = np.frombuffer(img_data, np.uint8)
        return cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    
    def process_image(self, image_path, job_id, transformation='grayscale'):
        """Split image and publish tiles to Kafka with round-robin partitioning"""
        print(f"Processing image: {image_path} with job_id: {job_id}")
        
        tiles, positions, original_size = self.split_image(image_path)
        
        # Store job metadata to disk (persistent)
        job_metadata = {
            'positions': positions,
            'original_size': original_size,
            'total_tiles': len(tiles),
            'received_tiles': {},
            'transformation': transformation,
            'tile_assignments': {}
        }
        self.save_job_metadata(job_id, job_metadata)
        
        # Publish tiles to Kafka with different keys for load balancing
        for idx, tile in enumerate(tiles):
            encoded_tile = self.encode_tile(tile)
            
            message = {
                'job_id': job_id,
                'tile_id': idx,
                'tile_data': encoded_tile,
                'transformation': transformation,
                'position': positions[idx]
            }
            
            # CRITICAL: Use tile_id as key instead of job_id
            # This distributes tiles across different partitions
            tile_key = f"{job_id}:{idx}"
            
            self.producer.produce(
                'tasks',
                key=tile_key,
                value=json.dumps(message),
                callback=self.delivery_report
            )
        
        self.producer.flush()
        print(f"Published {len(tiles)} tiles for job {job_id} across {self.num_partitions} partitions")
        return len(tiles)
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [partition {msg.partition()}]')
    
    def reconstruct_image(self, job_id, job_meta):
        """Reconstruct image from processed tiles"""
        tiles = job_meta['received_tiles']
        positions = job_meta['positions']
        original_size = job_meta['original_size']
        
        # Create empty canvas
        result = np.zeros((original_size[1], original_size[0], 3), dtype=np.uint8)
        
        # Place tiles
        for tile_id, tile_data in tiles.items():
            tile = self.decode_tile(tile_data)
            x, y, w, h = positions[tile_id]
            
            # Handle edge tiles that might be smaller
            actual_h, actual_w = tile.shape[:2]
            result[y:y+actual_h, x:x+actual_w] = tile[:min(h, actual_h), :min(w, actual_w)]
        
        return result
    
    def consume_results(self):
        """Consume processed tiles from results topic"""
        consumer = Consumer(self.consumer_config)
        consumer.subscribe(['results'])
        
        print("Master node listening for results...")
        
        try:
            while True:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                result = json.loads(msg.value().decode('utf-8'))
                job_id = result['job_id']
                tile_id = result['tile_id']
                tile_data = result['tile_data']
                worker_id = result.get('worker_id', 'unknown')
                
                print(f"Received result for job {job_id}, tile {tile_id} from {worker_id}")
                
                # Load job metadata from disk
                job_meta = self.load_job_metadata(job_id)
                
                if job_meta:
                    # Store tile
                    job_meta['received_tiles'][tile_id] = tile_data
                    job_meta['tile_assignments'][tile_id] = worker_id
                    
                    # Save updated metadata
                    self.save_job_metadata(job_id, job_meta)
                    
                    # Update database
                    conn = sqlite3.connect('pipeline.db')
                    c = conn.cursor()
                    completed = len(job_meta['received_tiles'])
                    c.execute('UPDATE jobs SET completed_tiles = ? WHERE job_id = ?',
                             (completed, job_id))
                    conn.commit()
                    conn.close()
                    
                    print(f"Progress: {completed}/{job_meta['total_tiles']} tiles")
                    
                    # Check if all tiles received
                    if completed == job_meta['total_tiles']:
                        print(f"\n{'='*60}")
                        print(f"All tiles received for job {job_id}!")
                        print(f"{'='*60}")
                        
                        # Log load distribution
                        self.log_load_distribution(job_id, job_meta)
                        
                        print("Reconstructing image...")
                        reconstructed = self.reconstruct_image(job_id, job_meta)
                        
                        if reconstructed is not None:
                            output_path = f'results/{job_id}_result.png'
                            cv2.imwrite(output_path, reconstructed)
                            print(f"✓ Successfully saved result to {output_path}")
                            print(f"{'='*60}\n")
                            
                            # Update database
                            conn = sqlite3.connect('pipeline.db')
                            c = conn.cursor()
                            c.execute('''UPDATE jobs SET status = ?, completed_at = ? 
                                       WHERE job_id = ?''',
                                     ('completed', datetime.now(), job_id))
                            conn.commit()
                            conn.close()
                            
                            # Clean up metadata file
                            self.delete_job_metadata(job_id)
                        else:
                            print(f"ERROR: Failed to reconstruct image for job {job_id}")
                else:
                    print(f"WARNING: No metadata found for job {job_id}")
                
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
    
    def log_load_distribution(self, job_id, job_meta):
        """Log how tiles were distributed among workers"""
        assignments = job_meta.get('tile_assignments', {})
        
        if not assignments:
            print("No tile assignments recorded")
            return
        
        worker_loads = {}
        
        for tile_id, worker_id in assignments.items():
            worker_loads[worker_id] = worker_loads.get(worker_id, 0) + 1
        
        print(f"\n=== Load Distribution for Job {job_id} ===")
        for worker_id, count in sorted(worker_loads.items()):
            percentage = (count / len(assignments)) * 100
            bar = '█' * int(percentage / 2)  # Visual bar
            print(f"  {worker_id:20s}: {count:3d} tiles ({percentage:5.1f}%) {bar}")
        print(f"  Total workers used: {len(worker_loads)}")
        print("=" * 50 + "\n")
    
    def monitor_heartbeats(self):
        """Monitor worker heartbeats"""
        consumer_config = self.consumer_config.copy()
        consumer_config['group.id'] = 'master-heartbeat-monitor'
        consumer = Consumer(consumer_config)
        consumer.subscribe(['heartbeats'])
        
        print("Monitoring worker heartbeats...")
        
        try:
            while True:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f"Heartbeat consumer error: {msg.error()}")
                    continue
                
                heartbeat = json.loads(msg.value().decode('utf-8'))
                worker_id = heartbeat['worker_id']
                timestamp = heartbeat['timestamp']
                status = heartbeat['status']
                processed_count = heartbeat.get('processed_count', 0)
                
                # Update database
                conn = sqlite3.connect('pipeline.db')
                c = conn.cursor()
                c.execute('''INSERT OR REPLACE INTO workers (worker_id, last_heartbeat, status)
                           VALUES (?, ?, ?)''',
                         (worker_id, timestamp, f"{status} ({processed_count} tiles)"))
                conn.commit()
                conn.close()
                
                # Only log occasionally to reduce noise
                if processed_count % 5 == 0 or status != 'active':
                    print(f"Heartbeat from {worker_id}: {status}, processed {processed_count} tiles")
                
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
    
    def start(self):
        """Start master node consumers"""
        # Start result consumer in separate thread
        result_thread = threading.Thread(target=self.consume_results, daemon=True)
        result_thread.start()
        
        # Start heartbeat monitor in separate thread
        heartbeat_thread = threading.Thread(target=self.monitor_heartbeats, daemon=True)
        heartbeat_thread.start()
        
        # Keep main thread alive
        result_thread.join()
