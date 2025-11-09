from flask import Flask, render_template, request, jsonify, send_file
import os
import json
import uuid
from datetime import datetime, timedelta
import sqlite3
from master import MasterNode

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs('results', exist_ok=True)

# Initialize database
def init_db():
    conn = sqlite3.connect('pipeline.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS jobs
                 (job_id TEXT PRIMARY KEY, 
                  filename TEXT, 
                  status TEXT, 
                  total_tiles INTEGER,
                  completed_tiles INTEGER,
                  transformation TEXT,
                  created_at TIMESTAMP,
                  completed_at TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS workers
                 (worker_id TEXT PRIMARY KEY,
                  last_heartbeat TIMESTAMP,
                  status TEXT)''')
    conn.commit()
    conn.close()

init_db()

KAFKA_BROKER = '172.27.173.202:9092'
NUM_PARTITIONS = 8  # Number of partitions for parallel processing

master_node = MasterNode(KAFKA_BROKER, num_partitions=NUM_PARTITIONS)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_image():
    if 'image' not in request.files:
        return jsonify({'error': 'No image file provided'}), 400
    
    file = request.files['image']
    transformation = request.form.get('transformation', 'grayscale')
    
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if file:
        job_id = str(uuid.uuid4())
        filename = f"{job_id}_{file.filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Store job in database
        conn = sqlite3.connect('pipeline.db')
        c = conn.cursor()
        c.execute('''INSERT INTO jobs (job_id, filename, status, total_tiles, 
                     completed_tiles, transformation, created_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                  (job_id, file.filename, 'processing', 0, 0, transformation, datetime.now()))
        conn.commit()
        conn.close()
        
        # Process image
        try:
            total_tiles = master_node.process_image(filepath, job_id, transformation)
            
            # Update total tiles
            conn = sqlite3.connect('pipeline.db')
            c = conn.cursor()
            c.execute('UPDATE jobs SET total_tiles = ? WHERE job_id = ?', (total_tiles, job_id))
            conn.commit()
            conn.close()
            
            return jsonify({
                'job_id': job_id,
                'message': 'Image processing started',
                'total_tiles': total_tiles,
                'partitions': NUM_PARTITIONS
            }), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 500

@app.route('/status/<job_id>')
def get_status(job_id):
    conn = sqlite3.connect('pipeline.db')
    c = conn.cursor()
    c.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,))
    job = c.fetchone()
    conn.close()
    
    if job:
        # Get load distribution from disk-based job metadata
        load_distribution = {}
        try:
            job_meta = master_node.load_job_metadata(job_id)
            if job_meta and 'tile_assignments' in job_meta:
                assignments = job_meta['tile_assignments']
                for tile_id, worker_id in assignments.items():
                    load_distribution[worker_id] = load_distribution.get(worker_id, 0) + 1
        except Exception as e:
            # If metadata file doesn't exist or can't be loaded, just continue without load distribution
            pass
        
        return jsonify({
            'job_id': job[0],
            'filename': job[1],
            'status': job[2],
            'total_tiles': job[3],
            'completed_tiles': job[4],
            'transformation': job[5],
            'progress': (job[4] / job[3] * 100) if job[3] > 0 else 0,
            'load_distribution': load_distribution
        })
    return jsonify({'error': 'Job not found'}), 404

@app.route('/result/<job_id>')
def get_result(job_id):
    result_path = f'results/{job_id}_result.png'
    if os.path.exists(result_path):
        return send_file(result_path, mimetype='image/png')
    return jsonify({'error': 'Result not available yet'}), 404

@app.route('/workers')
def get_workers():
    conn = sqlite3.connect('pipeline.db')
    c = conn.cursor()
    c.execute('SELECT worker_id, last_heartbeat, status FROM workers')
    workers = c.fetchall()
    conn.close()
    
    worker_list = []
    current_time = datetime.now()
    
    for w in workers:
        # Parse last heartbeat with better error handling
        try:
            # Handle both ISO format with and without microseconds
            timestamp_str = w[1]
            if '.' in timestamp_str:
                # Has microseconds
                last_hb = datetime.fromisoformat(timestamp_str)
            else:
                # No microseconds
                last_hb = datetime.fromisoformat(timestamp_str)
            
            seconds_ago = (current_time - last_hb).total_seconds()
            is_active = seconds_ago < 30  # Consider active if heartbeat within 30 seconds
        except Exception as e:
            print(f"Error parsing timestamp for {w[0]}: {w[1]} - {e}")
            is_active = False
            seconds_ago = None
        
        worker_list.append({
            'worker_id': w[0],
            'last_heartbeat': w[1],
            'status': w[2],
            'is_active': is_active,
            'seconds_since_heartbeat': seconds_ago
        })
    
    # Count active workers
    active_count = sum(1 for w in worker_list if w['is_active'])
    
    return jsonify({
        'workers': worker_list, 
        'total': len(worker_list),
        'active': active_count,
        'partitions': NUM_PARTITIONS
    })

@app.route('/jobs')
def get_jobs():
    conn = sqlite3.connect('pipeline.db')
    c = conn.cursor()
    c.execute('SELECT * FROM jobs ORDER BY created_at DESC LIMIT 10')
    jobs = c.fetchall()
    conn.close()
    
    job_list = []
    for j in jobs:
        # Calculate processing time if completed
        processing_time = None
        if j[7]:  # completed_at exists
            try:
                created = datetime.fromisoformat(j[6])
                completed = datetime.fromisoformat(j[7])
                processing_time = (completed - created).total_seconds()
            except:
                pass
        
        job_list.append({
            'job_id': j[0],
            'filename': j[1],
            'status': j[2],
            'total_tiles': j[3],
            'completed_tiles': j[4],
            'transformation': j[5],
            'created_at': j[6],
            'processing_time': processing_time
        })
    
    return jsonify({'jobs': job_list})

@app.route('/stats')
def get_stats():
    """Get overall system statistics"""
    conn = sqlite3.connect('pipeline.db')
    c = conn.cursor()
    
    # Total jobs
    c.execute('SELECT COUNT(*) FROM jobs')
    total_jobs = c.fetchone()[0]
    
    # Completed jobs
    c.execute('SELECT COUNT(*) FROM jobs WHERE status = "completed"')
    completed_jobs = c.fetchone()[0]
    
    # Processing jobs
    c.execute('SELECT COUNT(*) FROM jobs WHERE status = "processing"')
    processing_jobs = c.fetchone()[0]
    
    # Total tiles processed
    c.execute('SELECT SUM(completed_tiles) FROM jobs')
    total_tiles = c.fetchone()[0] or 0
    
    # Average processing time
    c.execute('''SELECT AVG(
                    CAST((julianday(completed_at) - julianday(created_at)) * 86400 AS INTEGER)
                 ) FROM jobs WHERE status = "completed" AND completed_at IS NOT NULL''')
    avg_time = c.fetchone()[0]
    
    conn.close()
    
    return jsonify({
        'total_jobs': total_jobs,
        'completed_jobs': completed_jobs,
        'processing_jobs': processing_jobs,
        'total_tiles_processed': total_tiles,
        'average_processing_time': avg_time,
        'kafka_partitions': NUM_PARTITIONS
    })

if __name__ == '__main__':
    # Start master node in background
    import threading
    master_thread = threading.Thread(target=master_node.start, daemon=True)
    master_thread.start()
    
    # IMPORTANT: debug=False to prevent app restarts that clear job state
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
