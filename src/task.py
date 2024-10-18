import sqlite3
from datetime import datetime
import json

class Task:
    def __init__(self, task: dict = None):
        if task is None:
            raise ValueError("Task cannot be None")

        required_keys = ['task_id', 'task_type', 'task_payload']
        for key in required_keys:
            if key not in task:
                raise ValueError(f"Missing required key: {key}")
            
        self.task_id = task['task_id']
        self.task_type = task['task_type']
        self.task_payload = task['task_payload']
        self.status = task.get('status', 'pending')
        self.created_at = datetime.now()
    
    def to_dict(self):
        return {
            'task_id': self.task_id,
            'task_type': self.task_type,
            'task_payload': self.task_payload,
            'status': self.status,
            'created_at': self.created_at.isoformat()
        }
        
    def to_bytes(self):
        return json.dumps(self.to_dict()).encode('utf-8')
        
    def __repr__(self):
        return f"<Task(task_id={self.task_id}, task_type={self.task_type}, status={self.status})>"

class TaskDatabase:
    def __init__(self, db_name='tasks.db'):
        self.db_name = db_name
        self.create_table()

    def create_table(self):
        print('CREATING Tables test')
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT, -- Unique identifier for the task
                task_type TEXT NOT NULL,              -- Type of the task or operation to perform
                payload TEXT NOT NULL,                -- JSON string or serialized data representing the task's input
                status TEXT DEFAULT 'pending',        -- Current status of the task (e.g., pending, in-progress, completed, failed)
                priority INTEGER DEFAULT 3,           -- Priority level (1-5, where 1 is the highest priority)
                attempts INTEGER DEFAULT 0,           -- Number of attempts made to process the task
                max_attempts INTEGER DEFAULT 5,       -- Maximum number of attempts allowed for this task
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp for when the task was created
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp for the last update to the task
                result TEXT,                           -- Result of the task after processing (optional, can be JSON)
                error_message TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def add_task(self, task_type, payload):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        print('dd')
        cursor.execute('INSERT INTO task_queues (task_type, payload, status) VALUES (?, ?, ?)',
                       (task_type, payload, 'pending'))
        conn.commit()
        conn.close()

    def get_pending_tasks(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM task_queues WHERE status = ?', ('pending',))
        tasks = cursor.fetchall()
        conn.close()
        pending_tasks = []
        # for task_row in tasks:
        #     task_dict = {
        #         'task_id': task_row[0],
        #         'task_type': task_row[1],
        #         'task_payload': json.loads(task_row[2]),
        #         'status': task_row[3],
        #         'created_at': task_row[7]
        #     }
        #     pending_tasks.append(Task(task_dict))

    def update_task_status(self, task_id, status):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('UPDATE task_queues SET status = ? WHERE id = ?', (status, task_id))
        conn.commit()
        conn.close()
