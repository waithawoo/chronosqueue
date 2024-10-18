import time
import json
import asyncio
from collections import deque

import zmq
import zmq.asyncio

from task import TaskDatabase, Task

TASK_ADD = b'\x01'
TASK_PROCESS = b'\x02'
TASK_ROUTE = b'\x03'
HEALTHCHECK = b'\x04'

class TaskRouter:
    """
    TaskRouter
        PayloadFormat => [Client ID(auto), Empty Delimiter b'', Payload Header, Payload Type, Payload]
    """
    def __init__(self, host='127.0.0.1', port=5555):
        self.db_name = 'tasks.db'
        self.task_db = TaskDatabase(self.db_name)
        self.context = zmq.asyncio.Context()
        self.router_socket = self.context.socket(zmq.ROUTER)
        self.router_socket.bind(f"tcp://{host}:{port}")
        # self.task_db = deque()  
        self.workers = []

    def register_worker(self, worker):
        self.workers.append(worker)
        print(f"Worker {worker.__name__} registered.")
        
    async def route_task(self, task_type, payload):
        pass
        # self.task_db.add_task(task_type, payload)
        # if self.task_db:
            # task = self.task_db.popleft()
            # await self.send_task_to_worker(task)

    async def send_task_to_worker(self, task):
        for worker in self.workers:
            if worker.can_process(task):
                print(f"Sending task to worker: {worker.__name__} with task: {task}")
                await worker.process(task)
                return
        print("No suitable worker found for the task.")

    def get_tasks_for_processing(self):
        # pending_tasks = self.task_db.get_pending_tasks()
        pending_tasks = self.task_db
        return pending_tasks

    async def health_check(self):
        while True:
            for worker in self.workers:
                if not worker.is_alive():
                    print(f"Worker {worker.__name__} is not alive. Removing from registry.")
                    self.workers.remove(worker)
            await asyncio.sleep(5)

    async def run(self):
        try:
            print('Router is running.')
            while True:
                message_parts = await self.router_socket.recv_multipart()
                print('run() - recv_parts : ', message_parts)
            
                client_id, _, payload_header, payload_type, payload_data = message_parts
            
                print('client_id ', client_id)
                print('payload_header ', payload_header)
                print('payload_type ', payload_type)
                print('payload_data ', payload_data)

                if payload_header == TASK_ADD:
                    task = payload_data.decode('utf-8')
                    if payload_type == b'json':
                        task = json.loads(task)
                    
                    task = Task(task)
                    print('task ', task)
                    print('task.task_type ', task.task_type)
                    print('task.payload ', task.task_payload)
                    print('task.status ', task.status)
                    
                    # self.task_db.append(task)
                    self.task_db.add_task(task.task_type, json.dumps(task.task_payload))
            
                    print(f"Added task: {self.task_db}")
                # _, worker_id, _ = await self.router_socket.recv_multipart()
                # tasks = self.get_tasks_for_processing()
                # print('run() - tasks : ', tasks)
                # for task in tasks:
                    # self.router_socket.send_multipart([worker_id, f"{task.task_id}".encode(), task.task_type.encode(), task.payload.encode()])
                    # self.task_db.update_task_status(task.task_id, 'in_progress')
                time.sleep(1) 
        except Exception as e:
            print('Router Error ', e)

if __name__ == "__main__":
    router = TaskRouter(port=5555)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(router.run(), router.health_check()))
