import json
import asyncio

import zmq
import zmq.asyncio

from payload import Payload
from task import Task

TASK_ADD = b'\x01'
TASK_PROCESS = b'\x02'
TASK_ROUTE = b'\x03'
HEALTHCHECK = b'\x04'

class TaskSender:
    """
    TaskSender Class
        PayloadFormat => [Client ID(auto), Empty Delimiter b'', Payload Header, Payload Type, Payload]
    """
    def __init__(self, port):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(f"tcp://localhost:{port}")

    async def send_task(self, payload):
        print(f"TaskSender sending task with payload_header: {payload.header}, payload_type: {payload.payload_type}")
        if payload.payload_type == 'json':
            task_payload = json.dumps(payload.data.to_dict()).encode('utf-8')
        else:
            task_payload = payload.data
        payload.data = task_payload
        await self.socket.send_multipart(payload.to_multipart())

async def main():
    try:
        task_sender = TaskSender(port=5555)

        td = {
            "task_id": 1,
            "task_type": "email",
            "task_payload": {
                "recipients": ["example@example.com"],
                "subject": "Test Email",
                "body": "This is a test email."
            }
        }
        task = Task(td)
        print('task in sender ', task)
        payload = Payload(header=TASK_ADD, payload_type=b'json', data=task.to_bytes())
        await task_sender.send_task(payload)
    except Exception as e:
        print('Error : ', e)

if __name__ == "__main__":
    asyncio.run(main())
