import os
import sys
import json
import time
import base64
from azure.storage.queue import QueueClient


if __name__ == '__main__':

    POLL_INTERVAL = os.environ.get('PollInterval', 30)
    VISIBILITY_TIMEOUT = os.environ.get('VisibilityTimeout', 30)
    MAX_DEQUEUE_COUNT = os.environ.get('MaxDequeueCount', 5)
    COMPLETION_TIME = os.environ.get('CompletionTime', 60)
    QUEUE_READS = os.environ.get('QueueReads', 1)
    LOG_PERIOD = 10

    try:
        connection_string = os.environ['AzureWebJobsStorage']
        queue_name = os.environ['AzureQueueName']
    except KeyError as e:
        print('Error: missing environment variable AzureWebJobsStorage or AzureQueueName')
        raise e

    queue = QueueClient.from_connection_string(conn_str=connection_string, queue_name=queue_name)
    try:
        queue.create_queue()
    except:
        pass

    print(f'Process started, reading {QUEUE_READS} messages...')
    for queue_read in range(QUEUE_READS):
        print(f'Reading message {queue_read} of {QUEUE_READS}')
        message = None
        while True:
            message = next(queue.receive_messages(visibility_timeout=VISIBILITY_TIMEOUT), None)
            if message:
                break
            print(f'No message yet, waiting {POLL_INTERVAL}s...')
            time.sleep(POLL_INTERVAL)

        try:
            content = json.loads(base64.b64decode(message.content))
        except:
            content = json.loads(message.content)

        if message.dequeue_count > MAX_DEQUEUE_COUNT:
            print('Message max dequeue count exceded, mouving it to poison queue...')
            poison_queue = QueueClient.from_connection_string(conn_str=connection_string, queue_name=queue_name+'-poison')
            try:
                poison_queue.create_queue()
            except:
                pass
            poison_queue.send_message(message.content)
            queue.delete_message(message)
            continue
        
        print(content)

        for i in range(int(COMPLETION_TIME/LOG_PERIOD)):
            time.sleep(LOG_PERIOD)
            print('Simulating some long running job...')
            message = queue.update_message(message, visibility_timeout=VISIBILITY_TIMEOUT)

        print('Message processed, deleting it')
        queue.delete_message(message)

    print('Process over, exiting')
