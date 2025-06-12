import base64
import json
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
TOPIC_NAME = 'projects/integral-hold-462207-u2/topics/file-meta-topic'

def get_file_metadata(event, context):
    try:
        file_data = {
            'name': event.get('name'),
            'bucket': event.get('bucket'),
            'contentType': event.get('contentType', 'Unknown'),
            'size': event.get('size', 'Unknown')
        }

        print("File metadata:", file_data)  # 👈 Add logging here

        message_json = json.dumps(file_data)
        message_bytes = message_json.encode('utf-8')

        publish_future = publisher.publish(TOPIC_NAME, message_bytes)
        print(f'Published message ID: {publish_future.result()}')  # 👈 Log this too

    except Exception as e:
        print(f'❌ ERROR: {e}')
