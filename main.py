import base64
import json
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
TOPIC_NAME = 'projects/YOUR_PROJECT_ID/topics/file-meta-topic'  # Replace YOUR_PROJECT_ID

def get_file_metadata(event, context):
    file_data = {
        'name': event.get('name'),
        'bucket': event.get('bucket'),
        'contentType': event.get('contentType', 'Unknown'),
        'size': event.get('size', 'Unknown')
    }

    message_json = json.dumps(file_data)
    message_bytes = message_json.encode('utf-8')

    try:
        publish_future = publisher.publish(TOPIC_NAME, message_bytes)
        print(f'Published message ID: {publish_future.result()}')
    except Exception as e:
        print(f'Error publishing message: {e}')
