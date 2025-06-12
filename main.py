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

        print("📦 File metadata:", file_data)

        message_json = json.dumps(file_data)
        message_bytes = message_json.encode('utf-8')

        publisher.publish(TOPIC_NAME, message_bytes)  # 🚫 No .result() or blocking

        print("✅ Message published (non-blocking).")

    except Exception as e:
        print(f'❌ ERROR publishing message: {e}')

