import boto3
import base64
from io import BytesIO
from PIL import Image
import json
import os
from flask import Flask, jsonify
from awscrt import mqtt
from awsiot import mqtt_connection_builder
from threading import Thread, Event
import time

os.environ['PYTHONUNBUFFERED'] = '1'

app = Flask(__name__)

account_id = os.getenv('ACCOUNT_ID')
queue_name = os.getenv('QUEUE_NAME')
region = os.getenv('REGION')
mqtt_endpoint = os.getenv('IOT_ENDPOINT')

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

sqs_client = boto3.client('sqs', region_name=region)

queue_url = f'https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}'

print("Python script started")

received_all_event = Event()
received_count = 0

def test_sqs_connection():
    try:
        response = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['All']
        )
        print("Successfully connected to SQS. Queue Attributes:", response['Attributes'])
    except Exception as e:
        print(f"Failed to connect to SQS: {e}")

def capture_frame(base64_string):
    try:
        image_data = base64.b64decode(base64_string.split(',')[1])
        image = Image.open(BytesIO(image_data))
        return image
    except Exception as e:
        print(f"Error converting base64 string to image: {e}")
        return None

def process_message(message):
    global received_count
    try:
        message_body = json.loads(message)
        topic_arn = message_body.get('Topic')
        frame_base64 = message_body.get('frame')

        if not topic_arn:
            print("Topic ARN is missing in the message.")
            return

        if not frame_base64:
            print("Frame is missing in the message.")
            return

        image = capture_frame(frame_base64)

        if image:
            status_message = "Image conversion successful"
            print(status_message)
        else:
            status_message = "Image conversion failed"
            print(status_message)
        received_count += 1

    except Exception as e:
        print(f"Error processing message: {e}")

def receive_messages():
    print("Started receiving messages!")
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            
            print("Received response from SQS:", response)

            if 'Messages' in response:
                print("Received message via SQS!")
                for message in response['Messages']:
                    print("Processing message:", message)
                    process_message(message['Body'])
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print("Deleted message from SQS")
            else:
                print("No messages to process. Waiting...")
                time.sleep(30)
        except Exception as e:
            print(f"Error receiving messages: {e}")

def on_connection_interrupted(connection, error, **kwargs):
    print(f"Connection interrupted. Error: {error}")

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print(f"Connection resumed. Return code: {return_code}, session present: {session_present}")

def on_message_received(topic, payload, **kwargs):
    print(f"Received message from topic '{topic}': {payload}")
    process_message(payload.decode())

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    print("Start!")
    test_sqs_connection()
    thread = Thread(target=receive_messages)
    thread.start()

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=mqtt_endpoint,
        port=8883,
        cert_filepath="certificates/f20bdd251ad976251adec198e4af213e5ae49e897dc970a211f71f024205695e-certificate.pem.crt",
        pri_key_filepath="certificates/f20bdd251ad976251adec198e4af213e5ae49e897dc970a211f71f024205695e-private.pem.key",
        ca_filepath="certificates/AmazonRootCA1.pem",
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id="myClientId",
        clean_session=False,
        keep_alive_secs=30,
    )

    connect_future = mqtt_connection.connect()
    connect_future.result()
    print("Connected!")

    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic="masterTopic",
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received
    )

    subscribe_result = subscribe_future.result()
    print(f"Subscribed with {str(subscribe_result['qos'])}")
    print(f"Subscribed with {str(subscribe_result['topic'])}")

    app.run(threaded=True, host='0.0.0.0', port=8080)

    received_all_event.wait()
    mqtt_connection.disconnect().result()
    print("Disconnected!")
