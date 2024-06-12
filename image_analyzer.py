import boto3
import base64
from io import BytesIO
from PIL import Image
import requests
import time
import json
import os
from flask import Flask, request, jsonify

app = Flask(__name__)

account_id = os.getenv('ACCOUNT_ID')
queue_name = os.getenv('QUEUE_NAME')
region = os.getenv('REGION')

sqs_client = boto3.client('sqs', region_name=region)
sns_client = boto3.client('sns', region_name=region)

queue_url = f'https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}'

def capture_frame(base64_string):
    # Decoding the base64 string to image
    try:
        image_data = base64.b64decode(base64_string.split(',')[1])
        image = Image.open(BytesIO(image_data))
        return image
    except Exception as e:
        print(f"Error converting base64 string to image: {e}")
        return None

def process_message(message):
    try:
        message_body = json.loads(message['Body'])
        topic_arn = message_body.get('Topic')

        if not topic_arn:
            print("Topic ARN is missing in the message.")
            return

        # Assuming the frame is stored in 'frame' key in message
        frame_base64 = message_body.get('frame')

        if not frame_base64:
            print("Frame is missing in the message.")
            return

        image = capture_frame(frame_base64)

        if image:
            # Perform any processing here
            status_message = "Image conversion successful"
            print(status_message)
        else:
            status_message = "Image conversion failed"
            print(status_message)

        # Send a message to SNS topic
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=status_message,
            Subject='Image Processing Result'
        )
        print(f"SNS publish response: {response}")

    except Exception as e:
        print(f"Error processing message: {e}")

def receive_messages():
    print("Started receiving messages!")
    while True:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )

        if 'Messages' in response:
            for message in response['Messages']:
                process_message(message)
                sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
        else:
            print("No messages to process. Waiting...")

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    print("Start!")
    from threading import Thread
    # Start the message receiving in a separate thread
    thread = Thread(target=receive_messages)
    thread.start()

    # Start the Flask app
    app.run(threaded = True, host='0.0.0.0', port=8080)
    print("Test")
