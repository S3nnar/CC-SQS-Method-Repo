import boto3
import base64
from io import BytesIO
from PIL import Image
import json
import os
import logging
from flask import Flask, jsonify
import watchtower
import paho.mqtt.client as mqtt
from threading import Thread
import time

app = Flask(__name__)

account_id = os.getenv('ACCOUNT_ID')
queue_name = os.getenv('QUEUE_NAME')
region = os.getenv('REGION')
mqtt_endpoint = os.getenv('IOT_ENDPOINT')

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

sqs_client = boto3.client('sqs', region_name=region)
sns_client = boto3.client('sns', region_name=region)

queue_url = f'https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}'

# Setup CloudWatch logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
cloudwatch_client = boto3.client('logs', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region)
handler = watchtower.CloudWatchLogHandler(log_group='AppRunnerLogs', boto3_client=cloudwatch_client)
logger.addHandler(handler)
logger.info("Python script started")

# Define the MQTT client
client = mqtt.Client()

def capture_frame(base64_string):
    try:
        image_data = base64.b64decode(base64_string.split(',')[1])
        image = Image.open(BytesIO(image_data))
        return image
    except Exception as e:
        logger.error(f"Error converting base64 string to image: {e}")
        return None

def process_message(message):
    try:
        message_body = json.loads(message)
        topic_arn = message_body.get('Topic')
        frame_base64 = message_body.get('frame')

        if not topic_arn:
            logger.error("Topic ARN is missing in the message.")
            return

        if not frame_base64:
            logger.info("Frame is missing in the message.")
            return

        image = capture_frame(frame_base64)

        if image:
            status_message = "Image conversion successful"
            logger.info(status_message)
        else:
            status_message = "Image conversion failed"
            logger.info(status_message)

        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=status_message,
            Subject='Image Processing Result'
        )
        logger.info(f"SNS publish response: {response}")

    except Exception as e:
        logger.error(f"Error processing message: {e}")

def receive_messages():
    logger.info("Started receiving messages!")
    while True:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )

        if 'Messages' in response:
            for message in response['Messages']:
                process_message(message['Body'])
                sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
        else:
            logger.info("No messages to process. Waiting...")
            time.sleep(30)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT Broker")
        client.subscribe("masterTopic")
    else:
        logger.error("Failed to connect, return code %d\n", rc)

def on_message(client, userdata, msg):
    logger.info(f"Received message: {msg.payload.decode()} from topic: {msg.topic}")
    process_message(msg.payload.decode())

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    logger.info("Start!")
    thread = Thread(target=receive_messages)
    thread.start()

    client.on_connect = on_connect
    client.on_message = on_message
    client.tls_set(ca_certs="certificates/AmazonRootCA1.pem", certfile="certificates/f20bdd251ad976251adec198e4af213e5ae49e897dc970a211f71f024205695e-certificate.pem.crt", keyfile="certificates/f20bdd251ad976251adec198e4af213e5ae49e897dc970a211f71f024205695e-private.pem.key")
    client.username_pw_set(aws_access_key_id, aws_secret_access_key)
    client.connect(mqtt_endpoint, 8883, 60)

    app.run(threaded=True, host='0.0.0.0', port=8080)
    client.loop_forever()
