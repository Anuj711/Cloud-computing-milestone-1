import json
import os
import glob
from google.cloud import pubsub_v1

# Setup credentials
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Configuration
project_id = "cloud-milestone-1-pub-sub"
subscription_id = "csvTest-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # Deserialize the message into a dictionary
    data_dict = json.loads(message.data.decode("utf-8"))

    # Print the values of the dictionary
    print(f"Received Record: {data_dict}")

    # Acknowledge the message
    message.ack()

if __name__ == "__main__":
    print(f"Listening for messages on {subscription_path}...\n")

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        # Keep the main thread alive to listen for messages
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

        print("\nConsumer stopped.")
