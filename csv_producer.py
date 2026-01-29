import csv
import json
import os
import glob
from google.cloud import pubsub_v1

# Setup credentials
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Configuration
project_id = "cloud-milestone-1-pub-sub"
topic_name = "csvTest"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

def publish_csv_data(file_path):
    with open(file_path, mode='r') as csv_file:
        # Use DictReader to automatically turn rows into dictionaries
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            # Data Cleaning: Handle empty strings
            for key in row:
                if row[key] == "":
                    row[key] = None

            # Serialize dictionary to JSON bytes
            message_data = json.dumps(row).encode("utf-8")

            try:
                future = publisher.publish(topic_path, message_data)
                future.result() # Verify success
                print(f"Published: {row['profileName']} at {row['time']}")
            except Exception as e:
                print(f"Failed to publish: {e}")

if __name__ == "__main__":
    print(f"Starting producer for topic: {topic_name}...")
    publish_csv_data("Labels.csv")

    print("Finished publishing all records.")
