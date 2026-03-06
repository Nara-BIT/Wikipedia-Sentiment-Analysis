import json
import requests
import pprint
from sseclient import SSEClient
from confluent_kafka import Producer

# 1. Setup Kafka Producer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'wiki-producer'
}
producer = Producer(conf)
KAFKA_TOPIC = 'wiki_edits'

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        pass # Silently succeed to keep the terminal clean

#print("🌍 Connecting to Wikipedia Live Stream...")

# 2. Connect to Wikimedia's EventStreams (No API Key needed!)
url = 'https://stream.wikimedia.org/v2/stream/recentchange'

# We MUST tell Wikipedia who we are, or they will instantly close the connection
headers = {
    'Accept': 'text/event-stream',
    'User-Agent': 'LocalDataEngineeringPipeline/1.0 (learning_project)'
}

print("🌍 Connecting to Wikipedia Live Stream...")
response = requests.get(url, stream=True, headers=headers)

# This will force Python to crash and show us the exact error if Wikipedia rejects us
response.raise_for_status() 

client = SSEClient(response)
print("✅ Connected! Listening for human edits...\n")

# 3. Process the live stream
try:
    for event in client.events():
        if event.event == 'message':
            try:
                change = json.loads(event.data)
                
                # We only want actual human edits that have a text comment
                if not change.get('bot') and change.get('type') == 'edit' and change.get('comment'):
                    
                    # Extract the useful data
                    payload = {
                        'user': change.get('user'),
                        'title': change.get('title'),
                        'comment': change.get('comment'),
                        'wiki': change.get('wiki')
                    }
                    
                    # Push to Kafka
                    producer.produce(
                        KAFKA_TOPIC, 
                        key=payload['wiki'], 
                        value=json.dumps(payload).encode('utf-8'), 
                        callback=delivery_report
                    )
                    producer.poll(0)
                    
                    # Print to terminal so we can watch it work
                    print(f"📝 {payload['user']} edited '{payload['title']}'")
                    print(f"   Comment: {payload['comment']}")
                    print("-" * 50)
                    
            except json.JSONDecodeError:
                continue
except KeyboardInterrupt:
    print("\n🛑 Stopping Producer...")
finally:
    producer.flush()
    print("Flushed all messages to Kafka.")