from fastapi import FastAPI
from pymongo import MongoClient
from bson.json_util import dumps
from fastapi.middleware.cors import CORSMiddleware
from threading import Thread
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
import json
import os

# MongoDB Config
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://abiyyutsaqif:Rahasia000@testmqtt.a7af8po.mongodb.net/?retryWrites=true&w=majority&appName=testmqtt")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["testmqtt"]
collection = db["sensor_data"]

# FastAPI app
app = FastAPI()

# CORS middleware (opsional)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "MongoDB FastAPI is running."}

@app.get("/data")
def get_data():
    data = collection.find().sort("timestamp", -1).limit(20)
    return json.loads(dumps(data))


# -------------------
# MQTT Setup
# -------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT broker")
        client.subscribe("testtopic/1")
    else:
        print("❌ Failed to connect, return code:", rc)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        print(f"📥 Received on {msg.topic}: {payload}")
        data = json.loads(payload)
        data["timestamp"] = datetime.now(timezone.utc)
        collection.insert_one(data)
        print("💾 Data saved to MongoDB")
    except Exception as e:
        print("❌ Error:", e)

def mqtt_worker():
    mqtt_client = mqtt.Client(client_id="clientId-WabvDww3kV", transport="websockets")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.tls_set()
    mqtt_client.connect("mqtt-dashboard.com", 8884, 60)
    mqtt_client.loop_forever()

# Start MQTT in a separate thread
mqtt_thread = Thread(target=mqtt_worker)
mqtt_thread.daemon = True
mqtt_thread.start()
