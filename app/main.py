# to run fastapi app (from Kafka directory (not from app directory)), run this command:
# uvicorn app.main:app --reload

from fastapi import FastAPI
import os

from app.producer import produce_event # app.producer because working directory (while running program) is kafka, so  inside kafka, app, inside that, is producer.py

app = FastAPI()

@app.get("/")
def root():
    return {"greeting" : "Welcome to Kafka's dashboard", "info" : "Go to /docs for better experience"}

@app.post("/produce")
def produce(user_id : int, topic : str, event : str):
    produce_event(user_id, topic, event)
    return {"message" : f"Event added under topic {topic}"}
    # os.makedirs("data/topics", exist_ok=True)