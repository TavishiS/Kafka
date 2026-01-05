# to run fastapi app (from Kafka directory (not from app directory)), run this command:
# uvicorn app.main:app --reload

from fastapi import FastAPI
import os

from app.producer import append_event # app.producer because working directory (while running program) is kafka, so  inside kafka, app, inside that, is producer.py

app = FastAPI()

@app.post("/produce")
def produce(topic : str, event : str):
    append_event(topic, event)
    return {"message" : f"Event added under topic {topic}"}
    # os.makedirs("data/topics", exist_ok=True)