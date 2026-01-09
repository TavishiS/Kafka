import os
from storage import append_event

def produce_event(topic : str, event : str):
    append_event(topic, event)