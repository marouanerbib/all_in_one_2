import os
import json

def load_config():
    with open("/home/horhoro/PycharmProjects/all_in_one_2/config/config.json", "r") as file:
        return json.load(file)
