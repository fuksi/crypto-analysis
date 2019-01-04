import os
import json

# Todo: modify this to use env variables remotely
DB_SETTINGS = {}

local_settings_path = './local_settings.json'
if os.path.isfile(local_settings_path):
    with open(local_settings_path) as f:
        settings = json.load(f)
        DB_SETTINGS = settings['DB']
