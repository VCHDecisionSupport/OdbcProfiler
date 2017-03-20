import json
# from PyGenericProfiler import *

json_path = 'full_profile_dict.json'
json_file = open(json_path, 'r')
json_text = json_file.read()
print(json_text)

meta_dict = json.loads(json_text)
print(meta_dict)
