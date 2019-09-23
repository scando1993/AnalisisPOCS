from shapely.geometry import mapping, shape
import json

with open('export.json','r') as f:
    data = json.load(f)
I = shape(data)
I.is_valid