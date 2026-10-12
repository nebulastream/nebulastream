import json

# U18.	Jsonparse: Parses a json dict per time and returns a string with the value

def jsonparse(self,json_content: str,key1: str)->str:

    try:
        data = json.loads(json_content)
        if isinstance(data, list):
            for item in data:
                return item.get(key1)
        elif isinstance(data, dict):
            return data.get(key1)
        else:
            return None
    except:
        return None