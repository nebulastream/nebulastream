import json


# U19.	Jsort: processes a json list and returns a sorted json list 
def jsort(self,jval:str)->str:
    try:
        return json.dumps(sorted(json.loads(jval)))
    except:
        return "[]"
