import json

# U20.	Jsortvalues: processes a json list where each value contains more than one tokens, sorts the tokens in each value 

def jsortvalues(self,jval:str)->str:
    def sortname(name):
        return " ".join(sorted(name.split(' ')))
    try:
        return json.dumps([sortname(name) for name in json.loads(jval)])
    except:
        return "[]"
