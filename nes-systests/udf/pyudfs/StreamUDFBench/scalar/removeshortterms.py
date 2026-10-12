
import json

# U24.	Removeshortterms:  processes a json list where each value contains more than one tokens and removes tokens with length less than 3 chars 

def removeshortterms(self,jval:str)->str:
    def removeshortwords(name):
        return " ".join([word for word in name.split(' ') if len(word) > 2])
    try:
        return json.dumps([removeshortwords(name) for name in json.loads(jval)])
    except:
        return "[]"