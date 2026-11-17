
import json
# U17.	Jsoncount: Returns the length of a json list
def jsoncount(self,jval: str) -> int:
    try:
        if jval[0]=='[':
            tot_json = json.loads(jval)
            return int(len(tot_json))
        else:
            return 1
    except:
        return None