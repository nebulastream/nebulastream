import json

# U2.	Clean: Performs a simple data cleaning task on the string tokens of a json list

def clean(self,val: str)->str:
    def removeshortwords(name):
        return " ".join([word for word in name.split(' ') if len(word) > 2])

    def sortname(name):
        return " ".join(sorted(name.split(' ')))

    def cleanpy(val):
        name_list = json.loads(val)
        name_list = [name.lower() for name in name_list]
        name_list = [removeshortwords(name) for name in name_list]
        name_list = [sortname(name) for name in name_list]
        return json.dumps(sorted(name_list))
    
    if val:
        try:
            return cleanpy(val)
        except:
            return "[]"
    else:
        return None