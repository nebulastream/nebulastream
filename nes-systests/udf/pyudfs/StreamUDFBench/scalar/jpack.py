import json

# U16.	Jpack: Converts a string to a json list with tokens

def jpack(self,input:str)->str:
    if input:
        try:
            string_split = input.split()
            return json.dumps([word for word in string_split])
        except:
            return ''
    else:
        return None
