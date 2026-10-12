import json

# U23. Lowerize: Converts to lower case the input text 
def lowerize(self,val: str)->str:
    if val:
        try:
            return val.lower()
        except:
            return ''
    else:
        return None
