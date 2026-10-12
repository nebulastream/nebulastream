import json
# U15.	Jaccard: Processes two json lists with tokens and calculated the jaccard distance

def jaccard(self,arg1:str,arg2:str)->float:

    if arg1 is not None and arg2 is not None:
        try:
            r=json.loads(arg1)
            s=json.loads(arg2)
            rset=set([tuple(x) if type(x)==list else x for x in r])
            sset=set([tuple(x) if type(x)==list else x for x in s])
            return float(len( rset & sset ))/(len( rset | sset ))
        except:
            return None
    else:
        return None