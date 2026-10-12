

def extractid(self,project:str)->str:
    if project:
        try:
            return project.split("::")[2]
        except:
            return None
    else:
        return None