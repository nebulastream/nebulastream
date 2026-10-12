

# U8.	Extractfunder: extracts funder from string with format funder::class::projectid
def extractfunder(self,project:str)->str:
    if project:
        try:
            if '::' in project:
                return project.split("::")[0]
            else:
                return None
        except:
            return None
    else:
        return None
