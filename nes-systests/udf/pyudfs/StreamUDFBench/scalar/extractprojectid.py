import re
# U11.	Extractprojectid: Processes a text snippet and extracts a 6 digit project identifier 

def extractprojectid(self,input: str)->str:
    if input:
        try:
            return re.findall(r"(?<!\d)[0-9]{6}(?!\d)",input)[0]
        except: return ''
    else:
        return None
