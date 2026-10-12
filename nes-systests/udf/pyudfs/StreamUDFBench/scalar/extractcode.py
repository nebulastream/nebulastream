
# U6.	Extractcode: Processes a structured string containing the funder’s id, the funding class and the project id, and extracts the project id

def extractcode(self,project: str)->str:
    if project:
        try:
            return project.split("::")[2]
        except:
            return None
    else:
        return None

