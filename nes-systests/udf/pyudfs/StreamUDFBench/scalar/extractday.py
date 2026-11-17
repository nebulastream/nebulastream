

# U7.	Extractday: Reads a date (as a string) and extracts an integer with the day 

def extractday(self,arg: str) -> int:
    if arg:
        try:
            return int(arg[arg.rfind('-')+1:])
        except:
            return -1
    else:
        return None
        