
# U12.  Extractyear : Reads a date (as a string) and extracts an integer with the year

def extractyear(self,arg: str) -> int:
    if arg:
        try:
            return int(arg[:arg.find('-')])
        except:
            return -1
    else:
        return None
