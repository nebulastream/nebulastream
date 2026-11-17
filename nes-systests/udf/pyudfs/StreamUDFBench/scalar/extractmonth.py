

# U10. Extractmonth: Reads a date (as a string) and extracts an integer with the month

def extractmonth(self,arg: str) -> int:
    if arg:
        try:
            return int(arg[arg.find('-')+1:arg.rfind('-')])
        except:
            return -1
    else:
        return None
