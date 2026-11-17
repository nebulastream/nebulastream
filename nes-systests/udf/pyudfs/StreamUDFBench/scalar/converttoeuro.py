

# U4.	Converttoeuro: : Converts currency to euro, returns a float

def converttoeuro(self,x:float,y:str)->float:

    euro_equals ={
    'EUR':1.00,
    '':1.00,
    'NOK':11.59,
    'AUD':1.63,
    'CAD':1.44,
    '$':1.09,
    'USD':1.09,
    'GBP':0.85,
    'CHF':0.98,
    'ZAR':20.41,
    'SGD':1.47,
    'INR':89.61,

    }
    if x is not None and y is not None:
        try:
            return float(x)/euro_equals[y]
        except:
            return 0.0
    else:
        return None