

# U1. Add_noise : adds gaussian noise to a value and returns a float 
def addnoise(self, val:int)->float:
    import random

    def add_noise(val, mean=0, std_dev=2):
        if val:       
            noise = random.gauss(mean, std_dev)
            result = float(val) + noise
            return result
        else:
            return None
    try: 
        return add_noise(val)
    except:
        return None