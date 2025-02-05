
class CPV:
        pass

#  print all the variables insidea object with its value
for key, value in CPV.__dict__.items():
    if not key.startswith('__'):
        print(f"{key}: {value}")


