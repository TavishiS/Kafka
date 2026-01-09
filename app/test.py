import os

s = "this is tavishi"
encoded = s.encode()

with open("test_file.txt", "wb") as f:
    f.write(encoded) # encoded string is written in file (but we can see it as normal, human-readable string)

with open("test_file.txt", "rb") as f:
    lines = f.readlines() # but while reading, encoded is only read (it is not treated as normal text)
    for line in lines:
        print(line)
        print(line.decode())