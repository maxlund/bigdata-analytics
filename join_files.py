import os, sys

files = os.listdir(sys.argv[1])
data = list()
for fn in files:
    with open(sys.argv[1]+'/'+fn, 'rb') as input_file:
        data.extend(input_file.readlines())
        
with open(sys.argv[2], 'wb') as output_file:
    for line in data:
        output_file.write(line)
