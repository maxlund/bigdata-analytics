import os, sys, csv

files = os.listdir(sys.argv[1])
data = list()
for fn in files:
    with open(sys.argv[1]+'/'+fn, 'rb') as input_file:
        data.extend(input_file.readlines())

with open(sys.argv[2], 'wb') as csv_file:
    mycsv = csv.writer(csv_file, delimiter=',')
    for row in data:
        mycsv.writerow(row)
