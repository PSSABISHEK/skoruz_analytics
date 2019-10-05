import sys
import json
import string

# CREATE EXTERNAL TABLE IF NOT EXISTS (user_time STRING, alt STRING) STORED AS ORC

f = open(sys.argv[1], "r")
datastore = json.load(f)
values = []
table_name = "user101"
i = len(datastore.keys())
j = 0
for key in datastore.keys():
    values.append(key)
    values.append("STRING")
    if j < i-1:
        values.append(";")
    j += 1
    print(j)
values = ",".join(values)
values = values.replace(',', ' ')
values = values.replace(';', ',')
print('CREATE TABLE ' + table_name + ' IF NOT EXIST  (' + values + ') STORED AS ORC')
