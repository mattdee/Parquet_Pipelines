#!/usr/bin/python

import parquet as pq
import sys
from io import BytesIO


# Purpose: Read parquet files fro stdin to pipeline in MemSQL

# Read binary input from stdin
# Python 2
#file = sys.stdin.read()
# Python 3
file = sys.stdin.buffer.read()

# Convert file object
data = BytesIO(file)


# Read the rows in the parquet file
# Print out CSV to MemSQL
for row in pq.reader(data):
	memsql_row =(str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3]))
	print (memsql_row.encode('ascii'))










