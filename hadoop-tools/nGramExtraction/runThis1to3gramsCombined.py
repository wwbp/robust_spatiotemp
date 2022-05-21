import sys
import os
from os.path import basename, splitext

if len(sys.argv) != 5:
	print("./runThis1to3gramsCombined.py INPUT_FILE OUTPUT_DIR MESSAGE_FIELD GROUP_ID")
	exit()

for n in [1,2,3]:
	n = str(n)
	runfile = "./runThis.sh"
	input_files = str(sys.argv[1])
	name = splitext(basename(input_files))[0]
	output_path = str(sys.argv[2]) + "/feat." + str(n) + "gram." + name
	message_field = str(sys.argv[3])
	group_id_field = str(sys.argv[4])

	#files = "\"" + ",".join([line.rstrip('\n') for line in open(input_files)]) + "\""
	files = ",".join([line.rstrip('\n') for line in open(input_files)]) 


	command = " ".join([runfile, files, output_path, message_field, group_id_field, n])
	print(command)
	os.system(command) 

