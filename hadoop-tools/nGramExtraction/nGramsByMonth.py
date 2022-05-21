import subprocess
import os
import sys

base = "/localdata/twitter10pct_cnty/part*/tw*.%s-%s-*.csv"
years = ["2013"]
#years = ["2013", "2014", "2015"]
months = ["03"]
runfile = "./runThisWhiteList.sh"

# i = 0
# for year in years:
# 	for month in months:
# 		i += 1
# 		directory = base % (year, month)
# 		#print(directory)

# 		command = ['hadoop', 'fs', '-ls', directory]
# 		p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# 		text = p.stdout.read()
# 		retcode = p.wait()
# 		print command
# 		exit()
# 		if any(part in text for part in ["part1", "part2", "part3"]):
# 			continue

# 		files = []
# 		for line in text.split("\n"):
# 			print line
# 			try:
# 				f = line.split()[-1]
# 				#print(f)
# 				files.append(f)
# 			except:
# 				pass
# 		exit()
# 		input_file_list = ",".join(files)
# 		if not input_file_list: continue

# 		if "part4" in input_file_list and (year == "2011" and int(month) > 10):
# 			message_field = str(2)
# 			group_id_field = str(31)
# 		else:
# 			message_field = str(2)
# 			group_id_field = str(6)


# 		for n in [2]:
# 			n = str(n)

# 			output_path = "/localdata/twitter10pct_feat/feat.%sgram.%s-%s" % (str(n), year, month) 


# 			command = " ".join([runfile, input_file_list, output_path, message_field, group_id_field, n])
# 			print(command)
# 			#os.system(command) 

# remaining = ["2012-05", "2012-07", "2012-08", "2012-11", "2012-12", "2013-01", "2013-05", "2013-06", "2013-07", "2013-08"]

# i = 0
# for part in remaining:
# 	year, month = part.split("-")
# 	i += 1
# 	directory = base % (year, month)
# 	#print(directory)


# 	command = ['hadoop', 'fs', '-ls', directory]
# 	p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# 	text = p.stdout.read()
# 	retcode = p.wait()

# 	if any(part in text for part in ["part1", "part2", "part3"]):
# 		continue

# 	files = []
# 	for line in text.split("\n"):
# 		try:
# 			f = line.split()[-1]
# 			#print(f)
# 			files.append(f)
# 		except:
# 			pass

# 	input_file_list = ",".join(files)
# 	if not input_file_list: continue

# 	if "part4" in input_file_list and (year == "2011" and int(month) > 10):
# 		message_field = str(2)
# 		group_id_field = str(31)
# 	else:
# 		message_field = str(2)
# 		group_id_field = str(6)

# 	whitelist = "1gram.%s-%s.fof_001.txt" % (year, month)

# 	for n in [2]:
# 		n = str(n)

# 		num_of_files = len(files)/2


# 		first_list = ",".join(files[:num_of_files])
# 		output_path = "/localdata/twitter10pct_feat/feat.%sgram.%s-%s.part%s" % (str(n), year, month, "1") 
# 		first_command = " ".join([runfile, first_list, output_path, message_field, group_id_field, n, whitelist])
# 		print(first_command)

# 		second_list = ",".join(files[num_of_files:])
# 		output_path = "/localdata/twitter10pct_feat/feat.%sgram.%s-%s.part%s" % (str(n), year, month, "2") 
# 		second_command = " ".join([runfile, second_list, output_path, message_field, group_id_field, n, whitelist])

# 		print(second_command)
# 		#exit()
# 		#os.system(command) 

remaining = ["2011-03", "2011-04", "2011-08", "2011-09", "2012-04", "2012-05", "2012-06", "2012-07", "2012-08", "2012-09", "2012-11", "2012-12", "2013-01", "2013-02", "2013-04", "2013-05", "2013-06", "2013-07", "2013-08", "2013-09", "2013-10", "2013-11", "2013-12", "2014-01", "2014-02", "2014-03"]

i = 0
for part in remaining:
	year, month = part.split("-")
	i += 1
	directory = base % (year, month)
	#print(directory)


	command = ['hadoop', 'fs', '-ls', directory]
	p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	text = p.stdout.read()
	retcode = p.wait()

	# if any(part in text for part in ["part1", "part2", "part3"]):
	# 	continue

	files = []
	for line in text.split("\n"):
		try:
			f = line.split()[-1]
			#print(f)
			files.append(f)
		except:
			pass

	input_file_list = ",".join(files)
	if not input_file_list: continue

	if "part4" in input_file_list and (year == "2011" and int(month) > 10):
		message_field = str(2)
		group_id_field = str(31)
	elif "part1" in input_file_list or "part2" in input_file_list or "part3" in input_file_list:
		message_field = str(2)
		group_id_field = str(31)
	else:
		message_field = str(2)
		group_id_field = str(6)

	whitelist = "1gram.%s-%s.fof_001.txt" % (year, month)

	for n in [3]:
		n = str(n)

		num_of_files = len(files) / 4


		first_list = ",".join(files[:num_of_files])
		output_path = "/localdata/twitter10pct_feat/feat.%sgram.%s-%s.part%s" % (str(n), year, month, "1") 
		first_command = " ".join([runfile, first_list, output_path, message_field, group_id_field, n, whitelist])
		print(first_command)

		second_list = ",".join(files[num_of_files:2*num_of_files])
		output_path = "/localdata/twitter10pct_feat/feat.%sgram.%s-%s.part%s" % (str(n), year, month, "2") 
		second_command = " ".join([runfile, second_list, output_path, message_field, group_id_field, n, whitelist])

		print(second_command)

		third_list = ",".join(files[2*num_of_files:3*num_of_files])
		output_path = "/localdata/twitter10pct_feat/feat.%sgram.%s-%s.part%s" % (str(n), year, month, "3") 
		third_command = " ".join([runfile, third_list, output_path, message_field, group_id_field, n, whitelist])
		print(third_command)

		fourth_list = ",".join(files[3*num_of_files:])
		output_path = "/localdata/twitter10pct_feat/feat.%sgram.%s-%s.part%s" % (str(n), year, month, "4") 
		fourth_command = " ".join([runfile, fourth_list, output_path, message_field, group_id_field, n, whitelist])

		print(fourth_command)
		#exit()
		#os.system(command) 

# /localdata/twitter10pct_feat/feat.2gram.2012-05
# /localdata/twitter10pct_feat/feat.2gram.2012-07
# /localdata/twitter10pct_feat/feat.2gram.2012-08
# /localdata/twitter10pct_feat/feat.2gram.2012-11
# /localdata/twitter10pct_feat/feat.2gram.2012-12
# /localdata/twitter10pct_feat/feat.2gram.2013-01
# /localdata/twitter10pct_feat/feat.2gram.2013-05
# /localdata/twitter10pct_feat/feat.2gram.2013-06
# /localdata/twitter10pct_feat/feat.2gram.2013-07
# /localdata/twitter10pct_feat/feat.2gram.2013-08
#--feat_occ_filter --set_p_occ 0.10 --group_freq_thresh 100000