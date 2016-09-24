import sys 
import os

#input param
rel1_name = sys.argv[1]    
rel2_name = sys.argv[2]
rel1_recordsize = int(sys.argv[3])
rel2_recordsize = int(sys.argv[4])
pagesize = int(sys.argv[5])
n_page = int(sys.argv[6])
n_rounds = int(sys.argv[7])

#calculating number of records in a page

rel1_record_in_page = pagesize/rel1_recordsize
rel2_record_in_page = pagesize/rel2_recordsize
n_bucket = n_page - 1

print "Total number of available pages: ", n_page
print "Number of buckets in hash table: ", n_bucket

#do hashing


#defining hash func

def hashfunc (record,roundNo):
	#print "Hashing record: ", record
	#print "On Round No: ", roundNo
	hash_val = (record + roundNo*(record*record))%n_bucket
	return hash_val;

def dohash(round,file1,file2):
		if(round> n_rounds):
			print "Sorry :( can't perform hash-join with" + str(n_rounds) + " rounds"
			sys.exit()
		rel1 = open(file1,"r")
		rel2 = open(file2,"r")
		count1 = 0
		count2 = 0		
		for rec in rel1:
			count1 += 1
		for rec in rel2:
			count2 += 1
		rel1.close()
		rel2.close()
		if(count1%rel1_record_in_page>0):
			count1 = count1/rel1_record_in_page + 1
		else:
			count1 = count1/rel1_record_in_page
		if(count2%rel2_record_in_page>0):
			count2 = count2/rel2_record_in_page + 1
		else:
			count2 = count2/rel2_record_in_page
		print "Size of rel1: ", count1
		print "Size of rel1: ", count2
		if(count1 + count2 <= n_page):
			print "Total Page Size:" + str(count1 + count2) + " Performing in Memory Join"
			rel1 = open(file1,"r")
			for i in rel1:
				rel2 = open(file2,"r")
				for j in rel2:
					rel1_val = int(i)
					rel2_val = int(j)
					if(rel1_val == rel2_val):
						file_name = "join-output.txt"
						file = open(file_name,"a")
						file.write(str(rel1_val) + "    " + str(rel2_val) )
						file.write("\n")
						file.close()
				rel2.close()
			rel1.close()
		else:	
			print "Hashing Round : ",round
			print "----reading relation 1:-------"
			listoflists = []
			bucket_list = []
			for i in range(0,n_bucket):
				listoflists.append(list(bucket_list))
			rel1 = open(file1,"r")
			for i in rel1:
				j = int(i)
				hash_val = hashfunc(j,round)
				print "Tuple :" + str(i) + "Mapped to bucket: " + str(hash_val)
				if(len(listoflists[hash_val])< rel1_record_in_page):
					listoflists[hash_val].append(j)
				if(len(listoflists[hash_val])>= rel1_record_in_page):
					print "Page for bucket " + str(hash_val) +" full -- Flushed to secondary storage"
					file_name = file1 + ".r" + str(round) + ".b" + str(hash_val) + ".txt"
					file = open(file_name,"a")
					file.write("\n".join([str(j) for j in listoflists[hash_val]]))
					file.write("\n")
					file.close()
					listoflists[hash_val][:] = []
			for i in range(0,n_bucket):
				if(len(listoflists[i])>0):
					file_name = file1 + ".r" + str(round) + ".b" + str(i) + ".txt"
					file = open(file_name,"a")
					file.write("\n".join([str(j) for j in listoflists[i]]))
					file.write("\n")
					file.close()
					listoflists[i][:] = []
			listoflists[:] = []
			rel1.close()
			print "Done with relation1:"
			print "Created following files:"
			for i in range(0,n_bucket):
				file_name = file1 + ".r" + str(round) + ".b" + str(i) + ".txt"
				if(os.path.isfile(file_name)):
					print file_name
			for i in range(0,n_bucket):
				listoflists.append(list(bucket_list))
			rel2 = open(file2,"r")
			print "----reading relation 2:------"
			for i in rel2:
				j = int(i)
				hash_val = hashfunc(j,round)
				print "Tuple :" + str(i) + "Mapped to bucket: " + str(hash_val)
				if(len(listoflists[hash_val])< rel2_record_in_page):
					listoflists[hash_val].append(j)
				if(len(listoflists[hash_val])>= rel2_record_in_page):
					print "Page for bucket " + str(hash_val) +" full -- Flushed to secondary storage"
					file_name = file2 + ".r" + str(round) + ".b" + str(hash_val) + ".txt"
					file = open(file_name,"a")
					file.write("\n".join([str(j) for j in listoflists[hash_val]]))
					file.write("\n")
					file.close()
					listoflists[hash_val][:] = []
			for i in range(0,n_bucket):
				if(len(listoflists[i])>0):
					file_name = file2 + ".r" + str(round) + ".b" + str(i) + ".txt"
					file = open(file_name,"a")
					file.write("\n".join([str(j) for j in listoflists[i]]))
					file.write("\n")
					file.close()
					listoflists[i][:] = []
			listoflists[:] = []
			print "Done with relation2:"
			print "Created following files:"
			for i in range(0,n_bucket):
				file_name = file2 + ".r" + str(round) + ".b" + str(i) + ".txt"
				if(os.path.isfile(file_name)):
					print file_name
			rel2.close()
			for i in range(0,n_bucket):
				file_name1 = file1 + ".r" + str(round) + ".b" + str(i) + ".txt"
				file_name2 = file2 + ".r" + str(round) + ".b" + str(i) + ".txt"
				if((os.path.isfile(file_name1)) and (os.path.isfile(file_name2))):
					dohash(round+1,file_name1,file_name2)
				if((os.path.isfile(file_name1)) and not(os.path.isfile(file_name2))):
					print "Bucket" + str(i) + ": No matching tuple from relation2. No further processing"
				if(not(os.path.isfile(file_name1)) and (os.path.isfile(file_name2))):
					print "Bucket" + str(i) + ": No matching tuple from relation1. No further processing"
	
dohash(1,rel1_name,rel2_name)

