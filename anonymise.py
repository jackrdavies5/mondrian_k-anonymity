"""
Mondrian Multidimensional k-Anonymity Tool
Implementation by Jack Davies 2021
jackrdavies5@gmail.com

Based on Mondrian Multidimensional k-Anonymity:
	LeFevre, K. et al. 2006. Mondrian Multidimensional k-Anonymity. Proceedings of the 22nd International Conference on Data Engineering (ICDE’06)
"""

import csv
import sys
import time

RECORDS = []
QIDS = []
K = 0
PARTITIONS = []
ANON_PARTITIONS = []


def importData(path, remove_headers):
	# import data from csv
	with open(path, newline='') as csvfile:
		testreader = csv.reader(csvfile)
		if(remove_headers == "1"):
			# skip header row
			next(testreader) 
		for row in testreader:
			# append to list
			RECORDS.append(tuple(row))
	csvfile.close

def bestAttrib(partition, QIDS):
	# pick best attribute based on largest range of values
	
	attrib_domains = [] # container for all QID domains

	# cycle over all QIDs and store domain for each
	for attrib in QIDS:
		attrib_vals = set(())
		for t in partition:
			attrib_vals.add(t[attrib])
		attrib_vals = list(attrib_vals)
		attrib_vals.sort()
		attrib_domains.append(attrib_vals)

	# find QID with largest range of values
	best = -1
	best_index = -1
	best_size = 0
	for i, QID in enumerate(QIDS):
		if(len(attrib_domains[i]) > best_size):
			best = QID
			best_index = i
			best_size = len(attrib_domains[i])

	return attrib_domains[best_index], best

def isNumericArray(array):
	# check if a set of values is numerical
	numeric = True
	for val in array:
		if not val.isnumeric():
			numeric = False
			break;

	return numeric

def frequencySet(attrib, attribDom, dataset):
	# create orderered frequency set for values in dataset
	frequencies = []
	for val in range(0, len(attribDom)):
		frequencies.append(0)
		for t in dataset:
			if t[attrib] == attribDom[val]:
				frequencies[val] += 1

	return frequencies

def findMedian(frequencies, attribDom):
	# find median value from attribute value frequency set
	total = sum(frequencies)
	median = 0
	if total % 2 != 0:
		median = (total+1)/2
	else:
		x = total/2
		y = (total/2)+1
		median = (x+y)//2

	# staticMedian used to save actual median
	staticMedian = median

	# subtract frequency counts in order until median is exceeded; index = median value
	median_val = -1
	for index, count in enumerate(frequencies):
		median -= count
		if median <= 0:
			median_val = index
			break

	return (staticMedian, attribDom[median_val])

def partitionStrict(attrib, medVal, attribDom, dataset):
	# split set of data into two distinct partitions using Strict Partitioning
	lhs = []
	rhs = []
	medIndex = attribDom.index(medVal)

	for t in dataset:
		if(attribDom.index(t[attrib]) <= medIndex):
			lhs.append(t)
		else:
			rhs.append(t)

	return (lhs, rhs)

def partitionRelaxed(median, attrib, medVal, attribDom, dataset):
	# split set of data into two partitions using Relaxed Partitioning

	lhs = []
	rhs = []
	medianTuples = []
	medIndex = attribDom.index(medVal)
	iterator = 0

	for t in dataset:
		if(attribDom.index(t[attrib]) < medIndex):
			lhs.append(t)
			iterator+=1
		elif(attribDom.index(t[attrib]) > medIndex):
			rhs.append(t)
		else:
			medianTuples.append(t)

	# move tuples from median subset one by one into other subsets
	for t in medianTuples:
		iterator+=1
		if(iterator <= median):
			lhs.append(t)
		else:
			rhs.append(t)

	return (lhs, rhs)

def mondrianStrict(dataset, sQIDS):
	# main algorithm - Strict

	# simple check - see if dataset can potentially be split
	if (len(dataset) < 2*K) or (len(sQIDS) == 0):
		return dataset
	
	(attribDom, best) = bestAttrib(dataset, sQIDS)
	freqs = frequencySet(best, attribDom, dataset)
	(median, medVal) = findMedian(freqs, attribDom)
	if medVal == attribDom[-1]:
		# median value is last value in ordered list
		# can't split partition on this dimension - recursively try next dimension
		NEW_QIDS = sQIDS[:]
		NEW_QIDS.remove(best)
		return mondrianStrict(dataset, NEW_QIDS)

	(lhs, rhs) = partitionStrict(best, medVal, attribDom, dataset)

	if(len(set(lhs)) < K or len(set(rhs)) < K):
		# size of either partition lower than K - recursively try next dimension
		NEW_QIDS = sQIDS[:]
		NEW_QIDS.remove(best)
		return mondrianStrict(dataset, NEW_QIDS)

	# reset QIDs
	if QIDS != sQIDS:
		sQIDS = QIDS
	
	# recursive call with new partitions as dataset - append optimal partitions to global container
	PARTITIONS.append(mondrianStrict(lhs, sQIDS))
	PARTITIONS.append(mondrianStrict(rhs, sQIDS))

def mondrianRelaxed(dataset, sQIDS):
	# main partitioning algorithm - Relaxed

	# simple check - see if dataset can potentially be split
	if len(dataset) < 2*K or (len(sQIDS) == 0):
		return dataset
	
	(attribDom, best) = bestAttrib(dataset, sQIDS)
	freqs = frequencySet(best, attribDom, dataset)
	(median, medVal) = findMedian(freqs, attribDom)
	(lhs, rhs) = partitionRelaxed(median, best, medVal, attribDom, dataset)

	if(len(set(lhs)) < K or len(set(rhs)) < K):
		# size of either partition lower than K - recursively try next dimension
		NEW_QIDS = sQIDS[:]
		NEW_QIDS.remove(best)
		return mondrianRelaxed(dataset, NEW_QIDS)

	# reset QIDs
	if QIDS != sQIDS:
		sQIDS = QIDS
	
	# recursive call with new partitions as dataset - append optimal partitions to global container
	PARTITIONS.append(mondrianRelaxed(lhs, sQIDS))
	PARTITIONS.append(mondrianRelaxed(rhs, sQIDS))


def generalise(partition, QIDS):
	# anonymise partitions by generalisation

	summaries = [] # container for QID summarisations

	# cycle over QIDs and store all values in partition for given QID as a summarisation
	for attrib in QIDS:
		attrib_set = []
		for p in partition:
			attrib_set.append(p[attrib])
		attrib_set = set(attrib_set)
		attrib_set = list(attrib_set)
		if len(attrib_set) > 1:
			if(attrib_set[0].isnumeric()):
				attrib_set = list(map(int, attrib_set))
			attrib_set.sort()
			summaries.append(attrib_set)
		else:
			summaries.append(attrib_set[0])

	# convert partition to a transcribable list of anonymised tuples
	anon_partition = []
	for t in partition:
		anon_tuple = list(t)
		for i, QID in enumerate(QIDS):
			anon_tuple[QID] = summaries[i]
		anon_partition.append(tuple(anon_tuple))

	return anon_partition

def writeToCsv(dataset, filename):
	# write data to csv file
	with open(filename, 'w', newline='') as csvfile:
		writer = csv.writer(csvfile, dialect='excel')
		writer.writerows(dataset)
		csvfile.close

def anonymise(args):
	# main function

	# define globals
	global K
	global QIDS
	K = int(args[3])
	QIDS = list(args[2].split(","))
	QIDS = list(map(int, QIDS))

	# import entire data set
	importData(args[1], args[5])

	# execute Strict or Relaxed algorithm
	if(args[6] == "1"):
		mondrianStrict(RECORDS, QIDS)
	else:
		mondrianRelaxed(RECORDS, QIDS)

	# cycle over all non-anonymised partitions and generalise values to anonymise
	for partition in PARTITIONS:
		if partition != None and len(partition) != 0:
			ANON_PARTITIONS.append(generalise(partition, QIDS))

	k_anonymisation = [] # container for full transcribable k-anonymisation
	for anon in ANON_PARTITIONS:
		# sanity check for size of equivalence classes
		if len(anon) < K:
			print("EC SIZE LOWER THAN K: ", len(anon))
			return
		# append anonymised tuple
		k_anonymisation += anon

	# check if k-anonymisation was made - if so then write output
	if(len(k_anonymisation) > 0):
		writeToCsv(k_anonymisation, args[4])
		if(args[6] == "1"):
			print("Successful strict anonymisation. Output to: " + args[4])
		else:
			print("Successful relaxed anonymisation. Output to: " + args[4])
	else:
		# k-anonymisation not possible for selected parameters
		print("Cannot be anonymised for K =", K, "or selected QIDs.")

def anonymiseNoOutput(args):
	# duplicate function of anonymise() with no output - used for testing of run time

	start_time = time.time_ns()
	global K
	global QIDS
	K = int(args[3])
	QIDS = list(args[2].split(","))
	QIDS = list(map(int, QIDS))
	importData(args[1], args[5])
	if(args[6] == "1"):
		print("STRICT")
		mondrianStrict(RECORDS, QIDS)
	else:
		mondrianRelaxed(RECORDS, QIDS)
	for partition in PARTITIONS:
		if partition != None:
			ANON_PARTITIONS.append(generalise(partition, QIDS))

	k_anonymisation = []
	for anon in ANON_PARTITIONS:
		k_anonymisation += anon


	if(len(k_anonymisation) == 0):
		print("Cannot be anonymised for K = ", K, " or selected QIDs.")

	return time.time_ns() - start_time # return time taken for execution of algorithm in NANOSECONDS

# get terminal argv to use as algorithm arguments
args = sys.argv
if(len(args) != 7):
	print("Incorrect number of arguments to run. Input: input_filename, QID_list, k_value, output_filename, headers[0=F,1=T], strict[0=F,1=T]")
	sys.exit()

anonymise(args)