import math
import argparse
import string
import os
from multiprocessing import Process

global wordMap 
wordMap = {}

def main():
	#Parsing the command line arguments to run the wordCount
	parser = argparse.ArgumentParser(description = "Word Frequency")
	parser.add_argument('-o', '--outputFile', required=True)
	parser.add_argument('-t', '--inputFile', required=True)
	arguments = vars(parser.parse_args())

	resultPath = os.getcwd()
	root = resultPath + "/root"
	
	inputFile = arguments['inputFile']
	outputFile = arguments['outputFile']

	tIDs = [0, 1, 2, 3]

	splitFile(inputFile)

	"""
	Using three process because the test case file isn't large enough to
	spawn over 4 processes but the implementation for 4 different processes
	will just require to comment out the the proccess4 allocation.
	Just dividing the file into the 4 seperate chunks to deal with.
	"""

	process1 = Process(target=worker, args=('partaa', root, tIDs[0]))
	process2 = Process(target=worker, args=('partab', root, tIDs[1]))
	process3 = Process(target=worker, args=('partac', root, tIDs[2]))
	#process4 = Process(target=worker, args=('partad', root, tIDs[3]))

	totalProcesses = [process1, process2, process3]#, process4]

	for process in totalProcesses:
		process.start()

	#waiting for the processes to complete execution.
	for process in totalProcesses:
		process.join()

	result = open(outputFile, 'a')
	finalCount(root, result)

	result.close()

	print "Counting done."

def worker(chunkPath, root, tID):
	"""
	Basic implementation of a worker server that is responsible for reading
	a chunk of the path allocated to it and count the number of words in it
	and write the results in a text file.
	"""
	count = 0
	with open(chunkPath, 'r') as chunk:
		for line in chunk:
			line = line.strip()
			line = line.translate(string.maketrans("",""), string.punctuation)
			words = line.split()
			for word in words:
				count += 1
				"""
				test file not large enough to convey this.
				idea is to not store large hunks fo data in memory and
				to keep writing to mtext files so that we do not run out of RAM.
				#if(count >5):
					#writeToDisk(root, tID)
				"""
				wordMap[word] = 1
	writeToDisk(root, tID)
		
def finalCount(root, result):
	"""
	not completely implemented completely but intention is to 
	combine the results generated in the different files into 1.
	"""
	for rootPath, dirs, files in os.walk(root):
		for fileName in files:
			filePath = os.path.join(rootPath, fileName)
			word = wordFromPath(root, filePath)
			frequency = str(frequency(filePath))
			result.write(word + ':' + frequency + '\n')

def wordFromPath(root, filePath):
	"""
	To extract words from a file path.
	"""
	endOfFile = len(root) + 1
	wPath = filePath[endOfFile:-4]
	word = wPath.replace("/", "")
	return word

def frequency(filePath):
	with open(filePath) as f:
		return len(f.read)

def numberOfFileLines(inputFile):
	"""
	just to output the number of lines of a text file.
	"""
	count = 0
	with open(inputFile, 'r') as iF:
		for line in iF:
			count += 1
	return count

def splitFile(inputFile):
	"""
	used to split the file into four different processes.
	"""
	numberOfLines = numberOfFileLines(inputFile)
	chunkSize = int(math.ceil(numberOfLines/4.0))
	os.system('split -l '+ str(chunkSize) + ' ' + inputFile + ' part')

def writeToDisk(root, tID):
	"""
	create a new file for each process and then write all the words of the hash map
	of that particular process to that file.
	"""
	filePath = root + '/results'
	if not os.path.exists(filePath):
		os.makedirs(root + '/results')
	finalFile = open("results" + str(tID) + ".txt", 'a')
	for key in wordMap:
		finalFile.write(str(key) + ":" + str(wordMap.get(key)))

if __name__ == "__main__":
	main()