import math
import argparse
import string
import os
from multiprocessing import Process, Lock

global wordMap 
wordMap = {}

def main():
	#Parsing the command line arguments to run the wordCount
	parser = argparse.ArgumentParser(description = "Word Frequency")
	parser.add_argument('-o', '--outputFile', required=True)
	parser.add_argument('-t', '--inputFile', required=True)
	arguments = vars(parser.parse_args())

	print "hi1."

	resultPath = os.getcwd()
	root = resultPath + "/root"
	
	inputFile = arguments['inputFile']
	outputFile = arguments['outputFile']

	splitFile(inputFile)

	diskLock = Lock()

	print "hi."

	process1 = Process(target=worker, args=('partaa', root, diskLock))
	#process2 = Process(target=worker, args=('partab', root, diskLock))
	#process3 = Process(target=worker, args=('partac', root, diskLock))
	#process4 = Process(target=worker, args=('partad', root, diskLock))

	totalProcesses = [process1]#, process2, process3, process3]

	for process in totalProcesses:
		process.start()

	for process in totalProcesses:
		process.join()

	result = open(outputFile, 'a')
	finalCount(root, result)

	result.close()

	print "Counting done."

def worker(chunkPath, root, diskLock):
	#wordMap = {}
	with open(chunkPath, 'r') as chunk:
		for line in chunk:
			line = line.strip()
			line = line.translate(string.maketrans("",""), string.punctuation)
			words = line.split()
			for word in words:
				wordMap[word] = 1

	diskLock.acquire()
	writeToDisk(root)
	diskLock.release()

def finalCount(root, result):
	for rootPath, dirs, files in os.walk(root):
		for fileName in files:
			filePath = os.path.join(rootPath, fileName)
			word = wordFromPath(root, filePath)
			frequency = str(frequency(filePath))
			result.write(word + ':' + frequency + '\n')

def wordFromPath(root, filePath):
	endOfFile = len(root) + 1
	wPath = filePath[endOfFile:-4]
	word = wPath.replace("/", "")
	return word

def frequency(filePath):
	with open(filePath) as f:
		return len(f.read)

def numberOfFileLines(inputFile):
	count = 0
	with open(inputFile, 'r') as iF:
		for line in iF:
			count += 1
	return count

def splitFile(inputFile):
	numberOfLines = numberOfFileLines(inputFile)
	print numberOfLines
	chunkSize = int(math.ceil(numberOfLines/4.0))
	os.system('split -l '+ str(chunkSize) + ' ' + inputFile + ' part')

def writeToDisk(root):
	finalFile = open(root + '/end', 'a')
	if not os.path.exists(finalFile):
		os.makedirs(fileName)
	for key in wordMap:
		finalFile.write(key + ":" + wordMap.get(key))

if __name__ == "__main__":
	main()
