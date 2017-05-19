from operator import add
from pyspark import SparkContext, SparkConf
import argparse
import os
import shutil
import time

TRANS_NUM = 0
SUPPORT_NUM = 0.0

def parseLine(line):
	bit_list = line[0].split(args.marker)
	bit_list = bit_list[0:len(bit_list)-1]
	record = frozenset(bit_list)
	return (record, line[1])

def mineOneItem(x):
	_item_list = []
	for bit in x[0]:
		_item_list.append((frozenset(bit.split(",")),x[1]))
	return _item_list

def mineItem(x):
	_item_list = []
	for cand in broadcast_pattern.value:
		if cand.issubset(x[0]):
			_item_list.append((cand, x[1]))
	return _item_list

def subsetCheck(input_set,parent_set):
	test_set = set(input_set)
	for item in test_set:
		test_set.remove(item)
		if test_set not in parent_set:
			return False
		test_set.add(item)
	return True

def getChildPattern(parent_pattern,ite):
	child_list = frozenset([i.union(j) for i in parent_pattern for j in parent_pattern if (len(i.union(j))==ite and subsetCheck(i.union(j),parent_pattern))])
	return child_list

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--input', type=str, nargs='+', help="Input data path.")
	parser.add_argument('--support', type=float, help="The support rate.",
                    default=0.85)
	parser.add_argument('--k', type=int, help="The number of frequent pattern.",
                    default=7)
	parser.add_argument('--output', type=str,
                    help="Output result path.", default="Apriori_Result")
	parser.add_argument('--verbose', action='store_true')
	parser.add_argument('--master', type=str,
                    help="The master of spark.", default="local")
	parser.add_argument('--marker', type=str,
                    help="The marker in each line.", default=" ")
	parser.add_argument('--numPartition', type=int,
                    help="The marker in each line.", default=16)
	global args 
	args = parser.parse_args()
	conf = SparkConf().setAppName("Apriori").setMaster(args.master)
	sc = SparkContext(conf=conf)
	start = time.time()

	#To get transaction from input file
	_trans = sc.textFile(args.input[0],args.numPartition).map(lambda line:(line, 1))
	TRANS_NUM = _trans.count()
	SUPPORT_NUM = TRANS_NUM*args.support
	if args.verbose:
		print "Total Transactions Number: "+str(TRANS_NUM)
		print "Support Transactions Number: "+str(SUPPORT_NUM)
	#To remove duplicated 
	_trans = _trans.reduceByKey(add).map(parseLine).cache()

	#To get 1-item frequent pattern
	one_item = _trans.flatMap(mineOneItem).reduceByKey(add).filter(lambda x:x[1]>SUPPORT_NUM).cache()
	result_buffer = one_item.map(lambda x:str(x[0])+":"+str(float(x[1])/TRANS_NUM))
	if args.verbose:
		print "1-item pattern:"
		print result_buffer.collect()
	#result_buffer.saveAsTextFile(args.output+"/1_item.out")

	#To get 2-k item frequent pattern
	frequent_pattern = one_item
	for i in range(2,args.k+1):
		child_pattern = getChildPattern(frequent_pattern.map(lambda x:x[0]).collect(),i)
		#print child_pattern
		if len(child_pattern)==0:
			break
		broadcast_pattern = sc.broadcast(child_pattern)
		frequent_pattern = _trans.flatMap(mineItem).reduceByKey(add).filter(lambda x:x[1]>SUPPORT_NUM).cache()
		result_buffer = frequent_pattern.map(lambda x:str(x[0])+":"+str(float(x[1])/TRANS_NUM))
		if args.verbose:
			print str(i)+"-item pattern:"
			print result_buffer.collect()
		#result_buffer.saveAsTextFile(args.output+"/"+str(i)+"_item.out")
		broadcast_pattern.unpersist()
	stop = time.time()
	if args.verbose:
		print "Complete! Time cost: {}".format(stop - start)