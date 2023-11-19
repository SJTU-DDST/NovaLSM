import os
from pprint import pprint
import sys
import os.path
import json

import time
import datetime
import pickle

def safe_divide(a, b):
    if b == 0:
        return 0
    else:
        return float(a)/float(b)

def convert_time(timestr):
    # print(timestr)
    answer = 0
    count = 0
    for i in range(len(timestr)):
        if timestr[i] == "s":
            answer = answer + 1000 * count
            count = 0
        elif timestr[i] == "m":
            answer = answer + count
            count = 0
            return answer
        else:
            count = count * 10 + ord(timestr[i]) - ord('0')
    return answer

def get_average(arr, gap = 10000):
    tmp_average = 0
    tmp_count = 0
    
    # gap = 10000
    cuts = 0
    
    if len(arr) == 0:
        return 0
    
    
    if len(arr) % gap == 0:
        cuts = int(len(arr)/gap)
    else:
        cuts = int(len(arr)/gap) + 1
        
    for i in range(cuts):
        begin = i * gap
        end = min(len(arr), begin + gap)
        tmp_sum = 0
        for j in range(begin, end):
            tmp_sum = tmp_sum + arr[j]
        tmp_average = float(tmp_average) * (float(tmp_count) / float(tmp_count + end - begin)) + float(tmp_sum) / float(tmp_count + end - begin)
        
    return tmp_average
    
def print_in_format(exps, number_of_tab):
    if type(exps) == dict:
        for exp in exps:
            print("\t" * number_of_tab + str(exp))
            print_in_format(exps[exp], number_of_tab + 1)
    elif type (exps) == list:
        pass 
        # pass
        # print("\t" * number_of_tab + str(exps))
    else:
        print("\t" * number_of_tab + str(exps))    
    
def parse_get_stats(file):
   
    getanalysis = {}
    getanalysis["get in memtable"] = {}
    getanalysis["get in l0 sstable"] = {}
    getanalysis["get in l1 sstable or upper"] = {}
    
    for op in getanalysis:
        getanalysis[op]["percentage"] = 0 #
        getanalysis[op]["total time"] = {}
        getanalysis[op]["memtable get time"] = {}
        getanalysis[op]["wait time"] = {}
        getanalysis[op]["wait percentage"] = 0 # 
        getanalysis[op]["l0 sstable get time"] = {}
        getanalysis[op]["l1 upper get time"] = {}
        # getanalysis[op]["total time"]["average"] = ""
        for op1 in getanalysis[op]:
            if type(getanalysis[op][op1]) == dict:
                getanalysis[op][op1]["avg"] = ""
                getanalysis[op][op1]["p95"] = ""
                getanalysis[op][op1]["p99"] = ""
                getanalysis[op][op1]["seq"] = []
    
    try:
        file = open(file, 'r')
    except:
        print("err happen while opening file")
        return 

    lines = file.readlines()
    mysum = 0
    for line in lines:
        if "get type" in line:
            line = line[line.find("get type"):]
            mysum = mysum + 1
            op = ""
            start = False
            ems = line.replace('\n', '').split(",")
            for i in range(len(ems)):
                ems[i] = ems[i].strip()
            for unit in ems:
                kvs = unit.split(":")
                for i in range(len(kvs)):
                    kvs[i] = kvs[i].strip()
                # print(kvs)
                if kvs[0] == "get type":
                    op = kvs[1]
                    getanalysis[op]["percentage"] = getanalysis[op]["percentage"] + 1
                elif kvs[0] == "wait for major compaction":
                    if kvs[1] == "true":
                        getanalysis[op]["wait percentage"] = getanalysis[op]["wait percentage"] + 1   
                    else:
                        getanalysis[op]["wait time"]["seq"].pop()
                else:
                    # print(kvs)
                    getanalysis[op][kvs[0]]["seq"].append(convert_time(kvs[1]))
    
    for op in getanalysis:
        getanalysis[op]["wait percentage"] = safe_divide(getanalysis[op]["wait percentage"], getanalysis[op]["percentage"])
        getanalysis[op]["percentage"] = safe_divide(getanalysis[op]["percentage"], mysum)
        # getanalysis[op]["total time"] = {}
        # getanalysis[op]["memtable get time"] = {}
        # getanalysis[op]["wait time"] = {} 
        # getanalysis[op]["l0 sstable get time"] = {}
        # getanalysis[op]["l1 upper get time"] = {}
        # getanalysis[op]["total time"]["average"] = ""
        for op1 in getanalysis[op]:
            if type(getanalysis[op][op1]) == dict:
                if len(getanalysis[op][op1]["seq"]) == 0:
                    continue
                else:
                    # print(getanalysis[op][op1]["seq"])
                    # print(sum(getanalysis[op][op1]["seq"]))
                     # safe_divide(sum(getanalysis[op][op1]["seq"]), len(getanalysis[op][op1]["seq"]))
                    # print(int(0.95 * float(len(getanalysis[op][op1]["seq"]))))
                    # print(len(getanalysis[op][op1]["seq"]))
                    if len(getanalysis[op][op1]["seq"]) == 0:
                        pass
                    else:
                        if op != "get in memtable":
                            getanalysis[op][op1]["avg"] = get_average(getanalysis[op][op1]["seq"], 1000)
                        else:
                            getanalysis[op][op1]["avg"] = get_average(getanalysis[op][op1]["seq"])
                            
                        getanalysis[op][op1]["seq"].sort()
                        getanalysis[op][op1]["p95"] = getanalysis[op][op1]["seq"][int(0.95 * float(len(getanalysis[op][op1]["seq"])))]
                        getanalysis[op][op1]["p99"] = getanalysis[op][op1]["seq"][int(0.99 * float(len(getanalysis[op][op1]["seq"])))]
                    # getanalysis[op][op1]["seq"] = [] # 

    print_in_format(getanalysis, 0)



parse_file = sys.argv[1]
parse_get_stats(parse_file)


