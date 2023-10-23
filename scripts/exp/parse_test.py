import os
from pprint import pprint
import sys
import os.path
import json

import time
import datetime

# 字符串转int
def convert_int(val):
	try:
		return int(val)
	except:
		return 0


def remove_empty(array):
	narray=[]
	for em in array:
		if em != '':
			narray.append(em)
	return narray

# 去除换行符并且用逗号分隔
def convert_disk_stats_to_array(line):
	ems = line.replace('\n', '').split(",")
	return ems

# 求和
def sum_disk_stats(line):
	ems = convert_disk_stats_to_array(line)
	s = 0
	for em in ems:
		s += convert_int(em)
	return s

params=[]

# 拆解实验的目录结构得到当前实验得到配置
def expname(exp_dir_name):
    # nova-d-$dist-w-$workload-nm-$num_memtables-np-$num_memtable_partitions-s-$enable_subrange-mc-$major_compaction_type
    # nova-zf-0.99-nm-256-lr-1-try-0-cfg-false-d-uniform-w-workloadw-ltc-1-stoc-1-l0-10240-np-64-nr-1
    ems = exp_dir_name.split("-")
    exp_params={}
    i = 1
    expname=""
    record_param = False
    if len(params) == 0:
        record_param = True

    while i < len(ems):
        if record_param:
            params.append(ems[i])
        value = ems[i+1]
        if ems[i+1] == "workloada":
            value="RW50"
        elif ems[i+1] == "workloadw":
            value="W100"
        elif ems[i+1] == "lc":
            value="LTC"
        elif ems[i+1] == "sc":
            value="StoC"
        exp_params[ems[i]] = value
        expname += value
        expname += ","
        i += 2

    print(exp_params)
    expname=expname[:-1]
    return expname

if __name__ == "__main__":

    exp = expname("nova-zf-0.99-nm-256-lr-1-try-0-cfg-false-d-uniform-w-workloadw-ltc-1-stoc-1-l0-10240-np-64-nr-1")
    print(exp)
    # nodes={}    
    # num_nodes=3
    # total_log_records=0
    # # if node_id != 0:
    # # 	continue
    # node_id = 0
    # try:
    #     file = open("{}/server-{}-out".format("/home/yuhang/NovaLSM/nova-tutorial-10000000/nova-zf-0.99-nm-256-lr-1-try-0-cfg-false-d-uniform-w-workloadw-ltc-1-stoc-1-l0-10240-np-64-nr-1", node_id), 'r')
    # except:
    #     pass

    # nodes[node_id] = {}
    # nodes[node_id]["storage"] = []
    # nodes[node_id]["storage_reads"] = []
    # nodes[node_id]["storage_writes"] = []
    # nodes[node_id]["active_memtables"] = []
    # nodes[node_id]["immutable_memtables"] = []
    # nodes[node_id]["memtables"] = []
    # nodes[node_id]["gets"] = []
    # nodes[node_id]["hits"] = []
    # nodes[node_id]["hit_rate"] = []
    # nodes[node_id]["steals"] = []
    # nodes[node_id]["puts"] = []
    # nodes[node_id]["waits"] = []
    # nodes[node_id]["wait_rate"] = []
    # nodes[node_id]["no_wait_rate"] = []
    # nodes[node_id]["memtable_hist"] = []
    # nodes[node_id]["db_size"] = []
    # nodes[node_id]["num_l0_tables"] = []
    # nodes[node_id]["log_records"] = 0

    # lines = file.readlines()
    # i = 0
    # while i < len(lines):
    #     line = lines[i]
    #     if "log records" in line: # 如果不开log的话 是没有这个记录的
    #         num_log_records=int(line.split(" ")[-3]) #
    #         nodes[node_id]["log_records"] += num_log_records
    #         total_log_records += num_log_records
    #         i+=1
    #         continue

    #     # 这里原来是干嘛的

    #     if "[stat_thread.cpp" not in line: #找到stats的起始打印
    #         i+=1
    #         continue

    #     try:
    #         # frdma 1
    #         # brdma 2
    #         # compaction 3
    #         # fg-storage 4
    #         # fg-storage-read 5
    #         # fg-storage-write 6
    #         # bg-storage 7
    #         # bg-storage-read 8
    #         # bg-storage-write 9
    #         # c-storage 10
    #         # c-storage-read 11
    #         # c-storage-write 12
    #         # active-memtables 13
    #         # immutable-memtables 14
    #         # steals 15
    #         # puts 16
    #         # wait-due-to-contention 17
    #         # gets 18
    #         # hits 19
    #         # scans 20
    #         # searched_file_per_miss 21
    #         # memtable-hist 22
    #         # puts-no-wait 23
    #         # puts-wait 24



    #         # db-overlapping-sstable-stats-0 25                               # db 25
    #         # db-size-stats-0 26
    #         # db-overlap-overall-0 27
    #         # db-overlap-0 28
    #         # db 29
    #         i+=4
    #         # 绕过stat_thread.cpp、以及前3个
    #         storage = sum_disk_stats(lines[i]) / 10 # fg-storage 4
    #         i+=1
    #         storage_reads = sum_disk_stats(lines[i]) / 1024 / 1024 / 10 # fg-storage-read 5
    #         i+=1
    #         storage_writes = sum_disk_stats(lines[i]) / 1024 / 1024 / 10 # fg-storage-write 6
            

    #         nodes[node_id]["storage"].append(storage)
    #         nodes[node_id]["storage_reads"].append(storage_reads)
    #         nodes[node_id]["storage_writes"].append(storage_writes)

    #         i+=7
    #         nodes[node_id]["active_memtables"].append(lines[i].replace("\n","")) # active-memtables 13
    #         i+=1
    #         nodes[node_id]["immutable_memtables"].append(lines[i].replace("\n","")) # immutable-memtables 14
    #         i+=1

    #         actives = nodes[node_id]["active_memtables"][-1].split(",")
    #         immutables = nodes[node_id]["immutable_memtables"][-1].split(",")
    #         all_mems = ""
    #         for j in range(len(actives)):
    #             all_mems += str(convert_int(actives[j]) + convert_int(immutables[j]))
    #             all_mems += ","

    #         nodes[node_id]["memtables"].append(all_mems)
    #         nodes[node_id]["steals"] = convert_disk_stats_to_array(lines[i]) # 这里好像把开头的也带上了steals也带上了 # steals 15
    #         i+=1
    #         nodes[node_id]["puts"] = convert_disk_stats_to_array(lines[i]) # puts 16
    #         i+=1
    #         nodes[node_id]["waits-due-to-connection"] = convert_disk_stats_to_array(lines[i]) # wait-due-to-contention 17
    #         i+=1
    #         nodes[node_id]["gets"] = convert_disk_stats_to_array(lines[i]) # gets 18
    #         i+=1
    #         nodes[node_id]["hits"] = convert_disk_stats_to_array(lines[i]) # hits 19
    #         i+=1
    #         nodes[node_id]["scans"] = convert_disk_stats_to_array(lines[i]) # scans 20
    #         i+=1
    #         nodes[node_id]["file_per_miss"] = convert_disk_stats_to_array(lines[i]) # searched_file_per_miss 21
    #         i+=1
    #         nodes[node_id]["memtable_hist"] = convert_disk_stats_to_array(lines[i]) # memtable-hist 22
    #         i+=1
    #         nodes[node_id]["no_waits"] = convert_disk_stats_to_array(lines[i]) # puts-no-wait 23
    #         i+=1
    #         nodes[node_id]["puts-wait"] = convert_disk_stats_to_array(lines[i]) # puts-wait 24
    #         i+=1
    #         # nodes[node_id]["waits"] = waits-due-to-connection + puts-wait  
    #         # print lines[i+base]
    #         # 这里开始有问题 不确定改的对不对
    #         count = 0
    #         while True:
    #             if i >= len(lines) or lines[i] == "":
    #                 break

    #             if lines[i].split(",")[0] == "db":
    #                 ems = convert_disk_stats_to_array(lines[i])
    #                 i+=1
    #                 nodes[node_id]["db_size"].append(ems[1])
    #                 nodes[node_id]["num_l0_tables"].append(ems[2])
    #                 nodes[node_id]["total_memtable_size"] = convert_int(ems[3])
    #                 nodes[node_id]["written_memtable_size"] = convert_int(ems[4])
    #                 nodes[node_id]["total_disk_reads"] = convert_int(ems[5])
    #                 nodes[node_id]["total_disk_writes"] = convert_int(ems[6])
    #                 break
    #             else:
    #                 i+=1
    #         # i += 7 #????????????????????????????????????
    #         # print lines[i+base]
            
    #     except:
    #         # raise e
    #         break
        
    # print(nodes)