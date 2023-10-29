import os
from pprint import pprint
import sys
import os.path
import json

import time
import datetime
import pickle

# 字符串转int
def convert_int(val):
	try:
		return int(val)
	except:
		return 0

# 字符串转float
def convert_float(val):
	try:
		return float(val)
	except:
		return 0
# 找到中位数
def median(values):
	nozerovalues = []
	for v in values:
		# if v > 5:
		nozerovalues.append(v)
	sorted(nozerovalues)
	if len(nozerovalues) == 0:
		return -1
	return nozerovalues[int(len(nozerovalues) / 2)]

# 除法
def safe_divide(a, b):
	if convert_float(b) <= 0:
		return 0
	return float(a) * 100.0 / float(b)

# 只是返回平均值
def stats(data):
	if len(data) == 0:
		return "0"
	ma = max(data)
	mi = min(data)
	avg = 0
	for i in range(len(data)):
		avg += data[i]
	avg = avg / len(data)
	diff=ma-avg
	return "{}".format(avg)

# 将一行数据转化为float数组
def get_numbers(line):
	ems = line.split(" ")
	narray = []
	for em in ems:
		em = em.replace('\n', '')
		em = em.replace('\t', '')
		if em != '':
			narray.append(float(em))
	return narray

# 传入一个具体的实验的目录
# 返回关于coll文件的处理结果
# done
def parse_rdma_resource(result_dir):
	nodes={}
	for node_id in range(num_nodes):
		nodes[node_id] = {}
		nodes[node_id]["[IB]InGbps"] = []
		nodes[node_id]["[IB]OutGbps"] = []
		if read_resources_stats is False:
			continue

		if node_id not in print_resource_servers:
			continue
		try:
			file = open("{}/{}-coll.txt".format(result_dir, node_id), 'r') # modify name
		except:
			continue
		lines = file.readlines()
		if len(lines) == 0 or len(lines) <= 2:
			continue
		# line 1
		# #Date Time [CPU]User% [CPU]Nice% [CPU]Sys% [CPU]Wait% [CPU]Irq% [CPU]Soft% [CPU]Steal% [CPU]Idle% [CPU]Totl% [CPU]Guest% [CPU]GuestN% [CPU]Intrpt/sec [CPU]Ctx/sec [CPU]Proc/sec [CPU]ProcQue [CPU]ProcRun [CPU]L-Avg1 [CPU]L-Avg5 [CPU]L-Avg15 [CPU]RunTot [CPU]BlkTot [IB]InPkt [IB]OutPkt [IB]InKB [IB]OutKB [IB]Err
		headers = lines[1].split(" ")
		# #Date
		# Time
		# [CPU]User%
		# [CPU]Nice%
		# [CPU]Sys%
		# [CPU]Wait%
		# [CPU]Irq% 
		# [CPU]Soft%
		# [CPU]Steal%
		# [CPU]Idle%
		# [CPU]Totl%
		# [CPU]Guest%
		# [CPU]GuestN%
		# [CPU]Intrpt/sec
		# [CPU]Ctx/sec
		# [CPU]Proc/sec
		# [CPU]ProcQue
		# [CPU]ProcRun
		# [CPU]L-Avg1
		# [CPU]L-Avg5
		# [CPU]L-Avg15
		# [CPU]RunTot
		# [CPU]BlkTot
		# [IB]InPkt
		# [IB]OutPkt
		# [IB]InKB
		# [IB]OutKB
		# [IB]Err
		stats = {}
		values = []
		for i in range(len(headers)):
			values.append([])

		for i in range(len(lines)):
			if i <= 1:
				continue
			# 20231011 15:51:30 0 0 0 0 0 0 0 100 0 0 0 2221 4939 12 2214 2 0.70 0.74 0.57 0 0 0 0 1 0 0
			ems = lines[i].split(" ")
			for j in range(len(ems)):
				values[j].append(convert_int(ems[j])) # 这里转换不了的列就直接设为0

		for i in range(len(headers)):
			name = headers[i]
			# [IB]InKB
			# [IB]OutKB
			if "KB" in headers[i]: # KB转化为Gbps
				name = headers[i][:-2] + "Gbps"
				for j in range(len(values[i])):
					values[i][j] = float(values[i][j] * 1024  * 8) / (1000 * 1000 * 1000) #纠正算法
			stats[name] = values[i]
		if "[IB]InGbps" not in stats:
			stats["[IB]InGbps"] = []
		if "[IB]OutGbps" not in stats:
			stats["[IB]OutGbps"] = []
		nodes[node_id] = stats
	return nodes

# 去掉传入的东西中空的元素
def remove_empty(array):
	narray=[]
	for em in array:
		if em != '':
			narray.append(em)
	return narray

# 传入具体的实验结果的目录
# 返回关于网络的stats结果
# done
def parse_net_resource(result_dir):
	nodes={}
	nodeid_nic = {}
	# for node_id in range(num_nodes):
	# 	try:
	# 		file = open("{}/node-{}-nic.txt".format(result_dir, node_id), 'r')
	# 	except:
	# 		continue
	# 	for line in file.readlines():
	# 		if "enp" in line:
	# 			nodeid_nic[node_id] = line.split(" ")[0]
	# 			break
 
	# 修改nic名称，不确定对不对?
	for i in range(num_nodes):
		if i == 0 or i == 1:
			nodeid_nic[i] = "eno12399"
		elif i == 2:
			nodeid_nic[i] = "enp65s0f0"
		else:
			nodeid_nic[i] = "enp3s0f0"
	# nodeid_nic[1] = "enp3s0f0"
 
	
	for node_id in range(num_nodes):
		nodes[node_id] = {}
		nodes[node_id]["[NET]TxGbps"]=[]
		if read_resources_stats is False:
			continue

		if node_id not in print_resource_servers:
			continue
		try:
			file = open("{}/{}-net.txt".format(result_dir, node_id), 'r')
		except:
			continue
		# Linux 5.4.0-150-generic (ddst-PowerEdge-R750) 	2023年10月11日 	_x86_64_	(112 CPU)

		# 15时51分35秒     IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s   rxcmp/s   txcmp/s  rxmcst/s   %ifutil
		# 15时51分36秒        lo     50.00     50.00      3.36      3.36      0.00      0.00      0.00      0.00
		lines = file.readlines()
		nets=[]
		stats={}
		for line in lines:
			read_line=False
			# 这里 nodeid_nic[node_id]应该是???什么对应的接口呢
			if nodeid_nic[node_id] in line:
				read_line = True

			if read_line == True:
				netarray = remove_empty(line.split(" "))
				# 15时51分36秒
				# lo
				# 50.00
				# 50.00
				# 3.36
				# 3.36
				# 0.00
				# 0.00
				# 0.00
				# 0.00
				nets.append(convert_float(netarray[5]) * 1024 * 8 / (1000 * 1000 * 1000)) # 索引改为5， 且纠正算法
		name="[NET]TxGbps"
		stats[name]=nets
		nodes[node_id] = stats
	# print nodes
	return nodes

# 传入具体的实验的目录
# 传出disk的使用
# done
def parse_disk_resource(result_dir):
	nodes={}
	node_disk={}
	for i in range(num_nodes):
		if i == 0:
			node_disk[i] = "dev8-16"
		elif i == 1:
			node_disk[i] = "sdb"
		elif i == 2:
			node_disk[i] = "dev8-16" # ycsb端的无所谓
		else:
			node_disk[i] = "dev8-0" # 设备的编号... 这个是一定需要改 done
		# if i == 7:
		# 	node_disk[i] = "dev8-0"
		# else:
		# 	node_disk[i] = "dev8-16"
	for node_id in range(num_nodes):
		nodes[node_id] = {}
		nodes[node_id]["[DISK]Util"] = []

		if read_resources_stats is False:
			continue

		if node_id not in print_resource_servers:
			continue

		try:
			file = open("{}/{}-disk.txt".format(result_dir, node_id), 'r')
		except:
			continue
# Linux 5.4.0-150-generic (ddst-PowerEdge-R750) 	2023年10月11日 	_x86_64_	(112 CPU)

# 0                  1         2       3         4        5          6          7         8          9
# 15时51分43秒       DEV       tps     rkB/s     wkB/s   areq-sz    aqu-sz     await     svctm     %util
# 15时51分44秒    dev7-0      0.00      0.00      0.00      0.00      0.00      0.00      0.00      0.00
# 15时51分44秒    dev7-1      0.00      0.00      0.00      0.00      0.00      0.00      0.00      0.00
# 15时51分44秒    dev7-2      0.00      0.00      0.00      0.00      0.00      0.00      0.00      0.00
# 15时51分44秒    dev7-3      0.00      0.00      0.00      0.00      0.00      0.00      0.00      0.00
		lines = file.readlines()
		disks=[]
		stats={}
		for line in lines:
			read_line=False
			if node_disk[node_id] in line:
				read_line = True

			if read_line == True:
				diskarray = remove_empty(line.split(" "))
				if disk_metric == "iops":
					disks.append(convert_float(diskarray[2]))
				elif disk_metric == "bandwidth":
					disks.append((convert_float(diskarray[4])) / 1024) # 写的带宽 单位是MB/s
				elif disk_metric == "size":
					disks.append(convert_float(diskarray[5]) / 2) # 这里提取东西有点问题...再看看
				elif disk_metric == "queue":
					disks.append(convert_float(diskarray[6]))
				elif disk_metric == "read":
					disks.append(convert_float(diskarray[3])) #全部更新索引 + 纠正算法
		name="[DISK]Util"
		stats[name]=disks
		nodes[node_id] = stats
	# print nodes
	return nodes

# 传入具体结果的实验的目录
# 得到cpu的使用率
# done
def parse_cpu_resource(result_dir):
	nodes={}
	for node_id in range(num_nodes):
		ncores_current = ncores_new[node_id]
		nodes[node_id] = {}
		nodes[node_id]["CPU"] = []
		for j in range(ncores_current):
			name="CORE{}".format(j)
			nodes[node_id][name]=[]

		if read_resources_stats is False:
			continue

		if node_id not in print_resource_servers:
			continue

		try:
			file = open("{}/{}-cpu.txt".format(result_dir, node_id), 'r')
		except:
			continue
# Linux 5.4.0-150-generic (ddst-S2600WFQ) 	10/11/2023 	_x86_64_	(72 CPU)

# 03:53:00 PM     CPU     %user     %nice   %system   %iowait    %steal     %idle
# 03:53:01 PM     all      0.03      0.00      0.01      0.04      0.00     99.92
# 03:53:01 PM       0      0.00      0.00      0.00      0.00      0.00    100.00
# 03:53:01 PM       1      0.00      0.00      0.00      0.00      0.00    100.00
# 03:53:01 PM       2      0.00      0.00      0.00      0.00      0.00    100.00
# 03:53:01 PM       3      0.00      0.00      0.00      0.00      0.00    100.00
		lines = file.readlines()
		cpus={}
		stats={}
		cpus["all"]=[]
		for i in range(ncores_current):
			cpus["{}".format(i)]=[]
		i = 0
		while i < len(lines):
			line = lines[i]
			if "all" not in line: # 只从all的行开始处理
				i+=1
				continue
			cpuarray = remove_empty(line.split(" "))
			cpus["all"].append(100.0-convert_float(cpuarray[-1])) # 得到的是使用率
			for j in range(ncores_current):
				i+=1
				if i >= len(lines):
					break
				line=lines[i]
				cpuarray = remove_empty(line.split(" "))
				cpus["{}".format(j)].append(100.0-convert_float(cpuarray[-1])) # 每个cpu的使用率
		name="CPU"
		stats[name]=cpus["all"]
		for j in range(ncores_current):
			name="CORE{}".format(j)
			stats[name]=cpus["{}".format(j)]
		nodes[node_id] = stats
	return nodes

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

# 传入具体的实验目录
# 返回log record的相关统计
# 这个暂时不管
def parse_disk_stats(result_dir):
	nodes={}
	total_log_records=0
	for node_id in range(num_nodes):
		# if node_id != 0:
		# 	continue
		try:
			file = open("{}/server-{}-out".format(result_dir, node_id), 'r')
		except:
			continue

		nodes[node_id] = {}
		nodes[node_id]["storage"] = []
		nodes[node_id]["storage_reads"] = []
		nodes[node_id]["storage_writes"] = []
		nodes[node_id]["active_memtables"] = []
		nodes[node_id]["immutable_memtables"] = []
		nodes[node_id]["memtables"] = []
		nodes[node_id]["gets"] = []
		nodes[node_id]["hits"] = []
		nodes[node_id]["hit_rate"] = []
		nodes[node_id]["steals"] = []
		nodes[node_id]["puts"] = []
		nodes[node_id]["waits"] = []
		nodes[node_id]["wait_rate"] = []
		nodes[node_id]["no_wait_rate"] = []
		nodes[node_id]["memtable_hist"] = []
		nodes[node_id]["db_size"] = []
		nodes[node_id]["num_l0_tables"] = []
		nodes[node_id]["log_records"] = 0

		lines = file.readlines()
		i = 0
		while i < len(lines):
			line = lines[i]
			if "log records" in line: # 如果不开log的话 是没有这个记录的
				num_log_records=int(line.split(" ")[-3]) #
				nodes[node_id]["log_records"] += num_log_records
				total_log_records += num_log_records
				i+=1
				continue

			# 这里原来是干嘛的

			if "[stat_thread.cpp" not in line: #找到stats的起始打印
				i+=1
				continue

			try:
				# frdma 1
				# brdma 2
				# compaction 3
				# fg-storage 4
				# fg-storage-read 5
				# fg-storage-write 6
				# bg-storage 7
				# bg-storage-read 8
				# bg-storage-write 9
				# c-storage 10
				# c-storage-read 11
				# c-storage-write 12
				# active-memtables 13
				# immutable-memtables 14
				# steals 15
				# puts 16
				# wait-due-to-contention 17
				# gets 18
				# hits 19
				# scans 20
				# searched_file_per_miss 21
				# memtable-hist 22
				# puts-no-wait 23
				# puts-wait 24
    
    
    
				# db-overlapping-sstable-stats-0 25                               # db 25
				# db-size-stats-0 26
				# db-overlap-overall-0 27
				# db-overlap-0 28
				# db 29
				i+=4
				# 绕过stat_thread.cpp、以及前3个
				storage = sum_disk_stats(lines[i]) / 10 # fg-storage 4
				i+=1
				storage_reads = sum_disk_stats(lines[i]) / 1024 / 1024 / 10 # fg-storage-read 5
				i+=1
				storage_writes = sum_disk_stats(lines[i]) / 1024 / 1024 / 10 # fg-storage-write 6
				

				nodes[node_id]["storage"].append(storage)
				nodes[node_id]["storage_reads"].append(storage_reads)
				nodes[node_id]["storage_writes"].append(storage_writes)

				i+=7
				nodes[node_id]["active_memtables"].append(lines[i].replace("\n","")) # active-memtables 13
				i+=1
				nodes[node_id]["immutable_memtables"].append(lines[i].replace("\n","")) # immutable-memtables 14
				i+=1

				actives = nodes[node_id]["active_memtables"][-1].split(",")
				immutables = nodes[node_id]["immutable_memtables"][-1].split(",")
				all_mems = ""
				for j in range(len(actives)):
					all_mems += str(convert_int(actives[j]) + convert_int(immutables[j]))
					all_mems += ","
     
				nodes[node_id]["memtables"].append(all_mems)
				nodes[node_id]["steals"] = convert_disk_stats_to_array(lines[i]) # 这里好像把开头的也带上了steals也带上了 # steals 15
				i+=1
				nodes[node_id]["puts"] = convert_disk_stats_to_array(lines[i]) # puts 16
				i+=1
				nodes[node_id]["waits-due-to-connection"] = convert_disk_stats_to_array(lines[i]) # wait-due-to-contention 17
				i+=1
				nodes[node_id]["gets"] = convert_disk_stats_to_array(lines[i]) # gets 18
				i+=1
				nodes[node_id]["hits"] = convert_disk_stats_to_array(lines[i]) # hits 19
				i+=1
				nodes[node_id]["scans"] = convert_disk_stats_to_array(lines[i]) # scans 20
				i+=1
				nodes[node_id]["file_per_miss"] = convert_disk_stats_to_array(lines[i]) # searched_file_per_miss 21
				i+=1
				nodes[node_id]["memtable_hist"] = convert_disk_stats_to_array(lines[i]) # memtable-hist 22
				i+=1
				nodes[node_id]["no_waits"] = convert_disk_stats_to_array(lines[i]) # puts-no-wait 23
				i+=1
				nodes[node_id]["waits"] = convert_disk_stats_to_array(lines[i]) # puts-wait 24
				i+=1
				# nodes[node_id]["waits"] = waits-due-to-connection + puts-wait  
				# print lines[i+base]
				# 这里开始有问题 不确定改的对不对
				count = 0
				while True:
					if i >= len(lines) or lines[i] == "":
						break
  
					if lines[i].split(",")[0] == "db":
						ems = convert_disk_stats_to_array(lines[i])
						i+=1
						nodes[node_id]["db_size"].append(ems[1])
						nodes[node_id]["num_l0_tables"].append(ems[2])
						nodes[node_id]["total_memtable_size"] = convert_int(ems[3])
						nodes[node_id]["written_memtable_size"] = convert_int(ems[4])
						nodes[node_id]["total_disk_reads"] = convert_int(ems[5])
						nodes[node_id]["total_disk_writes"] = convert_int(ems[6])
						break
					else:
						i+=1
				# i += 7 #????????????????????????????????????
				# print lines[i+base]
				
			except:
				# raise e
				break
			

	total_gets=0
	total_hits=0
	for node_id in nodes:
		for i in range(len(nodes[node_id]["gets"])):
			total_gets+=convert_int(nodes[node_id]["gets"][i])
			total_hits+=convert_int(nodes[node_id]["hits"][i])
			nodes[node_id]["hit_rate"].append(safe_divide(nodes[node_id]["hits"][i], nodes[node_id]["gets"][i]))
		for i in range(len(nodes[node_id]["puts"])):
			nodes[node_id]["wait_rate"].append(safe_divide(nodes[node_id]["waits"][i], nodes[node_id]["puts"][i]))
			nodes[node_id]["no_wait_rate"].append(safe_divide(nodes[node_id]["no_waits"][i], nodes[node_id]["puts"][i]))

	if print_db_stats:
		for metric in ["db_size", "num_l0_tables"]:
		# for metric in ["db_size", "num_l0_tables", "storage_reads", "storage_writes", "storage", "steals", "hit_rate", "no_wait_rate", "memtable_hist"]:
			# print(metric)
			for node_id in range(num_nodes):
				if node_id not in nodes:
					continue
				# out = str(node_id)
				out = ""
				for i in range(len(nodes[node_id][metric])):
					out += str(nodes[node_id][metric][i])
					out += ","
				# print(out)

		for metric in ["immutable_memtables"]:
			# print(metric)
			for node_id in range(num_nodes):
				if node_id not in nodes:
					continue
				# print(node_id)
				
				out=""
				for i in range(len(nodes[node_id][metric])):
					out += str(i)
					out += ","
				out += "\n"

				for i in range(len(nodes[node_id][metric])):
					out += str(nodes[node_id][metric][i])
					out += "\n"
				# print(out)
	# print(nodes)
	# hit_rate=0
	return nodes, safe_divide(total_hits, total_gets), total_log_records 

def parse_date(line):
	ems = line.split(".")
	seconds= time.mktime(datetime.datetime.strptime(ems[0], "%Y/%m/%d-%H:%M:%S").timetuple())
	return seconds * 1000000 + int(ems[1])

# 返回记录开始的时间, 只有秒数
def report_starttime(result_dir):
	for client_id in range(num_nodes):
		try:
			file = open("{}/client-{}-out".format(result_dir, client_id), 'r')
		except:
			continue
		lines = file.readlines()
		for line in lines: 
			if "current ops/sec" in line:
				ems = line.split(" ")
				start_time="{}-{}".format(ems[0], ems[1][:8])
				start_time=time.mktime(datetime.datetime.strptime(start_time, "%Y-%m-%d-%H:%M:%S").timetuple())
				break
		return start_time * 1000000

# 
def report_leveldb_waittime(result_dir):
	sum_wait=0
	num_wait=0
	for sid in range(13):
		for i in range(128):
			fname="{}/server-{}-dblogs/LOG-{}".format(result_dir, sid, i)
			try:
				file = open(fname, 'r')
			except:
				continue
			lines = file.readlines()
			
			tid=0
			threads_wait_time = {}
			for line in lines:
				if "Make room waiting.." in line:
					ems = line.replace('\n', '').split(" ")
					start=parse_date(ems[0])
					# tid=ems[-1].split("-")[3]
					if tid in threads_wait_time:
						continue
						# print "!!!!WRONG {} {}-{}".format(line, sid, i)
					threads_wait_time[tid]=start
					num_wait += 1
				elif "Make room; resuming..." in line:
					ems = line.replace('\n', '').split(" ")
					end=parse_date(ems[0])
					# tid=ems[-1].split("-")[3]
					sum_wait += (end - threads_wait_time[tid])
					del threads_wait_time[tid]
	return num_wait, sum_wait/1000000


def report_major_compaction_time(logname):
	sum_wait=0
	num_wait=0
	fname=logname
	try:
		file = open(logname, 'r')
	except:
		return
	lines = file.readlines()
	
	tid=0
	threads_wait_time = {}
	tuples = []
	stats = {}
	for line in lines:
		if "Major Compacting " in line:
			ems = line.replace('\n', '').split(" ")
			start=parse_date(ems[0])
			stats["start"] = start
		elif "Major Compacted " in line:
			ems = line.replace('\n', '').split(" ")
			end=parse_date(ems[0])
			stats["duration"] = (end - stats["start"])
			
			stats["output"] = int(ems[-2]) / 1024 / 1024
			stats["files"] = int(ems[-5].split("@")[0]) + int(ems[-7].split("@")[0])
			# print ("{},{},{},{}".format(stats["files"],stats["files"]*16, stats["output"], stats["duration"]/1000000))

	# return num_wait, sum_wait/1000000

def tid_string(ems):
	tt = ems[-1].split("-")
	if len(tt) > 2:
		return tt[3]
	else:
		return tt[1]

# 具体的实验目录，用于分析server的LOG，但是对应的LOG里面没有, 有待查明原因，应该是10G1server的内存空间够大??
def report_waittime(result_dir):
	sum_wait=0
	num_wait=0
	for sid in range(7): #server的id
		for i in range(128): #数据库db的id??
			fname="{}/server-{}-dblogs/LOG-{}".format(result_dir, sid, i)
			try:
				file = open(fname, 'r')
			except:
				continue
			lines = file.readlines()
			for line in lines:
				if "Load complete" in line:
					num_wait = 0
					sum_wait = 0
					continue
				if "Flushing" in line:
					continue
				ems = line.split(" ")[2]
				ems = ems.split(",")
				if len(ems) != 2:
					continue
				if convert_int(ems[1]) == 0:
					continue
				num_wait += 1
				sum_wait += int(ems[1])
	return num_wait, sum_wait/1000000

# 计算占用的磁盘空间
def report_diskspace(result_dir):
	sum_disk_space=0
	for sid in range(7):
		for file in ["{}/server-{}-db-disk-space", "{}/server-{}-rtable-disk-space"]:
			fname=file.format(result_dir, sid)
			try:
				file = open(fname, 'r')
			except:
				continue
			lines = file.readlines()
			try:
				space = int(lines[0].split("\t")[0])
				sum_disk_space += space
			except:
				# print ("!!!!!!!!!!!!!{}".format(result_dir))
				continue
	return sum_disk_space / 1024

# 处理一个实验的目录(具体的实验) nova-tutorial-10000000/nova-zf-0.99...
# 返回各种操作的延迟，总吞吐量，测试节点的performance信息，写暂停时间比率
def parse_performance(result_dir):
	throughput = 0
	# print (result_dir)
	nodes={}
	overall_latencies={}
	overall_latencies["read"]={}
	overall_latencies["write"]={}
	stalls = []

	for op in overall_latencies:
		overall_latencies[op]["avg"] = ""
		overall_latencies[op]["p95"] = ""
		overall_latencies[op]["p99"] = ""

# 遍历所有的machine
	for node_id in range(num_nodes):
		stall = 0
		num_duration = 0
# 遍历所有的client???client最多开16个??
		for client_id in range(16):
			performance = {}
			performance["thpt"] = []
			performance["Max"] = []
			performance["Min"] = []
			performance["Avg"] = []
			performance["p90"] = []
			performance["p99"] = []
			performance["p999"] = []
			performance["p9999"] = []
			
			try:
				file = open("{}/client-{}-{}-out".format(result_dir, node_id, client_id), 'r') # 这里的client端输出要改一下
			except:
				continue
			# 读进来
			lines = file.readlines()

			overall_thpt = 0
			basethpt = 0
			for line in lines:
				latency=0
				# 关于latency的行，提取出来，整理格式
				if "Latency(us)" in line:
					ems=line.split(",")
					latency=ems[2].replace(" ", "")
					latency=latency.replace("\n","")
				# 根据不同类型的操作进行读写的修改
				if "[READ], AverageLatency(us)," in line:
					overall_latencies["read"]["avg"] = latency
				elif "[READ], 95thPercentileLatency(us)," in line:
					overall_latencies["read"]["p95"] = latency
				elif "[READ], 99thPercentileLatency(us)," in line:
					overall_latencies["read"]["p99"] = latency
				elif "[UPDATE], AverageLatency(us)," in line:
					overall_latencies["write"]["avg"] = latency
				elif "[UPDATE], 95thPercentileLatency(us)," in line:
					overall_latencies["write"]["p95"] = latency
				elif "[UPDATE], 99thPercentileLatency(us)," in line:
					overall_latencies["write"]["p99"] = latency
				elif "[SCAN], AverageLatency(us)," in line:
					overall_latencies["read"]["avg"] = latency
				elif "[SCAN], 95thPercentileLatency(us)," in line:
					overall_latencies["read"]["p95"] = latency
				elif "[SCAN], 99thPercentileLatency(us)," in line:
					overall_latencies["read"]["p99"] = latency
				
    			# 这个是关于总结的行
				if "[OVERALL], Throughput(ops/sec)," in line:
					ems = line.split(",")
					# overall_thpt = ems[2].replace(" ", "")
					# overall_thpt = overall_thpt.replace("\n", "")
				# 每一秒都有一个这个行
				if "current ops/sec" in line:
					# 2019-12-29 08:53:01:106 22 sec: 1050634 operations; 50204 current ops/sec; [READ: Count=25128, Max=23647, Min=63, Avg=465.62, 90=679, 99=2141, 99.9=5963, 99.99=12983] [UPDATE: Count=25063, Max=66431, Mi
					# n=362, Avg=9757.04, 90=19951, 99=29807, 99.9=40479, 99.99=56991]
					# if start_time == "":
					# 	ems = line.split(" ")
					# 	start_time="{}-{}".format(ems[0], ems[1][:8])
					# 	start_time=time.mktime(datetime.datetime.strptime(start_time, "%Y-%m-%d-%H:%M:%S").timetuple())
					# 	start_time=start_time * 1000000

					ems = line.split(";")
					# 0: 2019-12-29 08:53:01:106 22 sec: 1050634 operations
					# 1:  50204 current ops/sec
					# 2: [UPDATE: Count=62852, Max=256895, Min=180, Avg=3207.75, 90=4735, 99=28975, 99.9=93375, 99.99=220671] 
					latencies = ems[2].split(",")
					# 0: [UPDATE: Count=62852
					# 1:  Max=256895
					# 2:  Min=180
					# 3:  Avg=3207.75
					# 4:  90=4735
					# 5:  99=28975
					# 6:  99.9=93375
					# 7:  99.99=220671
					thp = ems[1] # 50204 current ops/sec
					thp=thp.replace("current ops/sec", "") # 50204
					thp=convert_float(thp.replace(" ", "")) # 提取ops/sec
					performance["thpt"].append(thp)
					othp = ems[0].split(" ") 
					# 0: 2019-12-29
					# 1: 08:53:01:106
					# 2: 22
					# 3: sec:
					# 4: 1050634
					# 5: operations
					num_duration += 1
					if thp == 0: # thp = 0 说明出现了写暂停
						stall += 1
					try:
						duration = float(othp[2])
						ops = float(othp[4])
						if duration == 10: # 设定10s时的吞吐量为基本吞吐量
							basethpt = ops
						if duration == 	550: # 然后隔了540s做了一个平均
							overall_thpt = (ops-basethpt) / 540
					except:
						# print (line) #????
						continue
					
					# 这里
					for i in range(len(latencies)):
						if "READ" not in latencies[i]: # 
							continue
						performance["Max"].append(convert_float(latencies[i+1].split("=")[1]))
						performance["Min"].append(convert_float(latencies[i+2].split("=")[1]))
						performance["Avg"].append(convert_float(latencies[i+3].split("=")[1]))
						performance["p90"].append(convert_float(latencies[i+4].split("=")[1]))
						performance["p99"].append(convert_float(latencies[i+5].split("=")[1]))
						performance["p999"].append(convert_float(latencies[i+6].split("=")[1]))
						performance["p9999"].append(convert_float(latencies[i+7].split("]")[0].split("=")[1]))
			throughput += float(overall_thpt)
			if node_id not in nodes:
				nodes[node_id] = {}
			nodes[node_id][client_id] = performance
			stalls.append(safe_divide(stall, num_duration))
			# print (node_id, client_id, overall_thpt, overall_latencies, safe_divide(stall, num_duration))
	return overall_latencies, throughput, nodes, median(stalls)

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
		elif ems[i+1] == "workloade":
			value="SW50"
		elif ems[i+1] == "lc":
			value="LTC"
		elif ems[i+1] == "sc":
			value="StoC"
		exp_params[ems[i]] = value
		expname += value
		expname += ","
		i += 2

	expname=expname[:-1]
	return expname

def print_stats(exps, exp, all_attrs):
	header = ","
	for attr in all_attrs:
		header += attr + ","
		header += "stdev,"
	output=exp + " " + str(exps[exp]["thpt"]) +"\n"
	output+=header
	output+="\n"
	for i in range(num_servers):
		out = "node-{},".format(i)
		for attr in all_attrs:
			if "NET" in attr:
				out += str(exps[exp]["net"][i][attr])
			elif "DISK" in attr:
				out += str(exps[exp]["disk"][i][attr])
			elif "CPU0" in attr:
				cores=0
				for j in range(8):
					cores+=exps[exp]["cpu"][i]["CORE{}".format(j)]
				for j in range(16, 24):
					cores+=exps[exp]["cpu"][i]["CORE{}".format(j)]
				cores /= 16
				out += str(cores)
			elif "CPU1" in attr:
				cores=0
				for j in range(8,16):
					cores+=exps[exp]["cpu"][i]["CORE{}".format(j)]
				for j in range(24, 31):
					cores+=exps[exp]["cpu"][i]["CORE{}".format(j)]
				cores /= 16
				out += str(cores)
			elif "CPU" in attr:
				out += str(exps[exp]["cpu"][i][attr])
			else:
				out += str(exps[exp]["coll"][i][attr])
			out += ","
		output+=out
		output+="\n"
	return output

def concat_timeline(timelines):
	line = ""
	for i in range(len(timelines)):
		line += str(timelines[i])
		line += ","
	line += "\n"
	return line

#处理存放结果的目录, nova-tutorial-10000000
def parse_exp(exp_dir):
	exps={}
	median_exps={}
	for expdirname in os.listdir(exp_dir):
		if "nova" not in expdirname:
			continue
		# if "-np-4-" not in expdirname:
		# 	continue
		# if "0.00-" not in expdirname:
		# 	continue
		# if "nova-d-zipfian-w-workloada-ltc-10-stoc-0-l0-10240-np-64-mp-64-log-none-p-3-c-10" not in expdirname:
		# 	continue
		# if "ltc-3" not in expdirname:
		# 	continue
		# if "stoc-9-l0-160" not in expdirname:
		# 	continue

		# if "try-2-" not in expdirname:
		# 	continue
		
		# num_wait = 0
		result_dir = exp_dir + "/" + expdirname # 找到一个存放一个实验结束之后的目录, nova-tutorial-10000000/nova-zf-0.99 ....
		latencies, thpt, performance, stall_time = parse_performance(result_dir) # performance相关数据 来自于client out done
		num_wait, wait_time = report_waittime(result_dir) # 分析来自server的log，获取等待时间等数据 暂时还没用到 不开log选项不会产生这个

		rdma_resources = parse_rdma_resource(result_dir) # 分析来自server的coll.txt，获取关于rdma等的数据 done
		net_resources = parse_net_resource(result_dir)  # 分析来自server的net.txt，获取关于网络的数据 done
		cpu_resources = parse_cpu_resource(result_dir) # 分析来自server的cpu.txt，获取关于cpu的数据 done

		disk_resources = parse_disk_resource(result_dir) # 分析来自server的disk.txt，获取关于disk的数据 done
		disk_spaces, hit_rate, total_log_records = parse_disk_stats(result_dir) # 分析来自server的server- -out数据，获得关于log record的数据 现在是什么也没有 填补好了done
		disk_space_timeline = []
		disk_space = 0
		num_l0_tables = 0
		total_memtable_size = 0
		written_memtable_size = 0
		total_disk_reads = 0
		total_disk_writes = 0

		# 关于磁盘占用空间的统计
		if len(disk_spaces) > 0 and len(disk_spaces[0]["db_size"]) > 0:
			# print disk_spaces
			for node_id in disk_spaces:
				disk_space += int(disk_spaces[node_id]["db_size"][-1])
				num_l0_tables += int(disk_spaces[node_id]["num_l0_tables"][-1])
				total_memtable_size += int(disk_spaces[node_id]["total_memtable_size"])
				written_memtable_size += int(disk_spaces[node_id]["written_memtable_size"])
				total_disk_reads += int(disk_spaces[node_id]["total_disk_reads"])
				total_disk_writes += int(disk_spaces[node_id]["total_disk_writes"])

			db_length = 1e6
			for node_id in disk_spaces:
				db_length = min(db_length, len(disk_spaces[node_id]["db_size"]))
    
			for i in range(db_length):
				ds = 0
				for node_id in disk_spaces:
					ds += int(disk_spaces[node_id]["db_size"][len(disk_spaces[node_id]["db_size"]) - db_length + i])
				disk_space_timeline.append(ds)
   
			# for i in range(120): # 这是什么玩意 这个问题比较大 
			# 	ds = 0
			# 	for node_id in disk_spaces:
			# 		ds += int(disk_spaces[node_id]["db_size"][node_id])
			# 	disk_space_timeline.append(ds)


		# 关于吞吐量的统计
		thpt_timeline = []
		peak_thpt = 0
		for metric in ["thpt"]:
			exp_time = 0
			for node_id in sorted(performance):
				for client_id in sorted(performance[node_id]):
					exp_time = max(exp_time, len(performance[node_id][client_id][metric]))#ycsb客户端运行的最长时间

			for i in range(min(exp_time, 15000)):
				t = 0
				for node_id in sorted(performance):
					for client_id in sorted(performance[node_id]):
						if i < len(performance[node_id][client_id][metric]):
							t += convert_float(performance[node_id][client_id][metric][i])
				if t > peak_thpt:
					peak_thpt = t
				thpt_timeline.append(t)
		
		exp=expname(expdirname) #读出实验的配置字符串作为索引
		if exp not in exps:
			exps[exp] = {}
			exps[exp]["thpt"] = []
			exps[exp]["coll"] = {}
			exps[exp]["net"] = {}
			exps[exp]["cpu"] = {}
			exps[exp]["disk"] = {}
			for node_id in range(num_nodes):
				exps[exp]["cpu"][node_id]={}
				exps[exp]["cpu"][node_id]["CPU"] = 0
				for j in range(ncores):
					exps[exp]["cpu"][node_id]["CORE{}".format(j)] = 0
			# net
			for node_id in range(num_nodes):
				exps[exp]["net"][node_id]={}
				exps[exp]["net"][node_id]["[NET]TxGbps"] = 0
			# disk
			for node_id in range(num_nodes):
				exps[exp]["disk"][node_id]={}
				exps[exp]["disk"][node_id]["[DISK]Util"] = 0
			# rdma
			for node_id in range(num_nodes):
				exps[exp]["coll"][node_id]={}
				exps[exp]["coll"][node_id]["[IB]InGbps"] = 0
				exps[exp]["coll"][node_id]["[IB]OutGbps"] = 0

		exps[exp]["wait_time"] = wait_time
		exps[exp]["nwait"] = num_wait
		exps[exp]["stall_time"] = stall_time
		exps[exp]["disk_space"] = disk_space
		exps[exp]["num_l0_tables"] = num_l0_tables
		exps[exp]["total_memtable_size"] = total_memtable_size
		exps[exp]["written_memtable_size"] = written_memtable_size
		exps[exp]["memtable_size_reduction"] = safe_divide(total_memtable_size - written_memtable_size, total_memtable_size)
		exps[exp]["total_disk_reads"] = total_disk_reads
		exps[exp]["total_disk_writes"] = total_disk_writes

		exps[exp]["disk_space_timeline"] = disk_space_timeline
		exps[exp]["thpt"] = thpt
		exps[exp]["latencies"] = latencies
		exps[exp]["thpt_timeline"] = thpt_timeline
		exps[exp]["cpu_timeline"] =  {} 
		exps[exp]["net_timeline"] = {}
		exps[exp]["disk_timeline"] = {}
		exps[exp]["rdma_timeline"] = {}
		exps[exp]["hit_rate"] = hit_rate
		exps[exp]["total_log_records"] = total_log_records
		for node_id in cpu_resources:
			exps[exp]["cpu_timeline"][node_id] = cpu_resources[node_id]["CPU"]
		for node_id in cpu_resources:
			exps[exp]["net_timeline"][node_id] = net_resources[node_id]["[NET]TxGbps"]
		for node_id in cpu_resources:
			exps[exp]["disk_timeline"][node_id] = disk_resources[node_id]["[DISK]Util"]
		for node_id in cpu_resources:
			exps[exp]["rdma_timeline"][node_id] = rdma_resources[node_id]["[IB]OutGbps"]

		exps[exp]["peak_thpt"] = peak_thpt
		# CPU
		for node_id in range(num_nodes):
			exps[exp]["cpu"][node_id]["CPU"] = median(cpu_resources[node_id]["CPU"])
			for j in range(ncores_new[node_id]):
				exps[exp]["cpu"][node_id]["CORE{}".format(j)] = median(cpu_resources[node_id]["CORE{}".format(j)])
		# net
		for node_id in range(num_nodes):
			exps[exp]["net"][node_id]["[NET]TxGbps"] = median(net_resources[node_id]["[NET]TxGbps"])
		for node_id in range(num_nodes):
			exps[exp]["disk"][node_id]["[DISK]Util"] = median(disk_resources[node_id]["[DISK]Util"])
		# rdma
		for node_id in range(num_nodes):
			exps[exp]["coll"][node_id]["[IB]InGbps"] = median(rdma_resources[node_id]["[IB]InGbps"])
			exps[exp]["coll"][node_id]["[IB]OutGbps"] = median(rdma_resources[node_id]["[IB]OutGbps"])

	# for exp in exps:
	# 	print (exp, exps[exp]["thpt"], exps[exp]["total_log_records"], exps[exp]["disk_space"], exps[exp]["hit_rate"], exps[exp]["nwait"], exps[exp]["wait_time"])
	return exps

# 打印所有统计
def print_all(exps):
	header=""
	print (params)
	for p in params:
		header+=param_dict[p]
		header+=","
	for r in print_resources:
		header+=str(r)
		header+=","
	header+="Fetched log records,Number of waits,Wait duration,Stall duration,number of L0 SSTables,database size (MB),memtable hit rate,Total MemTable Size,Written MemTable Size,Reduction,Total Disk Reads,Total Disk Writes,Average throughput,peak_thpt,read_avg,read_p95,read_p99,write_avg,write_p95,write_p99,"
	for resource in print_resources:
		for node_id in print_resource_servers:
			header+="{}-{}".format(resource, node_id)
			header+=","	
	
	# print header
	summary=header
	summary+="\n"
	for exp in exps:
		out="{},".format(exp)
		header = out.replace(",", "-")
		node_resource_timelines={}
		thpt_timelines =  "Thpt,{}".format(header)
		thpt_timelines += ","

		exp_time=len(exps[exp]["thpt_timeline"])
		thpt_timelines += concat_timeline(exps[exp]["thpt_timeline"])

		resource_nodes={}
		if read_resources_stats:
			for resource in print_resources:
				resource_nodes[resource] = {}
				if resource not in node_resource_timelines:
					node_resource_timelines[resource] = {}
					resource_nodes[resource]["SUM"] = 0
					node_resource_timelines["avg_{}".format(resource)] = {}
				
				for node_id in exps[exp]["{}_timeline".format(resource)]:
					if node_id in print_resource_servers:
						timeline = exps[exp]["{}_timeline".format(resource)][node_id][-exp_time-10:-10]

						node_resource_timelines[resource][node_id] = "{}-{},".format(resource, node_id)
						node_resource_timelines[resource][node_id] += concat_timeline(timeline)
						node_resource_timelines["avg_{}".format(resource)][node_id] = stats(timeline[int(len(timeline)/2):])
						resource_nodes[resource][node_id] = float(node_resource_timelines["avg_{}".format(resource)][node_id])
						resource_nodes[resource]["SUM"] += resource_nodes[resource][node_id]
		
		if read_resources_stats:
			for resource in print_resources:
				out += str(resource_nodes[resource]["SUM"])
				out += ","

		out+=str(exps[exp]["total_log_records"])
		out+=","
		if float(exp_time) == 0:
			out += "0,0,0"
		else:
			out+=str(float(exps[exp]["nwait"]))
			out+=","
			out+=str(float(exps[exp]["wait_time"]))
			out+=","
			out+=str(float(exps[exp]["stall_time"]))

		out+=","
		out+=str(exps[exp]["num_l0_tables"])
		out+=","
		out+=str(exps[exp]["disk_space"])
		out+=","
		out+=str(exps[exp]["hit_rate"])
		out+=","
		out+=str(exps[exp]["total_memtable_size"])
		out+=","
		out+=str(exps[exp]["written_memtable_size"])
		out+=","
		out+=str(exps[exp]["memtable_size_reduction"])
		out+=","
		out+=str(float(exps[exp]["total_disk_reads"]) / 1024 / 1024 / 1024)
		out+=","
		out+=str(float(exps[exp]["total_disk_writes"]) / 1024 / 1024 / 1024)
		out+=","
		out+=str(exps[exp]["thpt"])
		out+=","
		out+=str(exps[exp]["peak_thpt"])
		out+=","
		out+=str(exps[exp]["latencies"]["read"]["avg"])
		out+=","
		out+=str(exps[exp]["latencies"]["read"]["p95"])
		out+=","
		out+=str(exps[exp]["latencies"]["read"]["p99"])
		out+=","
		out+=str(exps[exp]["latencies"]["write"]["avg"])
		out+=","
		out+=str(exps[exp]["latencies"]["write"]["p95"])
		out+=","
		out+=str(exps[exp]["latencies"]["write"]["p99"])
		out+=","
		if read_resources_stats:
			for resource in print_resources:
				for node_id in print_resource_servers:
					out += str(resource_nodes[resource][node_id])
					out += ","
		summary += out
		summary += "\n"

		if print_thpt_timeline:
			print (thpt_timelines)

		if print_resources_stats:
			for resource in print_resources:
				for node_id in node_resource_timelines["avg_{}".format(resource)]:
					ag = node_resource_timelines["avg_{}".format(resource)][node_id]
					print ("avg-{}-{},{}".format(resource, node_id, ag))

			for resource in print_resources:
				for node_id in node_resource_timelines[resource]:
					print (node_resource_timelines[resource][node_id])
	print (summary)

def print_in_format(exps, number_of_tab):
	if type(exps) == dict:
		for exp in exps:
			print("\t" * number_of_tab + str(exp))
			print_in_format(exps[exp], number_of_tab + 1)
	elif type (exps) == list:
		print("\t" * number_of_tab + str(exps))
	else:
		print("\t" * number_of_tab + str(exps))
	
     




# 各种参数的定义字典
param_dict={}
param_dict["d"]="Distribution"
param_dict["w"]="Workload"
param_dict["nm"]="Number of memtables"
param_dict["np"]="Number of memtable partitions"
param_dict["nr"]="Number of ranges"
param_dict["mc"]="Major compaction type"
param_dict["mp"]="Major compaction parallelism"
param_dict["ms"]="Major compaction # of tables per set"
param_dict["fm"]="Enable pruning memtables before flushing"
param_dict["s"]="Enable subrange"
param_dict["sn"]="Subrange: Number of values without flushing"
param_dict["l0"]="L0 size (GB)"
param_dict["zf"]="Zipfian constant"
param_dict["ltc"]="Number of LTCs"
param_dict["stoc"]="Number of StoCs"
param_dict["log"]="Log Method"
param_dict["ss"]="SSTable size"
param_dict["sp"]="Scatter policy"
param_dict["p"]="Scatter factor"
param_dict["nc"]="Number of clients"
param_dict["c"]="Cardinality"
param_dict["sr"]="SSTable replicas"
param_dict["f"]="failure duration"
param_dict["el"]="Enable Lookup index"
param_dict["try"]="Try"
param_dict["cfg"]="Migration"
param_dict["lr"]="Number of log record replicas"
param_dict["sub"]="Max sub compactions"
param_dict["l"]="Level"
param_dict["nc"]="Number of compaction threads"
param_dict["sr"]="Size ratio"
param_dict["nf"]="No Flush"
param_dict["of"]="Ordered Flush"

ncores = 112 # server的 cpu数量 应该改为112??
ncores_new = [112, 112, 72]
disk_metric="bandwidth"
# disk_metric="read"
# print_resource_servers=[0, 1, 2, 4]
# print_resource_servers=[0, 1, 2, 3, 4, 5]
# print_resource_servers=[0]
# print_resources=["cpu", "net", "disk", "rdma"]
# print_resource_servers=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
# print_resources=["disk"]
print_resources=["cpu", "net", "disk", "rdma"]
print_resource_servers=[0, 1, 2] # 要分析资源使用的服务器节点编号

read_resources_stats=True # 是否分析各种资源的使用stats
print_thpt_timeline=True # 是否打印出吞吐量时间变化
print_resources_stats=False # 是否打印各种资源的stats
print_db_stats=False # 是否打印数据库的stats

read_resources_stats=True
print_thpt_timeline=True
print_resources_stats=True
print_db_stats=True


num_nodes=int(sys.argv[1]) # 机器的数量 包括ltc stoc client
exp_dir=sys.argv[2] # 一组实验的目录
# debug=sys.argv[3] # 是否采用debug模式
exps=parse_exp(exp_dir)
with open(exp_dir + "/results.pickle", 'wb') as f:
    pickle.dump(exps, f)
# print_in_format(exps, 0)
# print(type(exps))
# print(type([1, 2, 3]))
# print_all(exps)

# report_major_compaction_time("/proj/bg-PG0/haoyu/nova/LOG-0")
