import paramiko
import optparse
import time
import random
import os
import sys
# from parser import Parser

class SSHInfo(object):
    def __init__(self, hostname, port, username, password):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
    
    def getIp(self):
        return self.hostname  
    
    def getSSHstylestring(self):
        return self.username + "@" + self.hostname 

def prepareSSHinfos():
    sshInfos = []
    sshInfos.append(SSHInfo("192.168.98.74", 22, "yuhang", "1111"))
    sshInfos.append(SSHInfo("192.168.98.70", 22, "yuhang", "zhangyu111"))
    sshInfos.append(SSHInfo("192.168.98.53", 22, "yuhang", "1111"))
    return sshInfos

class SSHProxy(object):
    def __init__(self, hostname, port, username, password):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        # self.transport = None
    
    def __init__(self, sshInfo):
        self.hostname = sshInfo.hostname
        self.port = sshInfo.port
        self.username = sshInfo.username
        self.password = sshInfo.password
        # self.transport = None

    # def open(self):  # 给对象赋值一个上传下载文件对象连接
    #     self.transport = paramiko.Transport((self.hostname, self.port))
    #     self.transport.connect(username=self.username, password=self.password)

    # background job记得重定向!
    def command(self, cmd):  # 正常执行命令的连接  至此对象内容就既有执行命令的连接又有上传下载链接
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=self.hostname, port=self.port, username=self.username, password=self.password)
        # ssh._transport = self.transport
        stdin, stdout, stderr = ssh.exec_command(cmd)  
        
        
        result = stdout.read().decode('utf-8')
        return result

    # def upload(self, local_path, remote_path):
    #     sftp = paramiko.SFTPClient.from_transport(self.transport)
    #     sftp.put(local_path, remote_path)
    #     sftp.close()

    # def close(self):
    #     self.transport.close()

    # def __enter__(self):  # 对象执行with上下文会自动触发
    #     # 
    #     # print('触发了enter')
    #     self.open()
    #     return self  # 这里发挥上面with语法内的as后面拿到的就是什么
    #     # return 123

    def getNode(self):
        return self.hostname

    def getSSHstylestring(self):
        return self.username + "@" + self.hostname 

    def __exit__(self, exc_type, exc_val, exc_tb):  # with执行结束自动触发
        # print('触发了exit')
        self.close()

def prepareSSHproxies(sshInfos):
    sshProxies = []
    for sshInfo in sshInfos:
        sshProxies.append(SSHProxy(sshInfo))
    return sshProxies

def seq(num):
    res = []
    i = 1
    while i <= num:
        res.append(i)
        i = i + 1
    return res

class Runner(object):
    def __init__(self, \
        ):
        self.home_dir = "/home/yuhang/NovaLSM"
        self.config_dir = self.home_dir + "/config"
        self.db_dir = self.home_dir + "/db"
        self.script_dir = self.home_dir + "/scripts"
        self.cache_bin_dir = self.home_dir + "/nova"
        self.client_bin_dir = self.home_dir + "/YCSB-Nova"
        self.results = "/tmp/results"
        self.recordcount = 10000000
        self.exp_results_dir = self.home_dir + "/nova-experiment-" + time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        self.mydebug = False
        
        self.nmachines = 3 # 5 machines in total
        self.nservers = 2 # 4 servers function(52 serve as client)
        self.number_of_ltcs = 1 # duplicate 2 servers as ltc 这个参数没有作用,nova_server_main里面不会使用
        self.nclients = 1 # 1 client

        # YCSB
        self.maxexecutiontime=1200
        self.workload = "workloada" # TBD
        self.nthreads = 512
        self.debug = "false"
        self.dist = "uniform"
        self.cardinality = 10 # 这里有点区别，应该是ycsb的一些设置不一样
        self.value_size = 1024
        self.operationcount = 0
        self.zipfianconstant = 0.99
        self.mem_pool_size_gb = 30
        self.partition = "range"
        self.nclients_per_server = 1

        # CC
        self.cc_nconn_workers = 512
        self.num_rdma_fg_workers = 16
        self.num_rdma_bg_workers = 16
        self.num_storage_workers = 256
        self.num_compaction_workers = 256
        self.num_recovery_threads = 32
        self.num_migration_threads = 32
        self.block_cache_mb = 0
        self.row_cache_mb = 0
        self.memtable_size_mb = 16
        self.ltc_config_path = self.config_dir + "/nova-tutorial-config"
        self.cc_nreplicas_per_range = 1
        self.cc_nranges_per_server = 1
        self.cc_log_buf_size = 1024
        self.max_stoc_file_size_mb = 18432
        self.sstable_size_mb = 16
        self.cc_stoc_files_path = self.home_dir + "/db_files/stoc_files"
        self.ltc_num_stocs_scatter_data_blocks = 1
        self.num_memtable_partitions = 64
        self.cc_log_record_policy = "exclusive"
        self.cc_log_max_file_size_mb = 18

        self.port = 10000 + random.randint(0, 1000)
        self.rdma_port = 20000 + random.randint(0, 1000)
        self.rdma_max_msg_size = 256 * 1024
        self.rdma_max_num_sends = 32
        self.rdma_doorbell_batch_size = 8
        self.enable_load_data = False
        self.enable_rdma = True
        self.num_memtables = 256

        self.log_record_mode = "none"
        self.num_log_replicas = 1
        self.zipfian_dist_file_path = "/tmp/zipfian"
        # self.try = 0 ?未识别

        #change cfg?
        self.change_cfg = "false"

        #subrange config
        self.recover_dbs = "true"
        self.enable_subrange = "true"
        self.enable_lookup_index = "true"
        self.enable_range_index = "true"
        self.enable_detailed_db_stats = "false"
        self.enable_flush_multiple_memtables = "true"
        self.subrange_no_flush_num_keys = 100
        self.enable_subrange_reorg = "false"

        #filter data...config
        self.scatter_policy = "power_of_two"

        #lsm config
        self.level = 6
        self.use_local_disk = "false"
        self.num_sstable_replicas = 1

        self.l0_start_compaction_mb = 4096
        self.l0_stop_write_mb = 10240
        self.ltc_migration_policy = "immediate"

        #compaction config
        self.major_compaction_type = "sc"
        self.major_compaction_max_parallism = 32
        self.major_compaction_max_tables_in_a_set = 20

    def preWork(self):
        self.sshInfos = prepareSSHinfos()
        self.sshProxies = prepareSSHproxies(self.sshInfos)
        os.makedirs(self.results, exist_ok=True)
        os.makedirs(self.exp_results_dir, exist_ok=True)
        return 
        
    def postWork(self):
        return
    
    def mainWork(self):
        servers = []
        clis = []
        machines = []
        for i in range(0, self.nservers):
            servers.append(i) 
        for i in range(0, self.nclients):
            clis.append(self.nmachines - 1 - i)
        for i in range(0, self.nmachines):
            machines.append(i)
        print("machines: ", machines)
        sys.stdout.flush()
        print("servers: ", servers)
        sys.stdout.flush()
        print("clis: ", clis)
        sys.stdout.flush()
        
        
        nova_servers="" # ltc和stoc的服务器加起来的所有server ip:port形式, 真正的所有服务器的集合, 传给nova_server_main用
        nova_all_servers="" # ltc server, ip:port形式, 传给YCSB用来给server发各种消息
        i = 0
        for s in servers:
            nova_port = str(self.port)
            nova_servers = nova_servers + "," + self.sshProxies[s].getNode() + ":" + nova_port
            i = i + 1
            if i <= self.number_of_ltcs:
                nova_all_servers = nova_all_servers + "," + self.sshProxies[s].getNode() + ":" + nova_port
        nova_servers = nova_servers[1:]
        nova_all_servers = nova_all_servers[1:]
        print("nova_servers: ", nova_servers)
        sys.stdout.flush()
        print("nova_all_servers: ", nova_all_servers)
        sys.stdout.flush()
        
        
        nstoc = self.nservers - self.number_of_ltcs
        print("server count: ", self.nservers)
        sys.stdout.flush()
        print("ltc count: ", self.number_of_ltcs)
        sys.stdout.flush()
        print("stoc count: ", nstoc)
        sys.stdout.flush() 
        
        #这里省略了try
        result_dir_name="nova" + \
                        "-zf-" + str(self.zipfianconstant) + \
                        "-nm-" + str(self.num_memtables) + \
                        "-lr-" + str(self.num_log_replicas) + \
                        "-cfg-" + str(self.change_cfg) + \
                        "-d-" + str(self.dist) + \
                        "-w-" + str(self.workload) + \
                        "-ltc-" + str(self.number_of_ltcs) + \
                        "-stoc-" + str(nstoc) + \
                        "-l0-" + str(self.l0_stop_write_mb) + \
                        "-np-" + str(self.num_memtable_partitions) + \
                        "-nr-" + str(self.cc_nranges_per_server)
        print("running experiment: ", result_dir_name)
        sys.stdout.flush()
        dir = self.exp_results_dir + "/" + result_dir_name
        print("Result Dir: ", dir)
        sys.stdout.flush()
        os.system("rm -rf " + dir)
        os.system("mkdir -p " + dir)
        os.system("chmod -R 777 " + dir)
        db_path = self.home_dir + "/db_files/nova-db-" + str(self.recordcount) + "-" + str(self.value_size)
        print("nova server ip+port: ", nova_servers) 
        sys.stdout.flush()
        print("ltc config: ", self.ltc_config_path)
        sys.stdout.flush()
        print("db path: ", db_path)
        sys.stdout.flush()
        print("cc servers: ", nova_all_servers) #这个其实就是ltc
        sys.stdout.flush()
        
        print("cleaning results")
        sys.stdout.flush()
        for m in machines:
            print("removing resluts at machine ", m)
            sys.stdout.flush()
            if self.mydebug == True:
                print("ssh prefix: ssh -oStrictHostKeyChecking=no -t "+self.sshProxies[m].getSSHstylestring())
                sys.stdout.flush()
                print("exec on node " + str(m) + ": sudo rm -rf " + self.results + " && mkdir -p " + self.results + " && chmod -R 777 " + self.results)
                sys.stdout.flush()
                print("exec on node " + str(m) + ": sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'")
                sys.stdout.flush()
            else:
                self.sshProxies[m].command("sudo rm -rf " + self.results + " && mkdir -p " + self.results + " && chmod -R 777 " + self.results)
                print("sudo rm -rf " + self.results + " && mkdir -p " + self.results + " && chmod -R 777 " + self.results)
                sys.stdout.flush()
                self.sshProxies[m].command("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'")
                print("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'")
                sys.stdout.flush()
        
        print("restoring images")
        sys.stdout.flush()
        for s in servers:
            print("restore database image " + str(s))
            sys.stdout.flush()
            if self.mydebug == True:
                print("ssh prefix: ssh -oStrictHostKeyChecking=no -t " + self.sshProxies[s].getSSHstylestring())
                sys.stdout.flush()
                print("exec on node " + str(s) + ": rm -rf " + str(self.home_dir) + "/db_files/nova-db-" + str(self.recordcount) + "-1024/ ")
                sys.stdout.flush()
                print("exec on node " + str(s) + ": cp -r " + str(self.home_dir) + "/db_files/snapshot-" + str(self.cc_nranges_per_server) + "-" + str(self.nservers) + "-" + str(self.number_of_ltcs) + "-" + str(self.dist) + "-" + str(self.num_memtable_partitions) + \
                    "-" + str(self.memtable_size_mb) + "-" + str(self.zipfianconstant) + "-" + str(self.num_sstable_replicas) + \
                    "/nova-db-" + str(self.recordcount) + "-1024/ " + str(self.home_dir) + "/db_files/ > /dev/null 2> /dev/null < /dev/null &")
                sys.stdout.flush()
            else:
                self.sshProxies[s].command("rm -rf " + str(self.home_dir) + "/db_files/nova-db-" + str(self.recordcount) + "-1024/ ")
                print("rm -rf " + str(self.home_dir) + "/db_files/nova-db-" + str(self.recordcount) + "-1024/ ")
                sys.stdout.flush()
                self.sshProxies[s].command("cp -r " + str(self.home_dir) + "/db_files/snapshot-" + str(self.cc_nranges_per_server) + "-" + str(self.nservers) + "-" + str(self.number_of_ltcs) + "-" + str(self.dist) + "-" + str(self.num_memtable_partitions) + \
                    "-" + str(self.memtable_size_mb) + "-" + str(self.zipfianconstant) + "-" + str(self.num_sstable_replicas) + \
                    "/nova-db-" + str(self.recordcount) + "-1024/ " + str(self.home_dir) + "/db_files/ > /dev/null 2> /dev/null < /dev/null &")
                print("cp -r " + str(self.home_dir) + "/db_files/snapshot-" + str(self.cc_nranges_per_server) + "-" + str(self.nservers) + "-" + str(self.number_of_ltcs) + "-" + str(self.dist) + "-" + str(self.num_memtable_partitions) + \
                    "-" + str(self.memtable_size_mb) + "-" + str(self.zipfianconstant) + "-" + str(self.num_sstable_replicas) + \
                    "/nova-db-" + str(self.recordcount) + "-1024/ " + str(self.home_dir) + "/db_files/ > /dev/null 2> /dev/null < /dev/null &")
                sys.stdout.flush()
                
                
        if self.mydebug == True:
            pass
        else:   
            time.sleep(10)
        print("syning restoring process")
        sys.stdout.flush()
        for m in machines:
            if self.mydebug == True:
                print("ssh prefix: ssh -oStrictHostKeyChecking=no -t " + self.sshProxies[m].getSSHstylestring())
                sys.stdout.flush()
                print("exec on node " + str(m) + ": ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c \"cp -r\"")
                sys.stdout.flush()
                print("wait for it done")
                sys.stdout.flush()
            else:
                while True:    
                    resultStr = self.sshProxies[m].command("ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c \"cp -r\"")
                    if int(resultStr) > 0:
                        time.sleep(10)
                        print("waiting for " + m)
                        sys.stdout.flush()
                    else:
                        break    
        
        print("Preparing sar")
        sys.stdout.flush()
        for m in machines:
            if self.mydebug == True:
                print("ssh prefix: ssh -oStrictHostKeyChecking=no -t " + self.sshProxies[m].getSSHstylestring())
                sys.stdout.flush()
                print("exec on node " + str(m) + ": sudo killall leveldb_main nova_shared_main nova_multi_thread_compaction nova_server_main java collectl sar")
                sys.stdout.flush()
                print("exec on node " + str(m) + ": sudo collectl -scx -i 1 -P > " + self.results + "/" + str(m) + "-coll.txt 2> /dev/null < /dev/null &")
                sys.stdout.flush()
                print("exec on node " + str(m) + ": sar -P ALL 1 > " + self.results + "/" + str(m) + "-cpu.txt 2> /dev/null < /dev/null &")
                sys.stdout.flush()
                print("exec on node " + str(m) + ": sar -n DEV 1 > " + self.results + "/" + str(m) + "-net.txt 2> /dev/null < /dev/null &")
                sys.stdout.flush()
                print("exec on node " + str(m) + ": sar -r 1 > " + self.results + "/" + str(m) + "-mem.txt 2> /dev/null < /dev/null &")
                sys.stdout.flush()
                print("exec on node " + str(m) + ": sar -d 1 > " + self.results + "/" + str(m) + "-disk.txt 2> /dev/null < /dev/null &")             
            else:
                self.sshProxies[m].command("sudo killall leveldb_main nova_shared_main nova_multi_thread_compaction nova_server_main java collectl sar")
                print("sudo killall leveldb_main nova_shared_main nova_multi_thread_compaction nova_server_main java collectl sar")
                sys.stdout.flush()
                self.sshProxies[m].command("sudo collectl -scx -i 1 -P > " + self.results + "/" + str(m) + "-coll.txt 2> /dev/null < /dev/null &")
                print("sudo collectl -scx -i 1 -P > " + self.results + "/" + str(m) + "-coll.txt 2> /dev/null < /dev/null &")
                sys.stdout.flush()
                self.sshProxies[m].command("sar -P ALL 1 > " + self.results + "/" + str(m) + "-cpu.txt 2> /dev/null < /dev/null &")
                print("sar -P ALL 1 > " + self.results + "/" + str(m) + "-cpu.txt 2> /dev/null < /dev/null &")
                sys.stdout.flush()
                self.sshProxies[m].command("sar -n DEV 1 > " + self.results + "/" + str(m) + "-net.txt 2> /dev/null < /dev/null &")
                print("sar -n DEV 1 > " + self.results + "/" + str(m) + "-net.txt 2> /dev/null < /dev/null &")
                sys.stdout.flush()                
                self.sshProxies[m].command("sar -r 1 > " + self.results + "/" + str(m) + "-mem.txt 2> /dev/null < /dev/null &")
                print("sar -r 1 > " + self.results + "/" + str(m) + "-mem.txt 2> /dev/null < /dev/null &")
                sys.stdout.flush()
                self.sshProxies[m].command("sar -d 1 > " + self.results + "/" + str(m) + "-disk.txt 2> /dev/null < /dev/null &")
                print("sar -d 1 > " + self.results + "/" + str(m) + "-disk.txt 2> /dev/null < /dev/null &")
                sys.stdout.flush()
        
        print("syning nova_server_main")
        sys.stdout.flush()
        for m in machines:
            if self.mydebug == True:
                print("ssh prefix: ssh -oStrictHostKeyChecking=no -t " + self.sshProxies[m].getSSHstylestring())
                sys.stdout.flush()
                print("exec on node " + str(m) + ": ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c nova_server_main")
                sys.stdout.flush()
                print("exec on node " + str(m) + ": wait for it done")
                sys.stdout.flush()
            else:
                while True:    
                    resultStr = self.sshProxies[m].command("ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c nova_server_main")
                    if int(resultStr) > 0:
                        time.sleep(10)
                        print("waiting for " + m)
                        sys.stdout.flush()
                    else:
                        break  
        
        print("starting server")
        sys.stdout.flush()
        server_id = 0
        for s in servers:
            print("creating server on " + str(s))
            sys.stdout.flush()
            cmd = str("stdbuf --output=0 --error=0 {}/nova_server_main " +\
                "--ltc_migration_policy={} " + \
                "--enable_range_index={} " + \
                "--num_migration_threads={} " + \
                "--num_sstable_replicas={} " + \
                "--level={} " + \
                "--l0_start_compaction_mb={} " + \
                "--subrange_no_flush_num_keys={} " + \
                "--enable_detailed_db_stats={} " + \
                "--major_compaction_type={} " + \
                "--major_compaction_max_parallism={} " + \
                "--major_compaction_max_tables_in_a_set={} " + \
                "--enable_flush_multiple_memtables={} " + \
                "--recover_dbs={} " + \
                "--num_recovery_threads={} " + \
                "--sampling_ratio=1 " + \
                "--zipfian_dist_ref_counts={} " + \
                "--client_access_pattern={} " +  \
                "--memtable_type=static_partition " + \
                "--enable_subrange={} " + \
                "--num_log_replicas={} " + \
                "--log_record_mode={} " + \
                "--scatter_policy={} " + \
                "--number_of_ltcs={} " + \
                "--enable_lookup_index={} " + \
                "--l0_stop_write_mb={} " + \
                "--num_memtable_partitions={} " + \
                "--num_memtables={} " + \
                "--num_rdma_bg_workers={} " + \
                "--db_path={} " + \
                "--num_storage_workers={} " + \
                "--stoc_files_path={} " + \
                "--max_stoc_file_size_mb={} " + \
                "--sstable_size_mb={} " + \
                "--ltc_num_stocs_scatter_data_blocks={} " + \
                "--all_servers={} " + \
                "--server_id={} " + \
                "--mem_pool_size_gb={} " + \
                "--use_fixed_value_size={} " + \
                "--ltc_config_path={} " + \
                "--ltc_num_client_workers={} " + \
                "--num_rdma_fg_workers={} " + \
                "--num_compaction_workers={} " + \
                "--block_cache_mb={} " + \
                "--row_cache_mb={} " + \
                "--memtable_size_mb={} " + \
                "--cc_log_buf_size={} " + \
                "--rdma_port={} " + \
                "--rdma_max_msg_size={} " + \
                "--rdma_max_num_sends={} " + \
                "--rdma_doorbell_batch_size={} " + \
                "--enable_rdma={} " + \
                "--enable_load_data={} " + \
                "--use_local_disk={}").format(str(self.cache_bin_dir), \
                                                        self.ltc_migration_policy, \
                                                        self.enable_range_index, \
                                                        self.num_migration_threads, \
                                                        self.num_sstable_replicas, \
                                                        self.level, \
                                                        self.l0_start_compaction_mb, \
                                                        self.subrange_no_flush_num_keys, \
                                                        self.enable_detailed_db_stats, \
                                                        self.major_compaction_type, \
                                                        self.major_compaction_max_parallism, \
                                                        self.major_compaction_max_tables_in_a_set, \
                                                        self.enable_flush_multiple_memtables, \
                                                        self.recover_dbs, \
                                                        self.num_recovery_threads, \
                                                        self.zipfian_dist_file_path, \
                                                        self.dist, \
                                                        # self.static_partition, \
                                                        self.enable_subrange, \
                                                        self.num_log_replicas, \
                                                        self.log_record_mode, \
                                                        self.scatter_policy, \
                                                        self.number_of_ltcs, \
                                                        self.enable_lookup_index, \
                                                        self.l0_stop_write_mb, \
                                                        self.num_memtable_partitions, \
                                                        self.num_memtables, \
                                                        self.num_rdma_bg_workers, \
                                                        db_path, \
                                                        self.num_storage_workers, \
                                                        self.cc_stoc_files_path, \
                                                        self.max_stoc_file_size_mb, \
                                                        self.sstable_size_mb, \
                                                        self.ltc_num_stocs_scatter_data_blocks, \
                                                        nova_servers, \
                                                        server_id, \
                                                        self.mem_pool_size_gb, \
                                                        self.value_size, \
                                                        self.ltc_config_path, \
                                                        self.cc_nconn_workers, \
                                                        self.num_rdma_fg_workers, \
                                                        self.num_compaction_workers, \
                                                        self.block_cache_mb, \
                                                        self.row_cache_mb, \
                                                        self.memtable_size_mb, \
                                                        self.cc_log_buf_size, \
                                                        self.rdma_port, \
                                                        self.rdma_max_msg_size, \
                                                        self.rdma_max_num_sends, \
                                                        self.rdma_doorbell_batch_size, \
                                                        self.enable_rdma, \
                                                        self.enable_load_data, \
                                                        self.use_local_disk)
            print(cmd)
            sys.stdout.flush()
            if self.mydebug == True:
                print("ssh prefix: ssh -oStrictHostKeyChecking=no -t " + self.sshProxies[s].getSSHstylestring())
                sys.stdout.flush()
                print("exec on node " + str(s) + ": mkdir -p " + str(self.cc_stoc_files_path))
                sys.stdout.flush()
                print("exec on node " + str(s) + ": mkdir -p " + str(db_path))
                sys.stdout.flush()
                print("exec on node " + str(s) + ": " + cmd + " >& " + self.results + "/server-" + str(s) + "-out < /dev/null &")
                sys.stdout.flush()     
            else:
                self.sshProxies[s].command("mkdir -p " + str(self.cc_stoc_files_path))
                self.sshProxies[s].command("mkdir -p " + str(db_path))
                self.sshProxies[s].command(cmd + " >& " + self.results + "/server-" + str(s) + "-out < /dev/null &")
                time.sleep(1)
            server_id = server_id + 1
        
        if self.mydebug == True:
            pass
        else:   
            time.sleep(30)
        print("starting ycsb")
        sys.stdout.flush()
        for c in clis:
            for i in seq(self.nclients_per_server):
                print("creating client on {}-{}".format(c, i))
                cmd="stdbuf --output=0 --error=0 bash " + \
                    "{}/exp/run_ycsb.sh " + \
                    "{} " + \
                    "{} " + \
                    "{} " + \
                    "{} " + \
                    "{} " + \
                    "{} " + \
                    "{} " + \
                    "{} " + \
                    "{} " + \
                    "{} " + \
                    "{} " + \
                    "{} " + \
                    "{} " + \
                    "0"
                cmd = cmd.format(self.script_dir, str(self.nthreads), nova_all_servers, self.debug, self.partition, self.recordcount, self.maxexecutiontime, \
                                self.dist, str(self.value_size), self.workload, self.ltc_config_path, self.cardinality, self.operationcount, self.zipfianconstant)
                print(cmd)
                sys.stdout.flush()
                if self.mydebug == True:
                    print("ssh prefix: ssh -oStrictHostKeyChecking=no -t " + self.sshProxies[c].getSSHstylestring())
                    sys.stdout.flush()
                    print("exec on node " + str(s) + ": cd {} && {} >& {}/client-{}-{}-out < /dev/null &".format(self.client_bin_dir, cmd, self.results, c, i))
                    sys.stdout.flush()
                else:
                    self.sshProxies[c].command("{} >& {}/client-{}-{}-out < /dev/null &".format(cmd, self.results, c, i))
        
        print("change_cfg: " + self.change_cfg)
        sys.stdout.flush()
        if self.change_cfg == "true":
            os.system("java -jar {}/nova_coordinator.jar {} {}".format(self.cache_bin_dir, nova_servers, self.ltc_config_path))
        
        if self.mydebug == True:
            pass
        else:   
            time.sleep(10)
        stop = "false"
        sleep_time = 0
        max_wait_time = self.maxexecutiontime + 1200
        print("waiting for experiments done")
        sys.stdout.flush()
        for c in clis:
            if self.mydebug == True:
                print("ssh prefix: ssh -oStrictHostKeyChecking=no -t " + self.sshProxies[c].getSSHstylestring())
                sys.stdout.flush()
                print("exec on node " + str(c) + ": ps -ef | grep -v \"grep --color=auto ycsb\" | grep -v ssh | grep -v bash | grep ycsb | grep -c java")
                sys.stdout.flush()
                print("wait for it done")
                sys.stdout.flush()
            else:
                while True:    
                    resultStr = self.sshProxies[c].command("ps -ef | grep -v \"grep --color=auto ycsb\" | grep -v ssh | grep -v bash | grep ycsb | grep -c java")
                    if int(resultStr) > 0:
                        time.sleep(10)
                        sleep_time = sleep_time + 10
                        print("waiting for {} for {} seconds".format(c, sleep_time))
                        sys.stdout.flush()
                        if sleep_time > max_wait_time:
                            stop = "true"
                            break
                    else:
                        break  
                if stop == "true":
                    print("exceeded maximum wait time")
                    sys.stdout.flush()
                    break
                
        print("getting db size")
        sys.stdout.flush()
        for s in servers:
            cmd="du -sm {}".format(db_path)
            print(cmd)
            sys.stdout.flush()
            if self.mydebug == True:
                print("ssh prefix: ssh -oStrictHostKeyChecking=no -t " + self.sshProxies[s].getSSHstylestring())
                sys.stdout.flush()
                print("exec on node " + str(s) + ": {} >& {}/server-{}-db-disk-space < /dev/null".format(cmd, self.results, s))
                sys.stdout.flush()
            else:
                self.sshProxies[s].command("{} >& {}/server-{}-db-disk-space < /dev/null".format(cmd, self.results, s))
        
        print("getting stoc file size")
        sys.stdout.flush()
        for s in servers:
            cmd="du -sm {}".format(self.cc_stoc_files_path)
            print(cmd)
            sys.stdout.flush()
            if self.mydebug == True:
                print("ssh prefix: ssh -oStrictHostKeyChecking=no -t " + self.sshProxies[s].getSSHstylestring())
                sys.stdout.flush()
                print("exec on node " + str(s) + ": {} >& {}/server-{}-rtable-disk-space".format(cmd, self.results, s))
                sys.stdout.flush()
            else:
                self.sshProxies[s].command("{} >& {}/server-{}-rtable-disk-space".format(cmd, self.results, s))
        
        print("killing experiment processes")
        sys.stdout.flush()
        for m in machines:
            print("kill experiment processes on {}".format(m))
            sys.stdout.flush()
            if self.mydebug == True:
                print("ssh prefix: ssh -oStrictHostKeyChecking=no -t " + self.sshProxies[m].getSSHstylestring())
                sys.stdout.flush()
                print("exec on node " + str(m) + ": sudo killall leveldb_main nova_shared_main nova_multi_thread_compaction nova_server_main java collectl sar")
                sys.stdout.flush()
            else:
                self.sshProxies[m].command("sudo killall leveldb_main nova_shared_main nova_multi_thread_compaction nova_server_main java collectl sar")
        
        dir = "{}/{}".format(self.exp_results_dir, result_dir_name)
        print("saving to {}".format(dir))
        sys.stdout.flush()
        os.system("rm -rf {}".format(dir))
        os.system("mkdir -p {}".format(dir))
        os.system("chmod -R 777 {}".format(dir))
        
        print("getting logs")
        sys.stdout.flush()
        server_id = 0
        for s in servers:
            if self.mydebug == True:
                print("ssh prefix: ssh -oStrictHostKeyChecking=no -t " + self.sshProxies[s].getSSHstylestring())
                sys.stdout.flush()
                print("exec on node " + str(s) + ": mkdir -p {}/server-{}-dblogs/ && cp -r {}/*/LOG* {}/server-{}-dblogs/".format(self.results, server_id, db_path, self.results, server_id))
                sys.stdout.flush()
            else:
                self.sshProxies[s].command("mkdir -p {}/server-{}-dblogs/ && cp -r {}/*/LOG* {}/server-{}-dblogs/".format(self.results, server_id, db_path, self.results, server_id))
            server_id = server_id + 1
        
        print("transferring to main node")
        sys.stdout.flush()
        for m in machines:
            if self.mydebug == True:
                print("exec: scp -r {}:{}/* {}".format(self.sshProxies[m].getSSHstylestring(), self.results, dir))
                sys.stdout.flush()
            else:
                os.system("scp -r {}:{}/* {}".format(self.sshProxies[m].getSSHstylestring(), self.results, dir))
        if self.mydebug == True:
            pass
        else:   
            time.sleep(10)   
        
        print("processing results")
        sys.stdout.flush()
        if self.mydebug == True:
            print("exec: sudo python /home/yuhang/NovaLSM/scripts/exp/parse_ycsb_nova_leveldb.py {} {} > {}/stats_tutorial_out".format(self.nmachines, self.exp_results_dir, self.exp_results_dir))
            sys.stdout.flush()
        else:
            os.system("sudo python /home/yuhang/NovaLSM/scripts/exp/parse_ycsb_nova_leveldb.py {} {} > {}/stats_tutorial_out".format(self.nmachines, self.exp_results_dir, self.exp_results_dir)) 
        return 
        

if __name__ == "__main__":    
    runner = Runner()
    runner.preWork()
    runner.mainWork()
    runner.postWork()
    
    # print(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) 
    # for sshProxy in sshProxies:
    #     print(sshProxy.getNode())
    #     print(sshProxy.command("sudo collectl -scx -i 1 -P > /tmp/results/test1-coll.txt 2> /dev/null < /dev/null &"))
    
    # for sshInfo in sshInfos:
    #     print(sshInfo.hostname)
    #     print(sshInfo.port)
    #     print(sshInfo.username)
    #     print(sshInfo.password)
    #     print(sshInfo.getIp())
    #     print(sshInfo.getSSHstylestring())
    
    
    
    # ssh = paramiko.SSHClient()
    
    
    
    # ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # ssh.connect(hostname='172.16.219.173',port=22,username='yuhang',password='zgh123456')
    
    
    
    
    
    
    
    
    # 不太用得上命令行参数
    # parser = optparse.OptionParser()
    # parser.add_option("--debug", action = "store", type = str, dest = "debug", help = "run in debugging mode or not")
    # parser.add_option("--recordcount", action = "store", type = int, dest = "recordcount", help = "how many records you want to run in an experiment")
    
    
    # parser.add_option("--debug",   help="debugging mode")
    # parser.add_option("--ty",    help="{sc | util | cmpdev }")
    # parser.add_option("--out",   help="output directory")
    # parser.add_option("--ncore", help="# core (only for utilization and cmpdev)", default="1")
    # (opts, args) = parser.parse_args()
    

    # check arg
    # for opt in vars(opts):
    #     val = getattr(opts, opt)
    #     if val == None:
    #         print("Missing options: %s" % opt)
    #         parser.print_help()
    #         exit(1)