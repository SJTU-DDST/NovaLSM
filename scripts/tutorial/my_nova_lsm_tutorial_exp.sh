#!/bin/bash

#ssh版本的node
#ssh_nodes=("yuhang@192.168.98.74" "yuhang@192.168.98.73" "yuhang@192.168.98.70" "yuhang@192.168.98.53" "yuhang@192.168.98.52")
ssh_nodes=("yuhang@192.168.98.74" "yuhang@192.168.98.70" "yuhang@192.168.98.53")
#ip的node，就先用74 73 70 53 52
#ip_nodes=("192.168.98.74" "192.168.98.73" "192.168.98.70" "192.168.98.53" "192.168.98.52")
ip_nodes=("192.168.98.74" "192.168.98.70" "192.168.98.53")

home_dir="/home/yuhang/NovaLSM"
config_dir="$home_dir/config"
db_dir="$home_dir/db"
script_dir="$home_dir/scripts"
cache_bin_dir="$home_dir/nova"
client_bin_dir="/tmp/YCSB-Nova"
results="/tmp/results"
recordcount="$1"
exp_results_dir="$home_dir/nova-tutorial-$recordcount"
dryrun="$2"
mydebug="$3"

mkdir -p $results
mkdir -p $exp_results_dir

nmachines="3" # 5 machines in total
nservers="2" # 4 servers function(52 serve as client)
number_of_ltcs="1" # duplicate 2 servers as ltc 这个参数没有作用,nova_server_main里面不会使用
nclients="1"

# YCSB
maxexecutiontime=1200
workload="workloadw"
nthreads="512"
debug="false"
dist="uniform"
cardinality="10" #这里有点区别，应该是ycsb的一些设置不一样
value_size="1024"
operationcount="0"
zipfianconstant="0.99"
mem_pool_size_gb="30"
partition="range"
nclients_per_server="1"

# CC
cc_nconn_workers="512"
num_rdma_fg_workers="16"
num_rdma_bg_workers="16"
num_storage_workers="256"
num_compaction_workers="256"
num_recovery_threads="32"
num_migration_threads="32"
block_cache_mb="0"
row_cache_mb="0"
memtable_size_mb="16"
ltc_config_path=""
cc_nreplicas_per_range="1"
cc_nranges_per_server="1"
cc_log_buf_size="1024"
max_stoc_file_size_mb="18432"
sstable_size_mb="16"
cc_stoc_files_path="$home_dir/db_files/stoc_files"
ltc_num_stocs_scatter_data_blocks="1"
num_memtable_partitions="64"
cc_log_record_policy="exclusive"
cc_log_max_file_size_mb="18"

port=$((10000+RANDOM%1000))
rdma_port=$((20000+RANDOM%1000))
rdma_max_msg_size=$((256*1024))
rdma_max_num_sends="32"
rdma_doorbell_batch_size="8"
enable_load_data="false"
enable_rdma="true"
num_memtables="256"

log_record_mode="none"
num_log_replicas="1"
zipfian_dist_file_path="/tmp/zipfian"
try="0"

#change cfg?
change_cfg="false"

#subrange config
recover_dbs="true"
enable_subrange="true"
enable_lookup_index="true"
enable_range_index="true"
enable_detailed_db_stats="false"
enable_flush_multiple_memtables="true"
subrange_no_flush_num_keys="100"
enable_subrange_reorg="false"

#filter data...config
scatter_policy="power_of_two"

#lsm config
level="6"
use_local_disk="false"
num_sstable_replicas="1"

l0_start_compaction_mb=$((4*1024))
l0_stop_write_mb=$((10*1024))
ltc_migration_policy="immediate"

#compaction config
major_compaction_type="sc"
major_compaction_max_parallism="32"
major_compaction_max_tables_in_a_set="20"


#实际的跑bench的函数
function run_bench() {
	servers=()
	clis=()
	machines=()
	i=0
	n=0
	while [ $n -lt $nservers ] #遍历server的个数
	do
		# if [[ $i == "9" ]]; then
		# 	i=$((i+1))
		# 	continue	
		# fi
		servers+=("$i") #server里面加一个node
		i=$((i+1))
		n=$((n+1))
	done

	i=0
	n=0
	while [ $n -lt $nclients ] #遍历client的个数
	do
		id=$((nmachines-1-i))
		# if [[ $id == "9" ]]; then
		# 	i=$((i+1))
		# 	continue	
		# fi
		clis+=("$id") #client里面加一个node
		i=$((i+1))
		n=$((n+1))
	done

	for ((i=0;i<nmachines;i++)); #遍历machine的个数
	do
		id=$((i))
		machines+=("$id") #给machine node的命名
	done

#打印出client server machine的内容，看看都是哪些node
	echo "client node id: " ${clis[@]}
	echo "server node id: " ${servers[@]}
	echo "machine node id: " ${machines[@]}

	nova_servers="" # ltc和stoc的服务器加起来的所有server ip:port形式, 真正的所有服务器的集合, 传给nova_server_main用
	nova_all_servers="" # ltc server, ip:port形式, 传给YCSB用来给server发各种消息
	i="0"
	for s in ${servers[@]}
	do
		nova_port="$port"
		nova_servers="$nova_servers,${ip_nodes[s]}:$nova_port"
		i=$((i+1))
		if [[ $i -le number_of_ltcs ]]; then
			nova_all_servers="$nova_all_servers,${ip_nodes[s]}:$nova_port"
		fi
		nova_port=$((nova_port+1))
	done

	nova_servers="${nova_servers:1}" #这里是去掉第一个逗号
	nova_all_servers="${nova_all_servers:1}" 

# 打印一下当前的机器配置
	current_time=$(date "+%Y-%m-%d-%H-%M-%S")
	nstoc=$((nservers-number_of_ltcs))
	echo "server count: " "$nservers" 
	echo "ltc count: " "$number_of_ltcs" 
	echo "stoc count: " "$nstoc"
# 打印一下结果的存放文件
	result_dir_name="nova-zf-$zipfianconstant-nm-$num_memtables-lr-$num_log_replicas-try-$try-cfg-$change_cfg-d-$dist-w-$workload-ltc-$number_of_ltcs-stoc-$nstoc-l0-$l0_stop_write_mb-np-$num_memtable_partitions-nr-$cc_nranges_per_server"
	echo "running experiment $result_dir_name"

# 把目录新建出来
	# Copy the files over local node
    dir="$exp_results_dir/$result_dir_name"
    echo "Result Dir $dir..."
    rm -rf $dir
    mkdir -p $dir
    chmod -R 777 $dir

	# cmd="java -jar $cache_bin_dir/nova_config_generator.jar $config_dir "migration" $recordcount $number_of_ltcs $cc_nreplicas_per_range $cc_nranges_per_server $zipfianconstant"
	# echo $cmd
	# eval $cmd
	# number_of_stocs=$((nservers-number_of_ltcs))
# 找到config的path
	ltc_config_path="$config_dir/nova-tutorial-config"
	
	db_path="$home_dir/db_files/nova-db-$recordcount-$value_size"

	echo "nova server ip+port: " "$nova_servers" 
	echo "ltc config: " "$ltc_config_path" 
	echo "db path: " "$db_path"
	echo "cc servers: $nova_all_servers" #这个其实就是ltc

	# for m in ${machines[@]}
	# do
	# 	echo "machine $m ssh: ${ssh_nodes[m]}"
	# done


# dryrun代表的是查看config??
	if [[ $dryrun == "true" ]]; then
		return
	fi

#这里开始正式跑

	echo "cleaning results"

# 遍历所有machine 删除各个machine的result dir并且重建,清空系统的cache
	for m in ${machines[@]}
	do
		echo "remove $results at machine $m"
    	if [[ $mydebug == "true" ]]; then
			echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]}"
			echo "exec on node $m: " "sudo rm -rf $results && mkdir -p $results && chmod -R 777 $results" 
			echo "exec on node $m: " "sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'"
		else
			ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]} "sudo rm -rf $results && mkdir -p $results && chmod -R 777 $results"
			ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]} "sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'"
		fi
	done

	echo "restoring images"

	# restore the database image. 
	for s in ${servers[@]}
	do
		echo "restore database image $s"
		if [[ $mydebug == "true" ]]; then
			echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[s]}"
			echo "exec on node $s: " "rm -rf $home_dir/db_files/nova-db-$recordcount-1024/ && cp -r $home_dir/db_files/snapshot-$cc_nranges_per_server-$nservers-$number_of_ltcs-$dist-$num_memtable_partitions-$memtable_size_mb-$zipfianconstant-$num_sstable_replicas/nova-db-$recordcount-1024/ $home_dir/db_files/ &"
		else
			ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[s]} "rm -rf $home_dir/db_files/nova-db-$recordcount-1024/ && cp -r $home_dir/db_files/snapshot-$cc_nranges_per_server-$nservers-$number_of_ltcs-$dist-$num_memtable_partitions-$memtable_size_mb-$zipfianconstant-$num_sstable_replicas/nova-db-$recordcount-1024/ $home_dir/db_files/ &" &
		fi
	done

	sleep 10

	echo "syning restoring process"

	for m in ${machines[@]}
	do
		if [[ $mydebug == "true" ]]; then
			echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]}"
			echo "exec on node $m: " "ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c \"cp -r\""
			echo "wait for it done"
		else
			while ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]} "ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c \"cp -r\""
			do
				sleep 10
				echo "waiting for $m"
			done
		fi
	done
	
	# start stats
	echo "Preparing sar"

	for m in ${machines[@]}
	do
		if [[ $mydebug == "true" ]]; then
			echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]}"
			echo "exec on node $m: " "sudo killall leveldb_main nova_shared_main nova_multi_thread_compaction nova_server_main java collectl sar"
			echo "exec on node $m: " "sudo collectl -scx -i 1 -P > $results/$m-coll.txt &"
			echo "exec on node $m: " "sar -P ALL 1 > $results/$m-cpu.txt &"
			echo "exec on node $m: " "sar -n DEV 1 > $results/$m-net.txt &"
			echo "exec on node $m: " "sar -r 1 > $results/$m-mem.txt &"
			echo "exec on node $m: " "sar -d 1 > $results/$m-disk.txt &"
		else
			echo "processing node $m, ip: ${ip_nodes[m]}"
			ssh -oStrictHostKeyChecking=no ${ssh_nodes[m]} "sudo killall leveldb_main nova_shared_main nova_multi_thread_compaction nova_server_main java collectl sar"
			echo "sudo killall done"
			ssh -oStrictHostKeyChecking=no ${ssh_nodes[m]} "sudo collectl -scx -i 1 -P > $results/$m-coll.txt &"
			echo "sudo collectl done"
			ssh -oStrictHostKeyChecking=no ${ssh_nodes[m]} "sar -P ALL 1 > $results/$m-cpu.txt &"
			echo "sar -p done"
			ssh -oStrictHostKeyChecking=no ${ssh_nodes[m]} "sar -n DEV 1 > $results/$m-net.txt &"
			echo "sar -n done"
			ssh -oStrictHostKeyChecking=no ${ssh_nodes[m]} "sar -r 1 > $results/$m-mem.txt &"
			echo "sar -r done"
			ssh -oStrictHostKeyChecking=no ${ssh_nodes[m]} "sar -d 1 > $results/$m-disk.txt &"
			echo "sar -d done"
		fi
	done

	echo "syning nova_server_main"

	#等待这些进程结束(也许是上一次测试留下的)
	for m in ${machines[@]}
	do
		if [[ $mydebug == "true" ]]; then
			echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]}"
			echo "exec on node $m: " "ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c nova_server_main"
			echo "exec on node $m: " "wait for it done"
		else
			while ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]} "ps -ef | grep -v grep | grep -v ssh | grep -v bash | grep -c nova_server_main"
			do
				sleep 10
				echo "waiting for $m"
			done
		fi
	done

	echo "starting server"

	#启动服务器
	server_id=0
	for s in ${servers[@]}
	do
		echo "creating server on $s"
		nova_rdma_port=$((rdma_port))
		cmd="stdbuf --output=0 --error=0 ./nova_server_main --ltc_migration_policy=$ltc_migration_policy --enable_range_index=$enable_range_index --num_migration_threads=$num_migration_threads --num_sstable_replicas=$num_sstable_replicas --level=$level --l0_start_compaction_mb=$l0_start_compaction_mb --subrange_no_flush_num_keys=$subrange_no_flush_num_keys --enable_detailed_db_stats=$enable_detailed_db_stats --major_compaction_type=$major_compaction_type --major_compaction_max_parallism=$major_compaction_max_parallism --major_compaction_max_tables_in_a_set=$major_compaction_max_tables_in_a_set --enable_flush_multiple_memtables=$enable_flush_multiple_memtables --recover_dbs=$recover_dbs --num_recovery_threads=$num_recovery_threads  --sampling_ratio=1 --zipfian_dist_ref_counts=$zipfian_dist_file_path --client_access_pattern=$dist  --memtable_type=static_partition --enable_subrange=$enable_subrange --num_log_replicas=$num_log_replicas --log_record_mode=$log_record_mode --scatter_policy=$scatter_policy --number_of_ltcs=$number_of_ltcs --enable_lookup_index=$enable_lookup_index --l0_stop_write_mb=$l0_stop_write_mb --num_memtable_partitions=$num_memtable_partitions --num_memtables=$num_memtables --num_rdma_bg_workers=$num_rdma_bg_workers --db_path=$db_path --num_storage_workers=$num_storage_workers --stoc_files_path=$cc_stoc_files_path --max_stoc_file_size_mb=$max_stoc_file_size_mb --sstable_size_mb=$sstable_size_mb --ltc_num_stocs_scatter_data_blocks=$ltc_num_stocs_scatter_data_blocks --all_servers=$nova_servers --server_id=$server_id --mem_pool_size_gb=$mem_pool_size_gb --use_fixed_value_size=$value_size --ltc_config_path=$ltc_config_path --ltc_num_client_workers=$cc_nconn_workers --num_rdma_fg_workers=$num_rdma_fg_workers --num_compaction_workers=$num_compaction_workers --block_cache_mb=$block_cache_mb --row_cache_mb=$row_cache_mb --memtable_size_mb=$memtable_size_mb --cc_log_buf_size=$cc_log_buf_size --rdma_port=$rdma_port --rdma_max_msg_size=$rdma_max_msg_size --rdma_max_num_sends=$rdma_max_num_sends --rdma_doorbell_batch_size=$rdma_doorbell_batch_size --enable_rdma=$enable_rdma --enable_load_data=$enable_load_data --use_local_disk=$use_local_disk"
		echo "$cmd"
		if [[ $mydebug == "true" ]]; then
			echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[s]}"
			echo "exec on node $s: " "mkdir -p $cc_stoc_files_path && mkdir -p $db_path && cd $cache_bin_dir && $cmd >& $results/server-$s-out &"
			server_id=$((server_id+1))
			nova_rdma_port=$((nova_rdma_port+1))
		else
			ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[s]} "mkdir -p $cc_stoc_files_path && mkdir -p $db_path && cd $cache_bin_dir && $cmd >& $results/server-$s-out &" &
			server_id=$((server_id+1))
			nova_rdma_port=$((nova_rdma_port+1))
			sleep 1
		fi
	done

	echo "starting ycsb"
	#启动ycsb
	sleep 30
	for c in ${clis[@]}
	do
		for i in $(seq 1 $nclients_per_server);
		do
			echo "creating client on $c-$i"
			cmd="stdbuf --output=0 --error=0 bash $script_dir/exp/run_ycsb.sh $nthreads $nova_all_servers $debug $partition $recordcount $maxexecutiontime $dist $value_size $workload $ltc_config_path $cardinality $operationcount $zipfianconstant 0"
			echo "$cmd"
			if [[ $mydebug == "true" ]]; then
				echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[c]}"
				echo "exec on node $c: " "cd $client_bin_dir && $cmd >& $results/client-$c-$i-out &"	
			else
				ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[c]} "cd $client_bin_dir && $cmd >& $results/client-$c-$i-out &" &
			fi
		done
	done

	# 这个怎么编译出来????，目前只能设为false
	if [[ $change_cfg == "true" ]]; then
		java -jar $cache_bin_dir/nova_coordinator.jar $nova_servers $ltc_config_path
	fi

	port=$((port+1))
	rdma_port=$((rdma_port+1))
	sleep 10
	sleep_time=0
	stop="false"
	max_wait_time=$((maxexecutiontime+1200))

	echo "waiting for experiments done"
	#执行时间结束之后
	for m in ${clis[@]}
	do
		if [[ $mydebug == "true" ]]; then
			echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]}"
			echo "exec on node $m: " "ps -ef | grep -v \"grep --color=auto ycsb\" | grep -v ssh | grep -v bash | grep ycsb | grep -c java"
			echo "wait for it done"
		else
			while ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]} "ps -ef | grep -v \"grep --color=auto ycsb\" | grep -v ssh | grep -v bash | grep ycsb | grep -c java"
			do
				sleep 10
				sleep_time=$((sleep_time+10))
				echo "waiting for $m for $sleep_time seconds"

				if [[ $sleep_time -gt $max_wait_time ]]; then
					stop="true"
					break
				fi
			done
			if [[ $stop == "true" ]]; then
				echo "exceeded maximum wait time"
				break
			fi
		fi
	done

	echo "getting db size"
	# 查看db大小
	# DB size. 
	for s in ${servers[@]}
	do
		cmd="du -sm $db_path"
		echo "$cmd"
		if [[ $mydebug == "true" ]]; then
			echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[s]}"
			echo "exec on node $s: " "$cmd >& $results/server-$s-db-disk-space"
		else
			ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[s]} "$cmd >& $results/server-$s-db-disk-space"
		fi
	done

	echo "getting stoc file size"
	for s in ${servers[@]}
	do
		cmd="du -sm $cc_stoc_files_path"
		echo "$cmd"
		if [[ $mydebug == "true" ]]; then
			echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[s]}"
			echo "exec on node $s: " "$cmd >& $results/server-$s-rtable-disk-space"
		else
			ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[s]} "$cmd >& $results/server-$s-rtable-disk-space"
		fi		
	done

	echo "killing experiment process"
    for m in ${machines[@]}
    do
    	echo "kill java at $m"
		if [[ $mydebug == "true" ]]; then
			echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]}"
			echo "exec on node $m: " "sudo killall leveldb_main nova_shared_main nova_multi_thread_compaction nova_server_main java collectl sar"
		else
			ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[m]} "sudo killall leveldb_main nova_shared_main nova_multi_thread_compaction nova_server_main java collectl sar"
		fi 	
    done

    dir="$exp_results_dir/$result_dir_name"
    echo "Save to $dir..."
    rm -rf $dir
    mkdir -p $dir
    chmod -R 777 $dir

	echo "getting logs"
	# DB logs.
    server_id=0
	for s in ${servers[@]}
	do
		if [[ $mydebug == "true" ]]; then
			echo "ssh prefix: " "ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[s]}"
			echo "exec on node $s: " "mkdir -p $results/server-$server_id-dblogs/ && cp -r $db_path/*/LOG* $results/server-$server_id-dblogs/"
		else
			ssh -oStrictHostKeyChecking=no -t ${ssh_nodes[s]} "mkdir -p $results/server-$server_id-dblogs/ && cp -r $db_path/*/LOG* $results/server-$server_id-dblogs/"
		fi
		# ssh -oStrictHostKeyChecking=no -t $s "rm -rf $db_path && rm -rf $cc_stoc_files_path"
		server_id=$((server_id+1))
	done

	echo "transferring to main node"
    for m in ${machines[@]}
    do
		if [[ $mydebug == "true" ]]; then
			echo "exec: scp -r ${ssh_nodes[m]}:$results/* $dir"
		else
			scp -r ${ssh_nodes[m]}:$results/* $dir
		fi
    done
    sleep 10
}

run_bench
echo "processing results"
if [[ $mydebug == "true" ]]; then
	echo "exec: sudo python /home/yuhang/NovaLSM/scripts/exp/parse_ycsb_nova_leveldb.py $nmachines $exp_results_dir > stats_tutorial_out"
else
	sudo python /home/yuhang/NovaLSM/scripts/exp/parse_ycsb_nova_leveldb.py $nmachines $exp_results_dir > stats_tutorial_out
fi

