

HOME="/home/yuhang/NovaLSM/YCSB-Nova"
HOME="/tmp/YCSB-Nova"
SRC="jdbc/src/main/java/com/yahoo/ycsb/db"
MEMSRC="jdbc/src/main/java/com/meetup/memcached"
END="$1"
workloads="$2"
suffix="Nova.bg-PG0.apt.emulab.net"
#suffix="edbt.BG.emulab.net"
# CLIENT_NODE="node-0"

for ((i=0;i<END;i++)); do
    echo "building server on node $i"
    
    scp -o StrictHostKeyChecking=no -r workloads/* haoyu@node-$i.$suffix:$HOME/workloads/
    if [[ "$workloads" == "workload_only" ]]; then
    	continue
    fi
    scp -o StrictHostKeyChecking=no bin/ycsb haoyu@node-$i.$suffix:$HOME/bin/ycsb
    scp -o StrictHostKeyChecking=no jdbc/pom.xml haoyu@node-$i.$suffix:$HOME/jdbc/pom.xml
    scp -o StrictHostKeyChecking=no -r $SRC/*.java haoyu@node-$i.$suffix:$HOME/$SRC/
    scp -o StrictHostKeyChecking=no -r $MEMSRC/*.java haoyu@node-$i.$suffix:$HOME/$MEMSRC/

	ssh -oStrictHostKeyChecking=no haoyu@node-$i.$suffix "cd $HOME && mvn -pl com.yahoo.ycsb:jdbc-binding -am clean package -DskipTests" &
	# sleep 1
done

sleep 2000

# scp rdma_bench_o2m haoyu@node-0.Nova.bg-PG0.apt.emulab.net:$HOME/
# scp rdma_bench_m2m haoyu@node-0.Nova.bg-PG0.apt.emulab.net:$HOME/
# scp nova_mem_server haoyu@node-0.Nova.bg-PG0.apt.emulab.net:$HOME/
