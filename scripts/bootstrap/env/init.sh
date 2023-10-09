#!/bin/bash

basedir="/home/yuhang/NovaLSM"
numServers=$1
#prefix="h"

# 5 nodes in total update machine in use
nodes=("192.168.98.74" "192.168.98.73" "192.168.98.70" "192.168.98.53" "192.168.98.52")

for ((i=0; i<numServers; i++)); do
    echo "*******************************************"
    echo "*******************************************"
    echo "******************* node$i ********************"
    echo "*******************************************"
    echo "*******************************************"
    ssh -oStrictHostKeyChecking=no "yuhang@${nodes[i]}" "sudo apt-get update"
    ssh -oStrictHostKeyChecking=no "yuhang@${nodes[i]}" "sudo apt-get --yes install screen"
	ssh -n -f -oStrictHostKeyChecking=no "yuhang@${nodes[i]}" screen -L -S env1 -dm "$basedir/scripts/bootstrap/env/setup-all.sh"
done

sleep 10

sleepcount="0"

for ((i=0;i<numServers;i++)); 
do
	while ssh -oStrictHostKeyChecking=no  "yuhang@${nodes[i]}" "screen -list | grep -q env1"
	do 
		((sleepcount++))
		sleep 10
		echo "waiting for node$i "
	done

done

echo "init env took $((sleepcount/6)) minutes"
