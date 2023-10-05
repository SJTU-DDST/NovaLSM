#!/bin/bash

basedir="/home/yuhang/NovaLSM"
numServers=$1
#prefix="h"

nodes=("yuhang@192.168.98.73" "yuhang@192.168.98.74")

for ((i=0; i<numServers; i++)); do
    echo "*******************************************"
    echo "*******************************************"
    echo "******************* node$i ********************"
    echo "*******************************************"
    echo "*******************************************"
    ssh -oStrictHostKeyChecking=no "${nodes[i]}" "sudo apt-get update"
    ssh -oStrictHostKeyChecking=no "${nodes[i]}" "sudo apt-get --yes install screen"
	ssh -n -f -oStrictHostKeyChecking=no "${nodes[i]}" screen -L -S env1 -dm "$basedir/scripts/env/setup-all.sh"
done

sleep 10

sleepcount="0"

for ((i=0;i<numServers;i++)); 
do
	while ssh -oStrictHostKeyChecking=no  "${nodes[i]}" "screen -list | grep -q env1"
	do 
		((sleepcount++))
		sleep 10
		echo "waiting for node$i "
	done

done

echo "init env took $((sleepcount/6)) minutes"
