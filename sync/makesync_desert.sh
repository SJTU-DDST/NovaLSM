#!/bin/bash

fmtbasedir="/home/yuhang/fmt-6.1.2"
basedir="/home/yuhang/NovaLSM"
numServers=$1
#prefix="h"

# 5 nodes in total update machine in use
nodes=("192.168.98.74"  "192.168.98.70" "192.168.98.53" "192.168.98.73"  "192.168.98.52")

for ((i=0; i<numServers; i++)); do
    echo "*******************************************"
    echo "*******************************************"
    echo "******************* node$i ********************"
    echo "*******************************************"
    echo "*******************************************"
    
    ssh -oStrictHostKeyChecking=no "yuhang@${nodes[i]}" "cd $fmtbasedir && cmake . && make -j32 && sudo make install"
    ssh -oStrictHostKeyChecking=no "yuhang@${nodes[i]}" "cd $basedir && cmake . && make clean && make -j4 "
    # echo "******************* node$i syned********************"
done

# cd /home/yuhang/fmt-6.1.2 && cmake . && make -j32 && sudo make install
# cd /home/yuhang/NovaLSM && cmake . && make clean && make -j16