#!/bin/bash

# ./gitsync.sh 5
# ./makesync.sh 5
# nodes=("192.168.98.74" "192.168.98.73" "192.168.98.70" "192.168.98.53" "192.168.98.52")
# make clean
# make -j4

cp nova_server_main ./nova/

# ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.73" "cd /home/yuhang/NovaLSM && mkdir nova"
# ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.70" "cd /home/yuhang/NovaLSM && mkdir nova"
# ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.53" "cd /home/yuhang/NovaLSM && mkdir nova"
# ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.52" "cd /home/yuhang/NovaLSM && mkdir nova"

scp nova_server_main     yuhang@192.168.98.73:/home/yuhang/NovaLSM/nova/nova_server_main  &
scp nova_server_main     yuhang@192.168.98.70:/home/yuhang/NovaLSM/nova/nova_server_main  &
scp nova_server_main     yuhang@192.168.98.53:/home/yuhang/NovaLSM/nova/nova_server_main  &
scp nova_server_main     yuhang@192.168.98.52:/home/yuhang/NovaLSM/nova/nova_server_main  &

# scp nova_server_main     yuhang@192.168.98.74:/home/yuhang/NovaLSM/nova_server_main &
scp nova_server_main     yuhang@192.168.98.73:/home/yuhang/NovaLSM/nova_server_main &
scp nova_server_main     yuhang@192.168.98.70:/home/yuhang/NovaLSM/nova_server_main &
scp nova_server_main     yuhang@192.168.98.53:/home/yuhang/NovaLSM/nova_server_main &
scp nova_server_main     yuhang@192.168.98.52:/home/yuhang/NovaLSM/nova_server_main &