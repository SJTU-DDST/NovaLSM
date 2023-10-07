#!/bin/bash

# this script is deserted because I have set ssh
# END=$1 #这里第一个参数是node的个数,也没什么用了
REMOTE_HOME="/home/yuhang"
setup_script="$REMOTE_HOME/NovaLSM/scripts/bootstrap/env"
limit_dir="$REMOTE_HOME/NovaLSM/scripts"
LOCAL_HOME="/home/yuhang/NovaLSM/scripts/bootstrap"

host74="192.168.98.74" #74
host73="192.168.98.73" #73

# host="Nova.bg-PG0.apt.emulab.net"
# host="Nova.bg-PG0.utah.cloudlab.us"
# scp -r $LOCAL_HOME/* haoyu@node-0.${host}:/proj/bg-PG0/haoyu/scripts/
# scp - r $LOCAL_HOME/* yuhang@${host73}:

# for ((i=0;i<END;i++)); do
echo "building server on node 73"
ssh -oStrictHostKeyChecking=no yuhang@${host73} "sudo bash $setup_script/setup-ssh.sh"
echo "building server on node 74"
ssh -oStrictHostKeyChecking=no yuhang@${host74} "sudo bash $setup_script/setup-ssh.sh"
# done

# not changing server limit
#
#for ((i=0;i<END;i++)); do
#    echo "building server on node $i"
#    ssh -oStrictHostKeyChecking=no haoyu@node-$i.${host} "sudo cp $limit_dir/ulimit.conf /etc/systemd/user.conf"
#    ssh -oStrictHostKeyChecking=no haoyu@node-$i.${host} "sudo cp $limit_dir/sys_ulimit.conf /etc/systemd/system.conf"
#    ssh -oStrictHostKeyChecking=no haoyu@node-$i.${host} "sudo cp $limit_dir/limit.conf /etc/security/limits.conf"
#    ssh -oStrictHostKeyChecking=no haoyu@node-$i.${host} "sudo reboot"
#done
