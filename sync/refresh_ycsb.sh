#!/bin/bash

basedir="/home/yuhang/NovaLSM"
#ssh版本的node
#ssh_nodes=("yuhang@192.168.98.74" "yuhang@192.168.98.73" "yuhang@192.168.98.70" "yuhang@192.168.98.53" "yuhang@192.168.98.52")
ssh_nodes=("yuhang@192.168.98.74" "yuhang@192.168.98.70" "yuhang@192.168.98.53")
#ip的node，就先用74 73 70 53 52
#ip_nodes=("192.168.98.74" "192.168.98.73" "192.168.98.70" "192.168.98.53" "192.168.98.52")
ip_nodes=("192.168.98.74" "192.168.98.70" "192.168.98.53")

ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.74" "rm -rf /tmp/YCSB-Nova"
ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.74" "cp -r "$basedir/YCSB-Nova/" /tmp/"
ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.74" "cd /tmp/YCSB-Nova && mvn -pl com.yahoo.ycsb:jdbc-binding -am clean package -DskipTests"

ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.70" "rm -rf /tmp/YCSB-Nova"
ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.70" "cp -r "$basedir/YCSB-Nova/" /tmp/"
ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.70" "cd /tmp/YCSB-Nova && mvn -pl com.yahoo.ycsb:jdbc-binding -am clean package -DskipTests"

ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.53" "rm -rf /tmp/YCSB-Nova"
ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.53" "cp -r "$basedir/YCSB-Nova/" /tmp/"
ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.53" "cd /tmp/YCSB-Nova && mvn -pl com.yahoo.ycsb:jdbc-binding -am clean package -DskipTests"