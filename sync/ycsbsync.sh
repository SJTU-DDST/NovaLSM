#!/bin/bash

ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.74" "rm -rf /tmp/YCSB-Nova"
scp -r YCSB-Nova     yuhang@192.168.98.74:/tmp
ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.74" "cd /tmp/YCSB-Nova && mvn -pl com.yahoo.ycsb:jdbc-binding -am clean package -DskipTests"

ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.70" "rm -rf /tmp/YCSB-Nova"
scp -r YCSB-Nova     yuhang@192.168.98.70:/tmp
ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.70" "cd /tmp/YCSB-Nova && mvn -pl com.yahoo.ycsb:jdbc-binding -am clean package -DskipTests"

ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.53" "rm -rf /tmp/YCSB-Nova"
scp -r YCSB-Nova     yuhang@192.168.98.53:/tmp
ssh -oStrictHostKeyChecking=no "yuhang@192.168.98.53" "cd /tmp/YCSB-Nova && mvn -pl com.yahoo.ycsb:jdbc-binding -am clean package -DskipTests"