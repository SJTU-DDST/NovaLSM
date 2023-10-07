#!/bin/bash

sudo apt-get update
sudo apt-get --yes install maven
# sudo apt-get install -y openjdk-8-jdk
# sudo update-alternatives --config java
# sudo update-alternatives --config javac

sudo apt-get install -y memcached
sudo apt-get install -y sysstat

sudo apt-get install -y mysql-server
sudo ufw allow mysql
systemctl start mysql

git clone https://github.com/scdblab/YCSB-Client-MySQL-memcached.git
cd YCSB-Client-MySQL-memcached
mvn -pl com.yahoo.ycsb:jdbc-binding -am clean package -DskipTests

# Perform the following steps manually to understand more about the benchmark.

# 1. create a mysql user.
# sudo mysql -u root
# CREATE USER 'user'@'localhost' IDENTIFIED BY '123456';
# GRANT ALL PRIVILEGES on *.* to 'user'@'localhost' IDENTIFIED BY '123456';
# FLUSH PRIVILEGES;
# sudo service mysql restart

# 2. create a schema. 
# mysql -u user -p
# create schema YCSB;

# 3. create the table.
# use YCSB;
# CREATE TABLE usertable (
# 	YCSB_KEY VARCHAR(255) PRIMARY KEY,
# 	FIELD0 TEXT, FIELD1 TEXT,
# 	FIELD2 TEXT, FIELD3 TEXT,
# 	FIELD4 TEXT, FIELD5 TEXT,
# 	FIELD6 TEXT, FIELD7 TEXT,
# 	FIELD8 TEXT, FIELD9 TEXT
# );
