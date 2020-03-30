#!/bin/bash

if [ $1 = '-c' ]
then
	javac -d src/bin/ -cp src/ds/hdfs:src/protobuf-java-3.11.4.jar:src/json-simple-1.1.1.jar src/ds/hdfs/*.java
elif [ $1 = '-client' ]
then
	java -cp src/bin:src/protobuf-java-3.11.4.jar ds.hdfs.Client
elif [ $1 = '-dataNode' ]
then
	java -cp src/bin:src/protobuf-java-3.11.4.jar ds.hdfs.DataNode
elif [ $1 = '-nameNode' ]
then
	java -cp src/bin:src/protobuf-java-3.11.4.jar:src/json-simple-1.1.1.jar ds.hdfs.NameNode
elif [ $1 = '-clean' ]
then
	rm -rf src/bin/* 
fi
