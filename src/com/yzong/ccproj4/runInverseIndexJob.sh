#!/bin/bash

# Compile MapReduce java code.
javac -classpath ${HADOOP_HOME}/hadoop-core.jar -d inverseindex_classes InverseIndex.java && jar -cvf inverseindex.jar -C inverseindex_classes/ .

# Remove old output directory.
hadoop fs -rmr /mnt/gutenberg-out/;

# Execute MapReduce job.
hadoop jar inverseindex.jar com.yzong.ccproj4.InverseIndex /mnt/gutenberg-txt/ /mnt/gutenberg-out/;

