#!/bin/bash

mkdir generatecorpuses_class;

# Compile MapReduce java code.
javac -classpath ${HADOOP_HOME}/hadoop-core.jar -d generatecorpuses_classes GenerateCorpuses.java && jar -cvf generatecorpuses.jar -C generatecorpuses_classes/ .

# Remove old output directory.
hadoop fs -rmr /mnt/corpuses-output/;
rm /mnt/output/*;

# Execute MapReduce job.
hadoop jar generatecorpuses.jar com.yzong.ccproj4.GenerateCorpuses /mnt/corpuses-input/ /mnt/corpuses-output/;

# Copy back the output from HDFS to local FS.
hadoop fs -copyToLocal /mnt/corpuses-output/* /mnt/output/

# Combine the output files and sort the entries.
cat part* > corpuses
sort corpuses > sorted-corpuses

echo Done!
