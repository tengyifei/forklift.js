#!/bin/bash

common="01 02 03 04 05 06 07 08 09"

ssh teng9@fa16-cs425-g06-01.cs.illinois.edu '$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs stop namenode' &
ssh teng9@fa16-cs425-g06-02.cs.illinois.edu '$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR stop resourcemanager' &
ssh teng9@fa16-cs425-g06-03.cs.illinois.edu '$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR stop historyserver' &

for i in $common 10
do
    ssh teng9@fa16-cs425-g06-$i.cs.illinois.edu '$HADOOP_PREFIX/sbin/stop-dfs.sh' &
    ssh teng9@fa16-cs425-g06-$i.cs.illinois.edu '$HADOOP_PREFIX/sbin/stop-yarn.sh' &
done

wait

