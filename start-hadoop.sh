#!/bin/bash

common="04 05 06 07 08 09"

ssh teng9@fa16-cs425-g06-01.cs.illinois.edu '$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode'
ssh teng9@fa16-cs425-g06-02.cs.illinois.edu '$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager'
ssh teng9@fa16-cs425-g06-03.cs.illinois.edu '$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver'

for i in 02 03 $common 10
do
    ssh teng9@fa16-cs425-g06-$i.cs.illinois.edu '$HADOOP_PREFIX/sbin/start-dfs.sh' &
done

wait

for i in $common 10
do
    ssh teng9@fa16-cs425-g06-$i.cs.illinois.edu '$HADOOP_PREFIX/sbin/start-yarn.sh' &
done

wait

