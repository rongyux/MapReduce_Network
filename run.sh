#!/bin/bash
#set -e
set -x

machine_name=`uname -n`
user_name=rongyu 
HADOOP_KHAN=/home/users/wuxiaoxu/hadoop/hadoop-client-khan/hadoop
HADOOP_MLAN=/root/rongyu01/hadoop-client/hadoop-client-nmg-mulan/hadoop
HADOOP_TH=$HADOOP_MLAN

SUFFIX_KHAN=hdfs://nmg01-khan-hdfs.dmop.baidu.com:54310
SUFFIX_MLAN=hdfs://nmg01-mulan-hdfs.dmop.baidu.com:54310

date=20170624

data_input=$SUFFIX_MLAN/app/ecom/fcr-important/shitu-log-wise/222_223/${date}/*/*
data_output=$SUFFIX_MLAN/app/ecom/fcr-model/rongyu01/tmp/${date}
#PRIORITY=VERY_HIGH
PRIORITY=NORMAL

function process_log()
{
    #${HADOOP_TH}/bin/hadoop fs -test -e $data_output
    #if [ $? == 0 ];
    #then
     #   ${HADOOP_TH}/bin/hadoop fs -rmr $data_output
    #fi

    ${HADOOP_TH}/bin/hadoop streaming \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.job.map.capacity=2000 \
    -D mapred.job.reduce.capacity=10000 \
    -D mapred.reduce.tasks=3000 \
    -D mapred.map.tasks=1000 \
    -D mapred.map.over.capacity.allowed=true \
    -D mapred.reduce.over.capacity.allowed=true \
    -D mapred.job.name=$user_name/ztc_trip_revised/$date_current \
    -D mapred.job.priority=${PRIORITY} \
    -D abaci.job.reduce.child.memory.mb=20000 \
    -D stream.memory.limit=20000  \
    -D hadoop.hce.memory.limit=20000 \
        -jobconf abaci.is.dag.job=true \
        -jobconf abaci.dag.vertex.num=3 \
        -jobconf abaci.dag.next.vertex.list.0=1 \
        -jobconf abaci.dag.next.vertex.list.1=2 \
        -jobconf mapred.job.map.capacity.0=500\
        -jobconf stream.map.streamprocessor.0="app/bin/python2.7 mapper.py" \
        -jobconf stream.reduce.streamprocessor.1="app/bin/python2.7 reducer.py" \
        -jobconf stream.reduce.streamprocessor.2="app/bin/python2.7 reducer2.py" \
        -jobconf stream.num.map.output.key.fields=2 \
        -jobconf mapred.map.tasks=500 \
        -jobconf stream.num.reduce.output.key.fields=2 \
        -jobconf stream.memory.limit=1000 \
        -jobconf mapred.job.map.capacity=200 \
        -jobconf mapred.job.reduce.capacity=10000 \
        -jobconf mapred.job.name="cpm_bid_relation" \
        -jobconf mapred.reduce.tasks=500\
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -jobconf num.key.fields.for.partition=1 \
    -input ${data_input} \
    -output ${data_output} \
    -mapper "cat" \
    -reducer "cat" \
    -file "mapper.py" \
    -file "reducer.py" \
    -file "reducer2.py" \
    -cacheArchive "/app/ecom/fcr-model/rongyu01/public/python2.7.tgz#app"
}

function main()
{
    #${HADOOP_TH}/bin/hadoop fs -test -e $data_test
    #if [ $? == 0 ];
    #then
     #   ${HADOOP_TH}/bin/hadoop fs -test -e $data_output
     #   if [ $? == 0 ];
      #  then
       #     return;
       # fi
        process_log
       # ${HADOOP_TH}/bin/hadoop fs -test -e $data_output
        #if [ $? == 0 ];
        #then
         #   key=`date "+%s"`
         #   echo  \{\"id\":\"$key\"\,\"key\":\"$key\"\,\"input\":\"$data_output\"\} > xbox_base_done.txt
            
        # ${HADOOP_TH}/bin/hadoop fs -test -e $data_done
        # if [ $? == 0 ];
        # then
        #     ${HADOOP_TH}/bin/hadoop fs -rm $data_done
        # fi
        # ${HADOOP_TH}/bin/hadoop fs -put xbox_base_done.txt  $data_done
        #fi
    #fi
}

main 

