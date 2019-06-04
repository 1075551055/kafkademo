package com.kafka.producer.demo.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class MyPartition implements Partitioner {
    private String author;
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int partitionSize = partitionInfos.size();
        if(key == null || !(key instanceof String)){
            throw new InvalidRecordException("We expect all message to have customer name as key");
        }
        if(((String)key).equals(this.author)){
            //分配到最后一个分区
            return partitionSize - 1;
        }
        //
        return (Math.abs(Utils.murmur2(keyBytes)) % (partitionSize - 1));
    }

    public void close() {

    }

    //当partition类初始化的时候会触发该方法，这里的configs是从new KafkaProducer(properties)中的properties参数传递过来的
    public void configure(Map<String, ?> configs) {
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            if(entry.getKey().equals("author")){
                this.author = (String) entry.getValue();
            }
        }

    }
}
