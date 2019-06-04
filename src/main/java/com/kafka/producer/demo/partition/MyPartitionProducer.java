package com.kafka.producer.demo.partition;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class MyPartitionProducer {
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.8.142:9092");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //传递给MyPartition中的configure方法
        kafkaProperties.put("author", "water");
        //指定partition
        kafkaProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartition.class.getCanonicalName());
        KafkaProducer producer = new KafkaProducer(kafkaProperties);
        ProducerRecord record = new ProducerRecord("test2", "water", "water partition");
        try {
            //想看到发送消息的效果，可以自动调用get方法，因为send方法不回让消息立刻发送出去的，而是累计到一定大小或者一定时间间隔后才进行发送，目的是提高吞吐量
            RecordMetadata result = (RecordMetadata) producer.send(record).get();
            System.out.println(result.partition());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
