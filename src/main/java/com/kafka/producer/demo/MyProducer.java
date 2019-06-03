package com.kafka.producer.demo;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.Future;

public class MyProducer {

    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "192.168.8.137:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProperties.put("request.timeout.ms", "160000");
        KafkaProducer producer = new KafkaProducer(kafkaProperties);
        ProducerRecord record = new ProducerRecord("test", "name", "water");
        try {
            Future result = producer.send(record);
            //想看到发送消息的效果，可以自动调用get方法，因为send方法不回让消息立刻发送出去的，而是累计到一定大小或者一定时间间隔后才进行发送，目的是提高吞吐量
            System.out.println(result.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
