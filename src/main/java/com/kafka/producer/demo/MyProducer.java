package com.kafka.producer.demo;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.Future;

public class MyProducer {

    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "192.168.8.136:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProperties.put("request.timeout.ms", "160000");
        KafkaProducer producer = new KafkaProducer(kafkaProperties);
        ProducerRecord record = new ProducerRecord("test", "name", "water");
        try {
            Future result = producer.send(record);
            System.out.println(result.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
