package com.kafka.consumer.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class MyConsumer {
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.8.144:9092");
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "myconsumer2");
        // 如果想从最开始消费，则需要设置该属性为earliest,默认是latest,所以默认情况下当有消息进来的时候,poll方法才能获取到消息
//        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer consumer = new KafkaConsumer(kafkaProperties);
        consumer.subscribe(Arrays.asList("test", "test2"));
//        consumer.subscribe(Pattern.compile("test.*"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(0);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.key() + ":" + record.value());
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }
}
