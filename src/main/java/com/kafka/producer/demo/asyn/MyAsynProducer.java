package com.kafka.producer.demo.asyn;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class MyAsynProducer {
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "192.168.8.137:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(kafkaProperties);
        ProducerRecord record = new ProducerRecord("test", "name2", "water asyn");
        try {
            //need to call get method to send msg
            producer.send(record, new ProducerCallback()).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class ProducerCallback implements Callback{

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            System.out.println("finish");
            if(exception != null){
                //need to log the exception
                exception.printStackTrace();
            }
        }
    }

}
