package io.conduktor.demos.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        String groupId = "my-java-application";
        String topic = "demo-java";

        // create Producer Properties
        Properties properties = new Properties();

        // connect to localhost - Broker와 연결
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // create consumer configs
        // set producer properties - 바이트로 역직렬화
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        // none, earliest, latest
        properties.setProperty("auto.offset.reset", "earliest");

        // create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for data
        while (true) {

            log.info("polling");

            // kafka data 기다리는 시간 -> kafka 과부하 방지
            ConsumerRecords<String,String> consumerRecords =
                    consumer.poll(Duration.ofMillis(1000));

            // for문에서 인자 하나씩이니, ConsumerRecord 단수형
            for (ConsumerRecord<String, String> consumerRecord :consumerRecords) {
                log.info("Key: " + consumerRecord.key() + ", Value: " + consumerRecord.value());
                log.info("Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset());
            }
        }
    }
}