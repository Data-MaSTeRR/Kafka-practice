package io.conduktor.demos.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

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

        // partition-consumer rebalancing 전략 선택 -> CooperativeStickyAssignor best choice
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // consumer group 내 groupId를 통한 정적할당 -> partition-consumer 캐시없이 관계 유지 가능
        // properties.setProperty("group.instance.id", "my-java-application-instance-1");

        // create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread - 현재 main 메서드라 mainThread
        final Thread mainThread = Thread.currentThread();

        // add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            // poll for data
            while (true) {

                // 아래 실시간으로 polling하는 것을 보고 싶으면 코드 입력
                //log.info("polling");

                // kafka data 기다리는 시간 -> kafka 과부하 방지
                ConsumerRecords<String,String> consumerRecords =
                        consumer.poll(Duration.ofMillis(1000));

                // for문에서 인자 하나씩이니, ConsumerRecord 단수형
                for (ConsumerRecord<String, String> consumerRecord :consumerRecords) {
                    log.info("Key: " + consumerRecord.key() + ", Value: " + consumerRecord.value());
                    log.info("Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.info("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer & this will also commit offsets
            log.info("The consumer is now gracefully shutdown");
        }
    }
}