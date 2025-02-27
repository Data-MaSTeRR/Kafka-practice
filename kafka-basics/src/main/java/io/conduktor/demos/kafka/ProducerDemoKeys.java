package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to localhost - Broker와 연결
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set producer properties - 바이트로 직렬화
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j=0; j<2; j++) {
            for (int i = 0; i < 10; i++) {

                String topic = "demo-java";
                String key = "id_" + i;
                String value = "hello world" + i;


                // create a Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                // send data - 비동기 전송 + 콜백추가 (버퍼의 모든 메시지 전송을 보장하지 않음 -> flush의 필요성)
                producer.send(producerRecord, (recordMetadata, e) -> {
                    // e -> callback()
                    if (e == null) { // record was successfully sent
                        log.info("Key: " + key + " | Partition: " + recordMetadata.partition());
                    } else {
                        log.error("Error with producing", e);
                    }
                });
            }
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // tell the producer to send all data and block until done -- sync / 프로듀서 내부의 전송 버퍼에 남은 메시지를 모두 전송하고 완료될 때까지 기다립니다.
        producer.flush();

        // flush and close the producer - 남은 메시지 전송이 완료되고, 관련 리소스가 정리
        producer.close();

    }
}