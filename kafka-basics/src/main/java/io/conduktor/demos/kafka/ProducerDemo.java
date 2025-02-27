package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello world");

        // create Producer Properties - Broker와 연결
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set producer properties - 바이트로 직렬화
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a Producer Record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo-java", "Hello world");

        // send data - 비동기 단순 전송 (버퍼의 모든 메시지 전송을 보장하지 않음 -> flush의 필요성)
        producer.send(producerRecord);

        // tell the producer to send all data and block until done -- sync / 프로듀서 내부의 전송 버퍼에 남은 메시지를 모두 전송하고 완료될 때까지 기다립니다.
        producer.flush();

        // flush and close the producer - 남은 메시지 전송이 완료되고, 관련 리소스가 정리
        producer.close();

    }
}