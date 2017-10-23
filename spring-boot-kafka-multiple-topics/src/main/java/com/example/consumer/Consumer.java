package com.example.consumer;

import com.example.model.Animal;
import com.example.model.Car;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private CountDownLatch latch = new CountDownLatch(2);

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(topics = "${kafka.topic.topic1}")
    public void consumerTopic1(Car consumerRecord) {
        LOGGER.info("received payload='{}'", consumerRecord);
        latch.countDown();
    }

    @KafkaListener(topics = "${kafka.topic.topic2}")
    public void consumerTopic2(Animal consumerRecord) {
        LOGGER.info("received payload='{}'", consumerRecord);
        latch.countDown();
    }
}
