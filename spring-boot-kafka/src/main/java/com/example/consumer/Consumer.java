package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private CountDownLatch latch = new CountDownLatch(1);

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(topics = {"${kafka.topic.boot}", "${kafka.topic.helloworld}", "${kafka.topic.topic1}"})
    public void consume(ConsumerRecord<String, String> consumerRecord) {
        LOGGER.info("received payload='{}'", consumerRecord);
        latch.countDown();
    }
}
