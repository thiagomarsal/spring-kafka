package com.example.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send(String topic, String payload) {
        send(topic, UUID.randomUUID().toString(), payload);
    }

    public void send(String topic, String key, String payload) {
        LOGGER.info("sending topic='{}', key='{}', payload='{}'", topic, key, payload);
        kafkaTemplate.send(topic, key, payload);
    }
}
