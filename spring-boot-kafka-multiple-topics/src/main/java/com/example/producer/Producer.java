package com.example.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.util.UUID;

public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    @Autowired
    private KafkaTemplate<String, ?> kafkaTemplate;

    public void send(String topic, Object payload) {
        send(topic, UUID.randomUUID().toString(), payload);
    }

    public void send(String topic, String key, Object payload) {
        LOGGER.info("sending topic='{}', key='{}', payload='{}'", topic, key, payload);
        kafkaTemplate.send(MessageBuilder.withPayload(payload).setHeader(KafkaHeaders.TOPIC, topic).build());
    }
}
