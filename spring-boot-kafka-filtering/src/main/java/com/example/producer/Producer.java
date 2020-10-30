package com.example.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
public class Producer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send(String topic, String payload) {
        send(topic, UUID.randomUUID().toString(), payload);
    }

    public void send(String topic, String key, String payload) {
        log.info("sending topic='{}', key='{}', payload='{}'", topic, key, payload);
        kafkaTemplate.send(topic, key, payload);
    }
}