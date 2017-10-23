package com.example.producer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ProducerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public Map<String, Object> producerConfigs() {
        final Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        final KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory());
        template.setProducerListener(new MyListener());
        template.setMessageConverter(new StringJsonMessageConverter());

        return template;
    }

    @Bean
    public Producer producer() {
        return new Producer();
    }

    public class MyListener implements ProducerListener<String, String> {

        private final Logger LOGGER = LoggerFactory.getLogger(MyListener.class);

        @Override
        public void onSuccess(String topic, Integer partition, String key, String value, RecordMetadata recordMetadata) {
            LOGGER.info("onSuccess topic='{}', key='{}', value='{}'", topic, key, value);
        }

        @Override
        public void onError(String topic, Integer partition, String key, String value, Exception exception) {
            LOGGER.info("onError topic='{}', key='{}', value='{}'", topic, key, value);
        }

        @Override
        public boolean isInterestedInSuccess() {
            return false;
        }
    }
}
