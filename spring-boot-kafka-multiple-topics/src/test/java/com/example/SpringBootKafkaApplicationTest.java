package com.example;

import com.example.consumer.Consumer;
import com.example.model.Animal;
import com.example.model.Car;
import com.example.producer.Producer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringBootKafkaApplicationTest {

    private static String[] TOPICS = {"topic1", "topic2"};

    @Autowired
    private Producer producer;

    @Autowired
    private Consumer consumer;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, TOPICS);

    @Before
    public void setUp() throws Exception {
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafka.getPartitionsPerTopic());
        }
    }

    @Test
    public void testSendMessage() throws Exception {
        producer.send(TOPICS[0], new Car("Ferrari"));
        producer.send(TOPICS[1], new Animal("Dog"));

        consumer.getLatch().await(10, TimeUnit.SECONDS);
        assertThat(consumer.getLatch().getCount()).isEqualTo(0);
    }
}
