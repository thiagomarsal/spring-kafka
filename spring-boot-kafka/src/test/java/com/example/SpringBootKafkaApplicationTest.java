package com.example;

import com.example.consumer.Consumer;
import com.example.producer.Producer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringBootKafkaApplicationTest {

    private static String[] TOPICS = {"boot.t", "helloworld.t", "topic1"};

    @Autowired
    private Producer producer;

    @Autowired
    private Consumer consumer;

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, TOPICS);

    @Test
    public void testSendMessage() throws Exception {
        producer.send(TOPICS[0], "Hello Boot!");
        producer.send(TOPICS[1], "Hello World!");
        producer.send(TOPICS[2], "Hello Topic1!");

        consumer.getLatch().await(5, TimeUnit.SECONDS);
        assertThat(consumer.getLatch().getCount()).isEqualTo(0);
    }
}
