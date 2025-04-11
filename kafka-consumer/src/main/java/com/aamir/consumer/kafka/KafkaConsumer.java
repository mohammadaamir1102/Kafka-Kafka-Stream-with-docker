package com.aamir.consumer.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaConsumer {

    @KafkaListener(topics = "test-topic", groupId = "my-group")
    public void consumeTestMessage(String message, Acknowledgment acknowledgment) {
        try {
            log.info("Consuming message: {}", message);
            // Process the message
            // For example, save the message to DB
            acknowledgment.acknowledge();
            // Acknowledge after processing
            //Jab bhi consumer restart hoga, wo last processed offset se messages read karega,
            // aur jab tak message process nahi ho jata, wo commit nahi karega.
        } catch (Exception e) {
            log.error("Failed to process message: {}", message, e);
            // Don't acknowledge so that the message will be retried later
        }
    }

    @KafkaListener(topics = "test-stream-topic", groupId = "my-group")
    public void consumeStreamMessage(String message, Acknowledgment acknowledgment) {
        try {
            log.info("Consuming message in consumeStreamMessage: {}", message);
            // Process the message
            // For example, save the message to DB
            acknowledgment.acknowledge();
            // Acknowledge after processing
            //Jab bhi consumer restart hoga, wo last processed offset se messages read karega,
            // aur jab tak message process nahi ho jata, wo commit nahi karega.
        } catch (Exception e) {
            log.error("Failed to process message: {}", message, e);
            // Don't acknowledge so that the message will be retried later
        }
    }

}
