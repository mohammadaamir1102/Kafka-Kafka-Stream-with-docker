package com.aamir.producer.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/producer")
@Slf4j
public class ProducerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping
    public CompletableFuture<ResponseEntity<String>> sendMessage(@RequestBody String message) {
        log.info("message: {}", message);

        return kafkaTemplate.send("test-topic", message)
                .thenApply(result -> {
                    String response = "Sent Message= " + message + " with offset= " + result.getRecordMetadata().offset();
                    log.info("Partition is {}", result.getRecordMetadata().partition());
                    return ResponseEntity.ok(response);
                })
                .exceptionally(ex -> {
                    log.error("Unable to send message= {} due to {}", message, ex.getMessage());
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body("Failed to send message: " + ex.getMessage());
                });
    }

}
