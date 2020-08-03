package com.example.kafka.web;

import com.example.kafka.service.ConsumerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConsumerController {
    private final ConsumerService consumerService;

    public ConsumerController(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @GetMapping(path = "/consumer/poll")
    public void poll() {
        consumerService.pollOnce();
    }
}
