package com.example.kafka.web;

import com.example.kafka.service.ProducerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@RestController
public class ProducerController {
    private final ProducerService producerService;

    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @GetMapping(path = "/producer/send")
    public void send() {
        this.producerService.send();
    }

    @PostMapping(path = "/producer/config")
    public void config(@RequestBody Properties properties) {
        this.producerService.reConfigureProducer(properties);
    }
}
