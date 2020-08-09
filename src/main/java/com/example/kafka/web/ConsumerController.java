//package com.example.kafka.web;
//
//import com.example.kafka.service.ConsumerService;
//import org.springframework.web.bind.annotation.GetMapping;
//import org.springframework.web.bind.annotation.PostMapping;
//import org.springframework.web.bind.annotation.RequestBody;
//import org.springframework.web.bind.annotation.RestController;
//
//import java.util.Properties;
//
//@RestController
//public class ConsumerController {
//    private final ConsumerService consumerService;
//
//    public ConsumerController(ConsumerService consumerService) {
//        this.consumerService = consumerService;
//    }
//
//    @GetMapping(path = "/consumer/poll")
//    public void poll() {
//        this.consumerService.pollOnce();
//    }
//
//    @PostMapping(path = "/consumer/config")
//    public void config(@RequestBody Properties properties) {
//        this.consumerService.reConfigureConsumer(properties);
//    }
//}
