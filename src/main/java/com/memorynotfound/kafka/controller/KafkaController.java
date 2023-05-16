package com.memorynotfound.kafka.controller;

import com.memorynotfound.kafka.model.Payload;
import com.memorynotfound.kafka.producer.Sender;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;



@RestController
@RequestMapping("v1")
public class KafkaController {

    @Autowired
    private Sender sender;

    @GetMapping("/publish/{name}")
    public String postMessage(@PathVariable("name") final String environment){

        Payload data = new Payload();
        data.setLoanNumber("300456695");
        data.setSourceEnv(environment);
        data.setStatus("PreQual");
        data.setMappingSetID("CREATEAPPLICATION");

        sender.sendBar(data);

        return "Message Published Successfully ok";
    }
}
