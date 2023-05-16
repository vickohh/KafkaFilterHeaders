package com.memorynotfound.kafka.producer;

import com.memorynotfound.kafka.model.Payload;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Service
public class Sender {

    private static  String ENVIRONMENT;
    private static final Logger LOG = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String, Payload> kafkaTemplate;

    @Value("${app.topic.foo}")
    private String topicFoo;

    @Value("${app.topic.bar}")
    private String topicBar;

//    public void sendFoo(User data){
//
//
//
//       Message<User> message = MessageBuilder
//                .withPayload(data)
//                .setHeader(KafkaHeaders.TOPIC, topicFoo)
//                .setHeader(KafkaHeaders.MESSAGE_KEY, "999")
//                .setHeader(KafkaHeaders.PARTITION_ID, 0)
//                .setHeader(ENVIRONMENT, data.getEnvironment())
//                .build();
//
//        LOG.info("sending message='{}' to topic='{}'", data, topicFoo);
//        kafkaTemplate.send(message);
//    }

    public void sendBar(Payload data){

        ENVIRONMENT = data.getSourceEnv();
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader(ENVIRONMENT, data.getSourceEnv().getBytes(StandardCharsets.UTF_16)));
        headers.add(new RecordHeader("VCK", "victor".getBytes()));

        ProducerRecord<String, Payload> bar = new ProducerRecord<>(topicBar, 0, "111", data, headers);
        LOG.info("sending message='{}' to topic='{}'", data.toString(), topicBar);

        kafkaTemplate.send(bar);
    }

//    public void sendBar(User data){
//
//
//        List<Header> headers = new ArrayList<>();
//        headers.add(new RecordHeader(ENVIRONMENT, data.getEnvironment().getBytes()));
//
//        ProducerRecord<String, User> bar = new ProducerRecord<>(topicBar, 0, "111", data, headers);
//        LOG.info("sending message='{}' to topic='{}'", data.toString(), topicBar);
//
//        kafkaTemplate.send(bar);
//    }
}
