package com.memorynotfound.kafka.consumer;

import com.memorynotfound.kafka.model.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Service;

@Service
public class Listener {

    private static final Logger LOG = LoggerFactory.getLogger(Listener.class);

//    @KafkaListener(topics = "${app.topic.foo}", containerFactory = "envKafkaListenerContainerFactory")
//    public void receive(@Payload User user,
//                        @Header(KafkaHeaders.OFFSET) Long offset,
//                        @Header(KafkaHeaders.CONSUMER) KafkaConsumer<String, String> consumer,
//                        @Header(KafkaHeaders.TIMESTAMP_TYPE) String timestampType,
//                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
//                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId,
//                        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String messageKey,
//                        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp,
//                        @Header("Environment") String customHeader) {
//
//        LOG.info("- - - - - - - - - - - - - - -");
//        LOG.info("received message FIRST CONSUMER A='{}'", user.toString());
//        LOG.info("consumer: {}", consumer);
//        LOG.info("topic: {}", topic);
//        LOG.info("message key: {}", messageKey);
//        LOG.info("partition id: {}", partitionId);
//        LOG.info("offset: {}", offset);
//        LOG.info("timestamp type: {}", timestampType);
//        LOG.info("timestamp: {}", timestamp);
//        LOG.info("Environment: {}", customHeader);
//    }

//    @KafkaListener(topics = "${app.topic.bar}")
//    public void receive(@Payload User user,
//                        @Headers MessageHeaders messageHeaders) {
//
//        LOG.info("- - - - - - - - - - - - - - -");
//        LOG.info("received message  -- SCOPE: TEST ONLY--SECOND CONSUMER A ='{}'", user.toString());
//        messageHeaders.keySet().forEach(key -> {
//            Object value = messageHeaders.get(key);
//            if (key.equals("Environment")){
//                LOG.info("{}: {}", key, new String((byte[])value));
//            } else {
//                LOG.info("{}: {}", key, value);
//            }
//        });
//
//    }
//    @KafkaListener(topics = "${app.topic.bar}",containerFactory = "prodKafkaListenerContainerFactory")
//    public void receivePROD(@org.springframework.messaging.handler.annotation.Payload Payload payload,
//                        @Headers MessageHeaders messageHeaders) {
//
//        LOG.info("- - - - - - - - - - - - - - -");
//        LOG.info("received message -- SCOPE: -- PROD ONLY-- PROD CONSUMER ='{}'", payload.toString());
//        messageHeaders.keySet().forEach(key -> {
//            Object value = messageHeaders.get(key);
//            if (key.equals("Environment")){
//                LOG.info("{}: {}", key, new String((byte[])value));
//            } else {
//                LOG.info("{}: {}", key, value);
//            }
//        });
//    }

    @KafkaListener(topics = "${app.topic.bar}",containerFactory = "testKafkaListenerContainerFactory")
    public void receiveTEST(@org.springframework.messaging.handler.annotation.Payload Payload payload,
                         @Headers MessageHeaders messageHeaders) {

        LOG.info("- - - - - - - - - - - - - - -");
        LOG.info("received message -- SCOPE: -- TEST ONLY-- TEST CONSUMER ='{}'", payload.toString());
        messageHeaders.keySet().forEach(key -> {
            Object value = messageHeaders.get(key);
            if (key.equals("Environment")){
                LOG.info("{}: {}", key, new String((byte[])value));
            } else {
                LOG.info("{}: {}", key, value);
            }
        });
    }
}
