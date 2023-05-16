package com.memorynotfound.kafka.consumer;


import com.memorynotfound.kafka.model.Payload;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.nio.charset.Charset;

import static java.util.Objects.nonNull;

@EnableKafka
@Configuration
public class ListenerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    Charset charset = StandardCharsets.UTF_16;

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "foo");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
    @Bean
    public Map<String, Object> prodConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "prod");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
    @Bean
    public Map<String, Object> testConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }


    //--------------------------------------- DEFAULT FACTORY----------------------------------------------------
    @Bean
    public ConsumerFactory<String, Payload> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(),new StringDeserializer(), new JsonDeserializer<>(Payload.class));
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Payload>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Payload> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }


    //--------------------------------------------PROD FACTORY -----------------------------------------------

    @Bean
    public ConsumerFactory<String, Payload> prodConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(prodConsumerConfigs(),new StringDeserializer(), new JsonDeserializer<>(Payload.class));
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Payload>> prodKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Payload> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(prodConsumerFactory());
        factory.setAckDiscarded(true);
        factory.setRecordFilterStrategy(consumerRecord -> {
            if(nonNull(consumerRecord.headers())) {
                System.out.println("prodKafkaListenerContainerFactory header iteration started....");
                Iterator<Header> iterator = consumerRecord.headers().iterator();
                while(iterator.hasNext()) {
                    Header header = iterator.next();
                    String headerValue = new String(header.value(), charset);
                    String headerkey = new String(header.key().getBytes(), charset);
                    if(headerValue.contains("PROD")) {
                        System.out.println("prodKafkaListenerContainerFactory msg consumed");
                        return false;
                    }else{
                        System.out.println("prodKafkaListenerContainerFactory()  msg no 'TEST' in headers");
                    }
                }
            }
            System.out.println("prodKafkaListenerContainerFactory() msg skipped");
            return true;
        });

        return factory;
    }



    //--------------------------------------------TEST FACTORY -----------------------------------------------
    @Bean
    public ConsumerFactory<String, Payload> testConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(testConsumerConfigs(),new StringDeserializer(), new JsonDeserializer<>(Payload.class));
    }
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Payload>> testKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Payload> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(testConsumerFactory());
        factory.setAckDiscarded(true);
        factory.setRecordFilterStrategy(consumerRecord -> {
            if(nonNull(consumerRecord.headers())) {
                System.out.println("testKafkaListenerContainerFactory() header iteration started....");
                Iterator<Header> iterator = consumerRecord.headers().iterator();
                while(iterator.hasNext()) {
                    Header header = iterator.next();
                    String headerValue = new String(header.value(), charset);
                    String headerkey = new String(header.key().getBytes(), charset);
                    if(headerValue.contains("TEST")) {
                        System.out.println("testKafkaListenerContainerFactory msg consumed");
                        return false;
                    }else{
                        System.out.println("testKafkaListenerContainerFactory()  msg no 'TEST' in headers");
                    }
                }
            }
            System.out.println("testKafkaListenerContainerFactory() msg skipped");
            return true;
        });

        return factory;
    }


//        factory.setRecordFilterStrategy(consumerRecord -> {
//            for (Header cs: consumerRecord.headers().toArray())
//            {
//                String s = new String(cs.value());
//                if(s.contains("PROD")){
//                    System.out.println("validaando PROD");
//                    return false;
//                }
//            }
//            return true;
//        });


//    @Bean
//    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, User>> testKafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, User> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        factory.setRecordFilterStrategy(consumerRecord -> {
//            for (Header cs: consumerRecord.headers().toArray())
//            {
//                String s = new String(cs.value());
//                if(s.contains("TEST")){
//                    System.out.println("validaando TEST");
//                    return false;
//                }
//            }
//            return true;
//        });
//        return factory;
//    }





    @Bean
    public DefaultKafkaHeaderMapper headerMapper(){
        return new DefaultKafkaHeaderMapper();
    }

}
