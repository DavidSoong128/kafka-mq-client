package com.horizon.test.producer;

import com.horizon.mqclient.api.Message;
import com.horizon.mqclient.core.consumer.KafkaHighConsumer;
import com.horizon.mqclient.core.producer.KafkaClientProducer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/20 9:57
 * @see
 * @since : 1.0.0
 */
public class ProducerLittleDataTest {

    public static void main(String[] args){
        KafkaClientProducer kafkaClientProducer = KafkaClientProducer.kafkaClientProducer();
        Map<String,Object> configMap = new HashMap<>();
//        configMap.put("send.buffer.bytes",1);
//        configMap.put("max.request.size",84);
//        KafkaClientProducer kafkaClientProducer = new KafkaClientProducer(configMap);
        Message message = new Message("send","message".getBytes());

        kafkaClientProducer.send(message);
//        kafkaClientProducer.close();
    }
}
