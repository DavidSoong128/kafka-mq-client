package com.horizon.test.producer;

import com.horizon.mqclient.api.Message;
import com.horizon.mqclient.core.producer.KafkaClientProducer;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/15 10:36
 * @see
 * @since : 1.0.0
 */
public class KafkaProducerTest {

    public static void main(String[] args){
        KafkaClientProducer producer = KafkaClientProducer.kafkaClientProducer();
        StringBuilder builder = new StringBuilder();
        for(int i=0;i<=100;i++){
            Message message = new Message("dabao",("message"+i).getBytes());
            producer.send(message);
        }
        producer.close();
    }
}
