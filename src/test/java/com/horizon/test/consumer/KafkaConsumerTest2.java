package com.horizon.test.consumer;

import com.horizon.mqclient.core.consumer.KafkaHighConsumer;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/15 11:06
 * @see
 * @since : 1.0.0
 */
public class KafkaConsumerTest2 {

    public static void main(String[] args){
        KafkaHighConsumer kafkaHighConsumer = KafkaHighConsumer.kafkaHighConsumer();
        String topic = "send";
        kafkaHighConsumer.subscribe(topic,new TestMessageHandler());
        try {
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            kafkaHighConsumer.close();
        }
    }

}
