package com.horizon.mqclient.core.consumer;

import com.caucho.hessian.io.ValueDeserializer;
import com.horizon.mqclient.common.DefaultConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * kafka client consumer
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 11:54
 * @since : 1.0.0
 */
public class KafkaClientConsumer implements Consumer{

    private Logger logger = LoggerFactory.getLogger(KafkaClientConsumer.class);

    private KafkaConsumer kafkaConsumer;

    private Map<String,Object> consumerConfigMap;

    private KafkaClientConsumer(){
        //1: init kafka consumer
        initKafkaConsumer();
        //2: init kafka client
        initKafkaClient();
    }

    private static class ConsumerHolder{
        private static KafkaClientConsumer defaultConsumer = new KafkaClientConsumer();
    }

    public static KafkaClientConsumer defaultConsumer(){
        return ConsumerHolder.defaultConsumer;
    }

    public KafkaClientConsumer(Map<String,Object> consumerConfigMap){
        if(consumerConfigMap == null){
            throw new IllegalArgumentException("input config map can`t be null");
        }
        //1: init kafka consumer
        initKafkaConsumer();
        //2: init kafka client
        initKafkaClient();
    }

    private void initKafkaClient() {

    }

    private void initKafkaConsumer() {
        if(consumerConfigMap.get(DefaultConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) == null){
            consumerConfigMap.put(DefaultConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        }
        if(consumerConfigMap.get(DefaultConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG) == null){
            consumerConfigMap.put(DefaultConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        }
        if(consumerConfigMap.get(DefaultConsumerConfig.FETCH_MIN_BYTES_CONFIG) == null){
            consumerConfigMap.put(DefaultConsumerConfig.FETCH_MIN_BYTES_CONFIG,1);
        }
        consumerConfigMap.put("key.deserializer", StringDeserializer.class);
        consumerConfigMap.put("value.deserializer", ValueDeserializer.class);
        kafkaConsumer = new KafkaConsumer(consumerConfigMap);
    }


    @Override
    public void subscribe(List topics) {

    }

    @Override
    public void subscribe(List topics, ConsumerRebalanceListener callback) {

    }

    @Override
    public void subscribe(String topic) {

    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {

    }
}
