package com.horizon.mqclient.core.producer;

import com.horizon.mqclient.common.DefaultProducerConfig;
import com.horizon.mqclient.common.KafkaClientConfig;
import com.horizon.mqclient.serializer.fst.ValueSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *    abstract implementation of producer client
 * </pre>
 * @author : David.Song/Java Engineer
 * @date : 2016/1/13 17:05
 * @see
 * @since : 1.0.0
 */
public abstract class AbstractProducer<K,V> implements Producer<K,V>{

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    protected KafkaProducer<K,V> kafkaProducer;

    protected Map<String,Object> producerConfigMap = new HashMap<>();

    public AbstractProducer(){
        initKafkaProducer();
    }

    public AbstractProducer(Map<String,Object> producerConfigMap){
        if(producerConfigMap == null){
            throw new IllegalArgumentException("producerConfigMap can`t be null");
        }
        this.producerConfigMap = producerConfigMap;
        initKafkaProducer();
    }

    private void initKafkaProducer() {
        if(producerConfigMap.get(DefaultProducerConfig.BATCH_SIZE_CONFIG) == null){
            producerConfigMap.put(DefaultProducerConfig.BATCH_SIZE_CONFIG,16384);
        }
        if(producerConfigMap.get(DefaultProducerConfig.BUFFER_MEMORY_CONFIG) == null){
            producerConfigMap.put(DefaultProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        }
        if(producerConfigMap.get(DefaultProducerConfig.ACKS_CONFIG) == null){
            producerConfigMap.put(DefaultProducerConfig.ACKS_CONFIG,"1");
        }
        if(producerConfigMap.get(DefaultProducerConfig.REQUEST_TIMEOUT_MS_CONFIG) == null){
            producerConfigMap.put(DefaultProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,30000);
        }
        if(producerConfigMap.get(DefaultProducerConfig.LINGER_MS_CONFIG) == null){
            producerConfigMap.put(DefaultProducerConfig.LINGER_MS_CONFIG,1);
        }
        if(producerConfigMap.get(DefaultProducerConfig.RETRIES_CONFIG) == null){
            producerConfigMap.put(DefaultProducerConfig.RETRIES_CONFIG,0);
        }
        if(producerConfigMap.get(DefaultProducerConfig.PARTITIONER_CLASS_CONFIG) == null){
            producerConfigMap.put(DefaultProducerConfig.PARTITIONER_CLASS_CONFIG,
                    "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        }
        producerConfigMap.put("key.serializer", StringSerializer.class);
        producerConfigMap.put("value.serializer", ValueSerializer.class);
        producerConfigMap.put("bootstrap.servers", KafkaClientConfig.configHolder().getKafkaServers());
        this.kafkaProducer = new KafkaProducer(producerConfigMap);
    }

    @Override
    public void flush() {
        this.kafkaProducer.flush();
    }

    @Override
    public void close() {
        this.kafkaProducer.close();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        this.kafkaProducer.close(timeout,unit);
    }
}
