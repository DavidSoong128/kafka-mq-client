package com.horizon.mqclient.core.producer;

import com.horizon.mqclient.api.Message;
import com.horizon.mqclient.api.ProducerSendCallback;
import com.horizon.mqclient.api.TopicWithPartition;
import com.horizon.mqclient.utils.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * kafka client producer
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 11:55
 * @since : 1.0.0
 */
public class KafkaClientProducer extends AbstractProducer<String,Message>{

    private Logger logger = LoggerFactory.getLogger(KafkaClientProducer.class);

    public KafkaClientProducer(){
        super();
    }

    public KafkaClientProducer(Map<String,Object> producerConfigMap){
        super(producerConfigMap);
    }

    private static class ProducerHolder{
        private static KafkaClientProducer clientProducer = new KafkaClientProducer();
    }

    public KafkaClientProducer kafkaClientProducer(){
        return ProducerHolder.clientProducer;
    }

    @Override
    public Map<TopicWithPartition, Long> send(String topic, Message message) {
        if(StringUtils.isEmpty(topic) || message == null){
            throw  new IllegalArgumentException("topic and message can`t be null");
        }
        ProducerRecord<String,Message>  messageRecord = new ProducerRecord<>(topic,message);
        Future<RecordMetadata> future = this.kafkaProducer.send(messageRecord);
        return parseSendFuture(future);
    }

    @Override
    public Map<TopicWithPartition, Long> send(String topic, Integer partition, Message message) {
        if(StringUtils.isEmpty(topic) || partition == null || partition<0
                || message == null){
            throw  new IllegalArgumentException("topic and message can`t be null,partition can`t be negative");
        }
        ProducerRecord<String,Message>  messageRecord = new ProducerRecord<>(topic,partition,null,message);
        Future<RecordMetadata> future = this.kafkaProducer.send(messageRecord);
        return parseSendFuture(future);
    }

    @Override
    public Map<TopicWithPartition, Long> send(String topic, Message message, ProducerSendCallback callback) {
        if(StringUtils.isEmpty(topic) || message == null){
            throw  new IllegalArgumentException("topic and message can`t be null");
        }
        ProducerRecord<String,Message>  messageRecord = new ProducerRecord<>(topic,message);
        Future<RecordMetadata> future = this.kafkaProducer.send(messageRecord,callback);
        return parseSendFuture(future);
    }

    @Override
    public Map<TopicWithPartition, Long> send(String topic, Integer partition, Message message,
                                              ProducerSendCallback callback) {
        if(StringUtils.isEmpty(topic) || partition == null || partition<0 || message == null){
            throw  new IllegalArgumentException("topic and message can`t be null,partition can`t be negative");
        }
        ProducerRecord<String,Message>  messageRecord = new ProducerRecord<>(topic,partition,null,message);
        Future<RecordMetadata> future = this.kafkaProducer.send(messageRecord,callback);
        return parseSendFuture(future);
    }

    private Map<TopicWithPartition, Long> parseSendFuture(Future<RecordMetadata> future) {
        Map<TopicWithPartition, Long> tpLongMap = new HashMap<>();
        try {
            RecordMetadata recordMetadata = future.get();
            TopicWithPartition topicWithPartition = new TopicWithPartition(recordMetadata.topic(),
                    recordMetadata.partition());
            tpLongMap.put(topicWithPartition, recordMetadata.offset());
        } catch (InterruptedException e) {
            logger.error("future get error ",e);
        } catch (ExecutionException e) {
            logger.error("future get error ", e);
        }
        return tpLongMap;
    }
}
