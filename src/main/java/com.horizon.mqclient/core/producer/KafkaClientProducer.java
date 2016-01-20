package com.horizon.mqclient.core.producer;

import com.horizon.mqclient.api.Message;
import com.horizon.mqclient.api.ProducerSendCallback;
import com.horizon.mqclient.api.TopicWithPartition;
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
 *
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 11:55
 * @since : 1.0.0
 */
public class KafkaClientProducer extends AbstractProducer<String, Message> {

    private Logger logger = LoggerFactory.getLogger(KafkaClientProducer.class);

    private KafkaClientProducer() {
        super();
    }

    public KafkaClientProducer(Map<String, Object> producerConfigMap) {
        super(producerConfigMap);
    }

    private static class ProducerHolder {
        private static KafkaClientProducer clientProducer = new KafkaClientProducer();
    }

    public static KafkaClientProducer kafkaClientProducer() {
        return ProducerHolder.clientProducer;
    }

    @Override
    public Map<TopicWithPartition, Long> send(Message message) {
        return this.send(message,false);
    }

    @Override
    public Map<TopicWithPartition, Long> send(Message message, boolean isBlockGet) {
        if (message == null || message.getTopic() == null) {
            throw new IllegalArgumentException("topic and message can`t be null");
        }
        ProducerRecord<String, Message> messageRecord = new ProducerRecord<>(message.getTopic(), message);
        Future<RecordMetadata> future = this.kafkaProducer.send(messageRecord);
        return blockGetOrReturn(isBlockGet, future);
    }

    @Override
    public Map<TopicWithPartition, Long> send(Message message, ProducerSendCallback callback) {
        return this.send(message,false,callback);
    }

    @Override
    public Map<TopicWithPartition, Long> send(Message message, boolean isBlockGet, ProducerSendCallback callback) {
        if (message == null || message.getTopic() == null) {
            throw new IllegalArgumentException("topic and message can`t be null");
        }
        ProducerRecord<String, Message> messageRecord = new ProducerRecord<>(message.getTopic(), message);
        Future<RecordMetadata> future = this.kafkaProducer.send(messageRecord, callback);
        return blockGetOrReturn(isBlockGet, future);
    }

    private Map<TopicWithPartition, Long> getBlockFutureResult(Future<RecordMetadata> future) {
        try {
            Map<TopicWithPartition, Long> tpLongMap = new HashMap<>();
            //get method will block until send complete
            RecordMetadata recordMetadata = future.get();
            TopicWithPartition topicWithPartition = new TopicWithPartition(recordMetadata.topic(),
                    recordMetadata.partition());
            tpLongMap.put(topicWithPartition, recordMetadata.offset());
        } catch (InterruptedException e) {
            logger.error("future get error ", e);
        } catch (ExecutionException e) {
            logger.error("future get error ", e);
        }
        return null;
    }

    private Map<TopicWithPartition, Long> blockGetOrReturn(boolean isBlockGet, Future<RecordMetadata> future) {
        if(isBlockGet)
            return getBlockFutureResult(future);
        return null;
    }
}
