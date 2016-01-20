package com.horizon.mqclient.core.consumer;

import com.horizon.mqclient.api.*;
import com.horizon.mqclient.common.ConsumerStatus;
import com.horizon.mqclient.common.MsgHandleStreamPool;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * kafka client consumer
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 11:54
 * @since : 1.0.0
 * @see AutoCommitMessageHandler
 */
public class KafkaHighConsumer extends AbstractConsumer<String,Message>{

    private Logger logger = LoggerFactory.getLogger(KafkaHighConsumer.class);

    private KafkaHighConsumer(){
        super(true);
    }

    public KafkaHighConsumer(Map<String,Object> consumerConfigMap){
        super(true,consumerConfigMap);
    }

    private static class ConsumerHolder{
        private static KafkaHighConsumer clientConsumer = new KafkaHighConsumer();
    }

    public static KafkaHighConsumer kafkaHighConsumer(){
        return ConsumerHolder.clientConsumer;
    }


    public void subscribe(String topic, MessageHandler handler, OffsetRebalanceListener listener){
        super.subscribe(topic,handler,listener);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer,handler));
    }

    public void subscribe(List topics , MessageHandler handler, OffsetRebalanceListener listener){
        super.subscribe(topics,handler,listener);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer, handler));
    }

    public void subscribe(String topic, MessageHandler handler){
        super.subscribe(topic,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer,handler));
    }

    public void subscribe(List topics ,MessageHandler handler){
        super.subscribe(topics,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer, handler));
    }

    public void subscribe(Pattern pattern, MessageHandler handler, OffsetRebalanceListener listener) {
        super.subscribe(pattern,handler,listener);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer,handler));
    }

    public void subscribe(Pattern pattern,MessageHandler handler) {
        super.subscribe(pattern,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer,handler));
    }

    public void assign(String topic, Integer[] partitions,MessageHandler handler) {
        super.assign(topic, partitions,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer,handler));
    }

    public void assign(TopicWithPartition topicWithPartition, MessageHandler handler){
        super.assign(topicWithPartition,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer,handler));
    }

    public void assign(TopicWithPartition[] topicWithPartitions, MessageHandler handler){
        super.assign(topicWithPartitions,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer,handler));
    }

    private class MessageWorkAutoCommitTask implements Runnable {

        private static final long POLL_TIMEOUT = 1;
        private final KafkaConsumer kafkaConsumer;
        private final MessageHandler handler;

        public MessageWorkAutoCommitTask(KafkaConsumer kafkaConsumer, MessageHandler handler) {
            this.kafkaConsumer = kafkaConsumer;
            this.handler = handler;
        }

        @Override
        public void run() {
            while (status == ConsumerStatus.RUNNING) {
                try {
                    ConsumerRecords<String, Message> records = kafkaConsumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, Message> record : records) {
                        handler.handleMessage(record.value());
                    }
                } catch (Exception ex) {
                    logger.error("poll message error ", ex);
                }
            }
        }
    }
}
