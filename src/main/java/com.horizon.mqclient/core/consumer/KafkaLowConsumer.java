package com.horizon.mqclient.core.consumer;

import com.horizon.mqclient.api.*;
import com.horizon.mqclient.common.ConsumerStatus;
import com.horizon.mqclient.common.MsgHandleStreamPool;
import com.horizon.mqclient.exception.KafkaMQException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * manual manage offset commit
 * @author : David.Song/Java Engineer
 * @date : 2016/1/6 14:29
 * @see
 * @since : 1.0.0
 */
public class KafkaLowConsumer extends AbstractConsumer<String,Message>{

    private Logger logger = LoggerFactory.getLogger(KafkaLowConsumer.class);

    private KafkaLowConsumer(){
        super(false);
    }

    public KafkaLowConsumer(Map<String,Object> consumerConfigMap){
        super(false,consumerConfigMap);
    }

    private static class ConsumerHolder{
        private static KafkaLowConsumer clientConsumer = new KafkaLowConsumer();
    }

    public static KafkaLowConsumer kafkaLowConsumer(){
        return ConsumerHolder.clientConsumer;
    }

    public void subscribe(String topic, MessageHandler handler) {
        super.subscribe(topic,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer,handler));
    }

    public void subscribe(List topics ,MessageHandler handler) {
        super.subscribe(topics,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer, handler));
    }

    public void subscribe(Pattern pattern,MessageHandler handler) {
        super.subscribe(pattern,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer, handler));
    }

    public void subscribe(Pattern pattern, MessageHandler handler, OffsetRebalanceListener listener) {
        super.subscribe(pattern,handler,listener);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer,handler));
    }

    public void subscribe(String topic, MessageHandler handler, OffsetRebalanceListener listener){
        super.subscribe(topic,handler,listener);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer,handler));
    }

    public void subscribe(List topics , MessageHandler handler, OffsetRebalanceListener listener){
        super.subscribe(topics,handler,listener);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer, handler));
    }

    public void assign(String topic, Integer[] partitions,MessageHandler handler) {
        super.assign(topic, partitions,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer, handler));
    }

    public void assign(TopicWithPartition topicWithPartition, MessageHandler handler) {
        super.assign(topicWithPartition,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer,handler));
    }

    public void assign(TopicWithPartition[] topicWithPartitions, MessageHandler handler) {
        super.assign(topicWithPartitions,handler);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer,handler));
    }

    @Override
    public void commitAsync(){
        if(status != ConsumerStatus.RUNNING){
            throw new KafkaMQException("The consumer is not running now!");
        }
        this.kafkaConsumer.commitAsync();
    }

    @Override
    public void commitAsync(Map<TopicWithPartition,Long> offsets, CommitOffsetCallback callback){
        if(status != ConsumerStatus.RUNNING){
            throw new KafkaMQException("The consumer is not running now!");
        }
        Map<TopicPartition,OffsetAndMetadata> tmpOffsets = new HashMap<>();
        for(TopicWithPartition topicWithPartition : offsets.keySet()){
            TopicPartition topicPartition = new TopicPartition(topicWithPartition.getTopic(),
                    topicWithPartition.getPartition());
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offsets.get(topicWithPartition));
            tmpOffsets.put(topicPartition,offsetAndMetadata);
        }
        this.kafkaConsumer.commitAsync(tmpOffsets,callback);
    }

    @Override
    public void commitAsync(CommitOffsetCallback callback){
        if(status != ConsumerStatus.RUNNING){
            throw new KafkaMQException("The consumer is not running now!");
        }
        this.kafkaConsumer.commitAsync(callback);
    }

    @Override
    public void commitSync(){
        if(status != ConsumerStatus.RUNNING){
            throw new KafkaMQException("The consumer is not running now!");
        }
        this.kafkaConsumer.commitSync();
    }

    @Override
    public void commitSync(Map<TopicWithPartition, Long> offsets){
        if(status != ConsumerStatus.RUNNING){
            throw new KafkaMQException("The consumer is not running now!");
        }
        Map<TopicPartition,OffsetAndMetadata> tmpOffsets = new HashMap<>();
        for(TopicWithPartition topicWithPartition : offsets.keySet()){
            TopicPartition topicPartition = new TopicPartition(topicWithPartition.getTopic(),
                    topicWithPartition.getPartition());
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offsets.get(topicWithPartition));
            tmpOffsets.put(topicPartition,offsetAndMetadata);
        }
        this.kafkaConsumer.commitSync(tmpOffsets);
    }

    private class MessageWorkManualCommitTask implements Runnable {

        private static final long POLL_TIMEOUT = 1;
        private final KafkaConsumer kafkaConsumer;
        private final MessageHandler handler;

        public MessageWorkManualCommitTask(KafkaConsumer kafkaConsumer, MessageHandler handler) {
            this.kafkaConsumer = kafkaConsumer;
            this.handler = handler;
        }

        @Override
        public void run() {
            while (status == ConsumerStatus.RUNNING) {
                try {
                    ConsumerRecords<String, Message> records = kafkaConsumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, Message> record : records) {
                        ConsumerResult result = new ConsumerResult(record.topic(), record.partition(),
                                                                   record.offset(),record.value());
                        handler.handleMessage(result);
                    }
                } catch (Exception ex) {
                    logger.error("poll message error ", ex);
                }
            }
        }
    }
}
