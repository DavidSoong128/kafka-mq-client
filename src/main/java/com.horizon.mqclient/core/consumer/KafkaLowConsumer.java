package com.horizon.mqclient.core.consumer;

import com.horizon.mqclient.api.CommitOffsetCallback;
import com.horizon.mqclient.api.Message;
import com.horizon.mqclient.api.MessageProcessor;
import com.horizon.mqclient.api.TopicWithPartition;
import com.horizon.mqclient.common.ConsumerStatus;
import com.horizon.mqclient.common.MsgHandleStreamPool;
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

    public static KafkaLowConsumer clientConsumer(){
        return ConsumerHolder.clientConsumer;
    }

    public void subscribe(String topic, MessageProcessor processor) throws Exception {
        super.subscribe(topic);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer,processor));
    }

    public void subscribe(List topics ,MessageProcessor processor) throws Exception {
        super.subscribe(topics);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer, processor));
    }

    public void subscribe(Pattern pattern,MessageProcessor processor) throws Exception {
        super.subscribe(pattern);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer, processor));
    }

    public void assign(String topic, Integer[] partitions,MessageProcessor processor) throws Exception {
        super.assign(topic, partitions);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer, processor));
    }

    public void assign(TopicWithPartition topicWithPartition, MessageProcessor processor) throws Exception {
        super.assign(topicWithPartition);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkManualCommitTask(kafkaConsumer,processor));
    }

    @Override
    public void commitAsync() throws Exception {
        if(status != ConsumerStatus.RUNNING){
            throw new Exception("The consumer is not running now!");
        }
        this.kafkaConsumer.commitAsync();
    }

    @Override
    public void commitAsync(Map<TopicWithPartition,Long> offsets, CommitOffsetCallback callback) throws Exception {
        if(status != ConsumerStatus.RUNNING){
            throw new Exception("The consumer is not running now!");
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
    public void commitAsync(CommitOffsetCallback callback) throws Exception {
        if(status != ConsumerStatus.RUNNING){
            throw new Exception("The consumer is not running now!");
        }
        this.kafkaConsumer.commitAsync(callback);
    }

    @Override
    public void commitSync() throws Exception {
        if(status != ConsumerStatus.RUNNING){
            throw new Exception("The consumer is not running now!");
        }
        this.kafkaConsumer.commitSync();
    }

    @Override
    public void commitSync(Map<TopicWithPartition, Long> offsets) throws Exception {
        if(status != ConsumerStatus.RUNNING){
            throw new Exception("The consumer is not running now!");
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
        private final MessageProcessor processor;

        public MessageWorkManualCommitTask(KafkaConsumer kafkaConsumer, MessageProcessor processor) {
            this.kafkaConsumer = kafkaConsumer;
            this.processor = processor;
        }

        @Override
        public void run() {
            while (status == ConsumerStatus.RUNNING) {
                try {
                    ConsumerRecords<String, Message> records = kafkaConsumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, Message> record : records) {
                        processor.handleMessage(record.value(),record.offset());
                    }
                } catch (Exception ex) {
                    logger.error("poll message error ", ex);
                }
            }
        }
    }
}
