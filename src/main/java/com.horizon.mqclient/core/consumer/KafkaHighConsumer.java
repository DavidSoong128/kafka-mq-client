package com.horizon.mqclient.core.consumer;

import com.caucho.hessian.io.ValueDeserializer;
import com.horizon.mqclient.api.MessageProcessor;
import com.horizon.mqclient.api.Message;
import com.horizon.mqclient.api.TopicWithPartition;
import com.horizon.mqclient.common.ConsumerStatus;
import com.horizon.mqclient.common.DefaultConsumerConfig;
import com.horizon.mqclient.common.MsgHandleStreamPool;
import com.horizon.mqclient.config.MQClientConfig;
import com.horizon.mqclient.utils.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * kafka client consumer
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 11:54
 * @since : 1.0.0
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

    public static KafkaHighConsumer clientConsumer(){
        return ConsumerHolder.clientConsumer;
    }


    public void subscribe(String topic, MessageProcessor processor) throws Exception {
        super.subscribe(topic);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer,processor));
    }

    public void subscribe(List topics ,MessageProcessor processor) throws Exception {
        super.subscribe(topics);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer, processor));
    }

    public void subscribe(Pattern pattern,MessageProcessor processor) throws Exception {
        super.subscribe(pattern);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer,processor));
    }

    public void assign(String topic, Integer[] partitions,MessageProcessor processor) throws Exception {
        super.assign(topic, partitions);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer,processor));
    }

    public void assign(TopicWithPartition topicWithPartition, MessageProcessor processor) throws Exception {
        super.assign(topicWithPartition);
        MsgHandleStreamPool.poolHolder().execute(new MessageWorkAutoCommitTask(kafkaConsumer,processor));
    }

    private class MessageWorkAutoCommitTask implements Runnable {

        private static final long POLL_TIMEOUT = 1;
        private final KafkaConsumer kafkaConsumer;
        private final MessageProcessor processor;

        public MessageWorkAutoCommitTask(KafkaConsumer kafkaConsumer, MessageProcessor processor) {
            this.kafkaConsumer = kafkaConsumer;
            this.processor = processor;
        }

        @Override
        public void run() {
            while (status == ConsumerStatus.RUNNING) {
                try {
                    ConsumerRecords<String, Message> records = kafkaConsumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, Message> record : records) {
                        processor.handleMessage(record.value());
                    }
                } catch (Exception ex) {
                    logger.error("poll message error ", ex);
                }
            }
        }
    }
}
