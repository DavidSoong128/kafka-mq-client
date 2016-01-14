package com.horizon.mqclient.api;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * <pre>
 *    produce send message ,and invoked callback after completed
 * </pre>
 * @author : David.Song/Java Engineer
 * @date : 2016/1/13 16:46
 * @see
 * @since : 1.0.0
 */
public abstract class ProducerSendCallback implements Callback{

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long currentOffset = metadata.offset();
        TopicWithPartition topicWithPartition = new TopicWithPartition(
                metadata.topic(),metadata.partition());
        this.sendComplete(topicWithPartition, currentOffset, exception);
    }

    /**
     * send invoked callback
     * @param topicWithPartition: topic and partition
     * @param offset : send offset value
     * @param exception : if send occur error or fail
     */
    public abstract void sendComplete(TopicWithPartition topicWithPartition,
                                      long offset,
                                      Exception exception);
}
