package com.horizon.mqclient.api;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 *     A callback interface that the user can implement to trigger custom actions when the
 *     set of partitions assigned to the consumer changes.
 * </pre>
 * @author : David.Song/Java Engineer
 * @date : 2016/1/19 9:14
 * @see
 * @since : 1.0.0
 */
public abstract class OffsetRebalanceListener implements ConsumerRebalanceListener{

    private KafkaConsumer kafkaConsumer;

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        Map<TopicWithPartition,Long> tpOffsetMap = new HashMap<>();
        for(TopicPartition topicPartition : partitions){
            long offset = this.kafkaConsumer.position(topicPartition);
            tpOffsetMap.put(new TopicWithPartition(topicPartition.topic(),
                    topicPartition.partition()), offset);
        }
        this.saveOffsetInExternalStore(tpOffsetMap);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for(TopicPartition topicPartition : partitions){
            long offset = this.readOffsetFromExternalStore(topicPartition.topic(),
                    topicPartition.partition());
            this.kafkaConsumer.seek(topicPartition,offset);
        }
    }
    /**
     * save the offsets in an external store using some custom code not described here
     */
    public abstract void saveOffsetInExternalStore(Map<TopicWithPartition,Long> tpOffsetMap);

    /**
     * read the offsets from an external store using some custom code not described here
     */
    public abstract long readOffsetFromExternalStore(String topic, int partition);


    public void setKafkaConsumer(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }
}
