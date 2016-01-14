package com.horizon.mqclient.api;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

/**
 * offset commit callback
 * @author : David.Song/Java Engineer
 * @date : 2016/1/6 15:23
 * @see
 * @since : 1.0.0
 */
public abstract class CommitOffsetCallback implements OffsetCommitCallback{

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        Map<TopicWithPartition,Long> tpOffsetMap = new HashMap<>();
        if(offsets != null && !offsets.isEmpty()){
            for(TopicPartition topicPartition : offsets.keySet()){
                OffsetAndMetadata offsetAndMetadata = offsets.get(topicPartition);
                tpOffsetMap.put(new TopicWithPartition(topicPartition.topic(),topicPartition.partition()),
                        offsetAndMetadata.offset());
            }
            this.commitComplete(tpOffsetMap);
        }
    }
    /**
     * @param tpOffsetMap: key: topicWithPartition, value: offset value
     */
    public abstract void commitComplete(Map<TopicWithPartition, Long> tpOffsetMap);
}
