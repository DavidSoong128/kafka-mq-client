package com.horizon.mqclient.api;

/**
 * <pre>
 *     topic associate with partition
 * </pre>
 * @author : David.Song/Java Engineer
 * @date : 2016/1/6 9:51
 * @see
 * @since : 1.0.0
 */
public class TopicWithPartition{

    private final int partition;

    private final String topic;

    public TopicWithPartition(String topic, int partition){
        this.topic = topic;
        this.partition = partition;
    }

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return "partition "+ partition +", topic "+topic;
    }
}
