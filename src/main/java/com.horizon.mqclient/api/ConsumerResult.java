package com.horizon.mqclient.api;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/21 15:06
 * @see
 * @since : 1.0.0
 */
public class ConsumerResult {

    private final String topic;
    private final int partition;
    private final long offset;
    private final Message value;

    public ConsumerResult(String topic, int partition, long offset, Message value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public Message getValue() {
        return value;
    }
}
