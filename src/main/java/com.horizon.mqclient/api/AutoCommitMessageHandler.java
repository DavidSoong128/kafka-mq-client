package com.horizon.mqclient.api;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/15 17:30
 * @see com.horizon.mqclient.core.consumer.KafkaHighConsumer
 * @since : 1.0.0
 */
public abstract class AutoCommitMessageHandler implements MessageHandler {

    /**
     * high consumer client override
     * @param message
     */
    @Override
    public abstract void handleMessage(Message message);

    /**
     * low consumer client  override
     * AutoCommitMessageHandler can`t support the operation
     * @param message
     * @param currentOffset: the lasted poll offset
     */
    @Override
    public void handleMessage(Message message, long currentOffset) {
        throw new UnsupportedOperationException("AutoCommitMessageHandler can`t support the operation");
    }
}
