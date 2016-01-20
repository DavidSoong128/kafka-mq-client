package com.horizon.mqclient.api;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/15 17:30
 * @see com.horizon.mqclient.core.consumer.KafkaLowConsumer
 * @since : 1.0.0
 */
public abstract class ManualCommitMessageHandler implements MessageHandler {

    /**
     * high consumer client override
     * ManualCommitMessageHandler can`t support the operation
     * @param message
     */
    @Override
    public void handleMessage(Message message){
        throw new UnsupportedOperationException("ManualCommitMessageHandler can`t support the operation");
    }

    /**
     * low consumer client  override
     * @param message
     * @param currentOffset: the lasted poll offset
     */
    @Override
    public abstract void handleMessage(Message message, long currentOffset);
}
