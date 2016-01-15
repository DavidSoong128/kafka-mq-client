package com.horizon.mqclient.api;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/15 17:30
 * @see
 * @since : 1.0.0
 */
public class AbstractMessageProcessor implements MessageProcessor{

    /**
     * high consumer client override
     * @param message
     */
    @Override
    public void handleMessage(Message message) {
    }

    /**
     * low consumer client  override
     * @param message
     * @param currentOffset: the lasted poll offset
     */
    @Override
    public void handleMessage(Message message, long currentOffset) {
    }
}
