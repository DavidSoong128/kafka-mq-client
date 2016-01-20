package com.horizon.mqclient.api;

/**
 * <pre>
 * handle business after pull message
 * </pre>
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 18:10
 * @see
 * @since : 1.0.0
 */
public interface MessageHandler {

    /**
     * high consumer api, auto commit offset
     * @param message
     */
    public void handleMessage(Message message);

    /**
     * low consumer api, need to manual commit offset
     * @param message
     * @param currentOffset: the lasted poll offset
     */
    public void handleMessage(Message message,long currentOffset);
}
