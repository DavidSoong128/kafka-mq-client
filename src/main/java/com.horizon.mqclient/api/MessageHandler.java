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
     * handle message
     * @param result
     */
    public void handleMessage(ConsumerResult result);

}
