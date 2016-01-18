package com.horizon.mqclient.core.producer;

import com.horizon.mqclient.api.Message;
import com.horizon.mqclient.api.ProducerSendCallback;
import com.horizon.mqclient.api.TopicWithPartition;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *   kafka client produce message to broker
 * </pre>
 * @author : David.Song/Java Engineer
 * @date : 2016/1/13 16:40
 * @see
 * @since : 1.0.0
 */
public interface Producer<K,V> {

    /**
     * Send the given record asynchronously and return a future which will eventually contain the response information.
     * but will not execute future.get() operation
     * @param message The record to send
     * @return A future which will eventually contain the response information
     */
    public Map<TopicWithPartition,Long> send(Message message);
    /**
     * Send the given record asynchronously and return a future which will eventually contain the response information.
     * @param message The record to send
     * @param isBlockGet whether if block future.get() after send message ,default : false
     * @return A future which will eventually contain the response information
     */
    public Map<TopicWithPartition,Long> send(Message message,boolean isBlockGet);
    /**
     * send message given record and topic and partition
     * invoke callback after send message,but will not execute future.get() operation
     * @param message
     * @return
     */
    public Map<TopicWithPartition,Long> send(Message message,ProducerSendCallback callback);
    /**
     * send message given record and topic and partition
     * invoke callback after send message
     * @param message
     * @param isBlockGet whether if block future.get() after send message ,default : false
     * @return
     */
    public Map<TopicWithPartition,Long> send(Message message, boolean isBlockGet, ProducerSendCallback callback);
    /**
     * Flush any accumulated records from the producer. Blocks until all sends are complete.
     */
    public void flush();

    /**
     * Close this producer
     */
    public void close();

    /**
     * Tries to close the producer cleanly within the specified timeout. If the close does not complete within the
     * timeout, fail any pending send requests and force close the producer.
     */
    public void close(long timeout, TimeUnit unit);
}
