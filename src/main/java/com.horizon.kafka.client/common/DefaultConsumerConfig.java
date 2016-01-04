package com.horizon.kafka.client.common;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 16:45
 * @see
 * @since : 1.0.0
 */
public interface DefaultConsumerConfig {
    /**
     * open auto commit after consumer message or not,default true
     */
    public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";

    /**
     * auto commit interval time ,default 1000ms
     */
    public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms";

    /**
     *The minimum amount of data the server should return for a fetch request，default 1 byte,
     * which means will return as soon as possible when exist data
     */
    public static final String FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes";

}
