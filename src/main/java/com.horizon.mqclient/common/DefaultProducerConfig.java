package com.horizon.mqclient.common;

/**
 * producer client config constants
 * @author : David.Song/Java Engineer
 * @date : 2016/1/13 17:28
 * @see
 * @since : 1.0.0
 */
public class DefaultProducerConfig {
    /**
     * The producer will attempt to batch records together into
     * fewer requests whenever multiple records are being sent to the same partition
     *
     * default: 16384
     */
    public static final String BATCH_SIZE_CONFIG = "batch.size";

    /**
     * The total bytes of memory the producer can use to buffer records waiting to be sent to the server.
     * If records are sent faster than they can be delivered to the server the producer
     * will either block or throw an exception based
     *
     * default: 33554432
     */
    public static final String BUFFER_MEMORY_CONFIG = "buffer.memory";

    /**
     * The number of acknowledgments the producer requires the leader to have received before considering a request complete
     * acks=0  If set to zero then the producer will not wait for any acknowledgment from the server at all
     * acks=1  This will mean the leader will write the record to its local log but will respond without awaiting full acknowledgement from all followers
     * acks=all This means the leader will wait for the full set of in-sync replicas to acknowledge the record
     *
     * default:1
     */
    public static final String ACKS_CONFIG = "acks";
    /**
     * The configuration controls the maximum amount of time the client will wait for the response of a request
     *
     * default:30000
     */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";

    /**
     * The producer groups together any records that arrive in between request transmissions into a single batched request
     *
     * default:1
     */
    public static final String LINGER_MS_CONFIG = "linger.ms";

    /**
     * Setting a value greater than zero will cause the client to resend any record whose
     * send fails with a potentially transient error
     *
     * default : 0
     */
    public static final String RETRIES_CONFIG = "retries";

    /**
     * Partitioner class that implements the <code>PartitionStrategy</code> interface
     *
     * default:class org.apache.kafka.clients.producer.internals.DefaultPartitioner
     */
    public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
}
