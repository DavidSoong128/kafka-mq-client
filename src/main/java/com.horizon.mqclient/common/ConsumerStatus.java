package com.horizon.mqclient.common;

/**
 * consumer run status
 * @author : David.Song/Java Engineer
 * @date : 2016/1/5 20:17
 * @see
 * @since : 1.0.0
 */
public enum ConsumerStatus {

    INIT, //Consumer init status

    RUNNING, //Consumer already run

    STOPPING, //Consumer is stopping

    STOPPED; //Consumer had stop
}
