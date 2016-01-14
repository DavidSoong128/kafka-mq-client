package com.horizon.mqclient.exception;

/**
 * kafka mq client exception
 * @author : David.Song/Java Engineer
 * @date : 2016/1/8 10:49
 * @see
 * @since : 1.0.0
 */
public class KafkaMQException extends RuntimeException{

    private String message;

    public KafkaMQException(){
        super();
    }

    public KafkaMQException(String message){
        super(message);
        this.message = message;
    }

    public KafkaMQException(String message, Throwable cause){
        super(message,cause);
        this.message = message;
    }

}
