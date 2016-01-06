package com.horizon.mqclient.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * mq transfer message format
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 15:15
 * @see
 * @since : 1.0.0
 */
public class Message implements Serializable {

    private String messageId ;

    private String topic ;
    /**
     * message content byte format
     */
    private byte[] messageByte ;

    /**
     * message content object format
     */
    private Object messageObj;

    /**
     * other data must to transfer
     */
    private Map<String,Object>  propertyMap = new HashMap<>();


    public Message(String topic, byte[] messageByte) {
        this.topic = topic;
        this.messageByte = messageByte;
    }

    public Message(String topic, Object messageObj) {
        this.topic = topic;
        this.messageObj = messageObj;
    }

    public Message(String topic, byte[] messageByte, Map<String, Object> propertyMap) {
        this.topic = topic;
        this.messageByte = messageByte;
        this.propertyMap = propertyMap;
    }

    public Message(String topic, Object messageObj, Map<String, Object> propertyMap) {
        this.topic = topic;
        this.messageObj = messageObj;
        this.propertyMap = propertyMap;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getMessageByte() {
        return messageByte;
    }

    public void setMessageByte(byte[] messageByte) {
        this.messageByte = messageByte;
    }

    public Object getMessageObj() {
        return messageObj;
    }

    public void setMessageObj(Object messageObj) {
        this.messageObj = messageObj;
    }

    public Map<String, Object> getPropertyMap() {
        return propertyMap;
    }

    public void setPropertyMap(Map<String, Object> propertyMap) {
        this.propertyMap = propertyMap;
    }
}
