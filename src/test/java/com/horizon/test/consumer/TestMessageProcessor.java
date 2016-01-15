package com.horizon.test.consumer;

import com.horizon.mqclient.api.Message;
import com.horizon.mqclient.api.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : David.Song/Java Engineer
 * @date : 2016/1/15 11:08
 * @see
 * @since : 1.0.0
 */
public class TestMessageProcessor implements MessageProcessor{

    private Logger logger = LoggerFactory.getLogger(TestMessageProcessor.class);

    @Override
    public void handleMessage(Message message) {
        String str = new String(message.getMessageByte());
        System.out.println("================="+str);
    }

    @Override
    public void handleMessage(Message message, long currentOffset) {

    }
}
