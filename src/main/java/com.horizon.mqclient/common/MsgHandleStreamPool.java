package com.horizon.mqclient.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * handle message thread pool
 * @author : David.Song/Java Engineer
 * @date : 2016/1/4 18:29
 * @see
 * @since : 1.0.0
 */
public class MsgHandleStreamPool {

    private ExecutorService  messageStreamPool ;

    private MsgHandleStreamPool(){
        this.messageStreamPool = Executors.newFixedThreadPool(1);
    }

    private static class PoolHolder{
        private static MsgHandleStreamPool msgHandleStreamPool = new MsgHandleStreamPool();
    }

    public static MsgHandleStreamPool poolHolder(){
        return PoolHolder.msgHandleStreamPool;
    }

    public void execute(Runnable workThread){
        this.messageStreamPool.execute(workThread);
    }

    public void turnOffPool(){
        this.messageStreamPool.shutdown();
    }

    public void turnOffPoolNow(){
        this.messageStreamPool.shutdownNow();
    }

    public boolean waitUntilAllTaskFinished() throws InterruptedException {
        return this.messageStreamPool.awaitTermination(10, TimeUnit.SECONDS);
    }
}
