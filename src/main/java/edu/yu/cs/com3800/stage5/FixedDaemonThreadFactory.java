package edu.yu.cs.com3800.stage5;

import java.util.concurrent.ThreadFactory;

public class FixedDaemonThreadFactory implements ThreadFactory {

    @Override
    public Thread newThread(Runnable r) {
        Thread result = new Thread(r);
        result.setDaemon(true);
        result.setName(result.getName() + "DaemonThreadPoolThread");
        return result;
    }

}
