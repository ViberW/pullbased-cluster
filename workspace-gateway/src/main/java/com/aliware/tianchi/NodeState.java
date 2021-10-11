package com.aliware.tianchi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.exp;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 节点的状态
 * @since 2021/9/10 14:00
 */
public class NodeState {
    private final static Logger logger = LoggerFactory.getLogger(NodeState.class);
    private static final long timeInterval = TimeUnit.SECONDS.toMillis(1);
    private static final long oneMill = TimeUnit.MILLISECONDS.toNanos(1);
    public volatile int serverActive = 1;
    private final Counter totalCounter = new Counter();
    private final Counter layCounter = new Counter();
    public volatile long timeout = 40L;
    private static final double ALPHA = 1 - exp(-1 / 60.0);//来自框架metrics的计算系数
    private final int windowSize = 5;
    ScheduledExecutorService scheduledExecutor;

    public NodeState(ScheduledExecutorService scheduledExecutor) {
        this.scheduledExecutor = scheduledExecutor;
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                //计算当前一段时间内的 最近5秒的平均延迟
                long high = offset();
                long low = high - windowSize;
                long sum = totalCounter.sum(low, high);
                if (sum > 0) {
                    long newTimeout = 10 + (layCounter.sum(low, high) / sum);
                    newTimeout = (long) (timeout + (newTimeout - timeout) * ALPHA);
                    timeout = Math.max(newTimeout, 20L);
                    logger.info("NodeState.timeout:{}", timeout);
                }
                clean(high);
            }
        }, 5, 1, TimeUnit.SECONDS);
    }

    public long getWeight() {
        return 500 / this.serverActive;
    }

    public void setServerActive(int w) {
        if (serverActive != w) {
            serverActive = w;
        }
    }

    public void end(long duration) {
        long offset = offset();
        totalCounter.add(offset, 1);
        layCounter.add(offset, duration / oneMill);
    }

    public long offset() {
        return System.currentTimeMillis() / timeInterval;
    }

    public void clean(long high) {
        long toKey = high - (windowSize << 1);
        totalCounter.clean(toKey);
        layCounter.clean(toKey);
    }

}
