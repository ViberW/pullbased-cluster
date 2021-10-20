package com.aliware.tianchi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 节点的状态
 * @since 2021/9/10 14:00
 */
public class NodeState {
    //    private static final long timeInterval = TimeUnit.SECONDS.toMillis(1);
//    private static final long oneMill = TimeUnit.MILLISECONDS.toNanos(1);
    public volatile int weight = 50;
    //    private final Counter<StateCounter> counter = new Counter<>(o -> new StateCounter());
//    public volatile long timeout = 10L;
    private final int windowSize = 5;
    private final static Logger logger = LoggerFactory.getLogger(NodeState.class);
    private volatile int executeTime = 10;

    public NodeState(/*ScheduledExecutorService scheduledExecutor*/) {
        /*scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                long high = offset();
                long low = high - windowSize;
                long[] ret = sum(low, high);
                if (ret[0] > 0) {
                    long newTimeout = ((1 + ret[1] / ret[0]));
                    newTimeout = *//*(long) (timeout + (newTimeout - timeout) * ALPHA)*//* (newTimeout + timeout) / 2;
                    timeout = newTimeout;
                }
                clean(high);
            }
        }, 5, 1, TimeUnit.SECONDS);*/
    }

    public int getWeight() {
        return Math.max(1, weight);
    }

    public void setWeight(int w) {
        if (weight != w) {
            weight = w;
        }
    }

    public void setExecuteTime(int executeTime) {
        if (this.executeTime != executeTime) {
            this.executeTime = executeTime;
        }
    }

    public long getTimeout() {
        return /*timeout +*/ executeTime + 5;
    }

  /*  public void end(long duration) {
        long offset = offset();
        StateCounter state = counter.get(offset);
        state.getDuration().add(duration / oneMill);
        state.getTotal().add(1);
    }

    public long[] sum(long fromOffset, long toOffset) {
        long[] result = {0, 0};
        Collection<StateCounter> sub = counter.sub(fromOffset, toOffset);
        if (!sub.isEmpty()) {
            sub.forEach(state -> {
                result[0] += state.getTotal().sum();
                result[1] += state.getDuration().sum();
            });
        }
        return result;
    }

    public long offset() {
        return System.currentTimeMillis() / timeInterval;
    }

    public void clean(long high) {
        long toKey = high - (windowSize << 1);
        counter.clean(toKey);
    }*/
}
