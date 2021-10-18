package com.aliware.tianchi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
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
    public volatile int weight = 50;
    private final Counter<StateCounter> counter = new Counter<>(o -> new StateCounter());
    //    public volatile long timeout = 10L; //这个是延迟的时间
    private final int windowSize = 5;
    private volatile int executeTime = 20;
    private volatile double failureRatio = 0;
//    private static final double ALPHA = 1 - exp(-1 / 60.0);//来自框架metrics的计算系数

    public NodeState(ScheduledExecutorService scheduledExecutor) {
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                long high = offset();
                long low = high - windowSize;
                long[] ret = sum(low, high);
                if (ret[0] > 0) {
                    double newRatio = ret[1] * 1.0 / ret[0];
                    newRatio = /*(failureRatio + (newRatio - failureRatio) * ALPHA)*/(newRatio + failureRatio) / 2;
                    failureRatio = Math.max(0, newRatio);
                    logger.info("NodeState:{}", failureRatio);
                }
                clean(high);
            }
        }, 5, 1, TimeUnit.SECONDS);
    }

    public int getWeight() {
        return Math.max(1, (int) (weight * (1 - failureRatio)));
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
        return /*timeout + */executeTime;
    }

    public void end(boolean f) {
        long offset = offset();
        StateCounter state = counter.get(offset);
        state.getTotal().add(1);
        if (f) {
            state.getFailure().add(1);
        }
    }

    public long[] sum(long fromOffset, long toOffset) {
        long[] result = {0, 0};
        Collection<StateCounter> sub = counter.sub(fromOffset, toOffset);
        if (!sub.isEmpty()) {
            sub.forEach(state -> {
                result[0] += state.getTotal().sum();
                result[1] += state.getFailure().sum();
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
    }

}
