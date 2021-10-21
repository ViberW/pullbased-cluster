package com.aliware.tianchi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 节点的状态
 * @since 2021/9/10 14:00
 */
public class NodeState {
    private final static Logger logger = LoggerFactory.getLogger(NodeState.class);
    private static final long timeInterval = TimeUnit.SECONDS.toMillis(2);
    public volatile int weight = 50;
    private final Counter<StateCounter> counter = new Counter<>(o -> new StateCounter());
    private final int windowSize = 5;
    private volatile int executeTime = 10;
    private double failureRatio = 0;

    public NodeState(ScheduledExecutorService scheduledExecutor) {
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                long high = offset();
                long low = high - windowSize;
                long[] ret = sum(low, high);
                if (ret[0] > 100) {//超过100的处理
                    double ratio = ret[1] * 1.0 / ret[0];
                    ratio = (ratio + failureRatio) / 2;
                    failureRatio = ratio;
                    logger.info("NodeState:{}", ratio);
                }
                clean(high);
            }
        }, 10, 2, TimeUnit.SECONDS);
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
        return /*timeout +*/ executeTime;
    }

    public void end(boolean failure) {
        long offset = offset();
        StateCounter state = counter.get(offset);
        if (failure) {
            state.getFailure().add(1);
        }
        state.getTotal().add(1);
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
