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
    public volatile Value weight = new Value(50);
    private final Counter<StateCounter> counter = new Counter<>(o -> new StateCounter());
    private final int windowSize = 5;
    private volatile int executeTime = 10;
    private volatile int coefficient = 100;
    private volatile double okRatio = 1;

    public NodeState(ScheduledExecutorService scheduledExecutor) {
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                long high = offset();
                long low = high - windowSize;
                long[] ret = sum(low, high);
                if (ret[0] > 100) {//超过100的处理
                    double ratio = (1 - (ret[1] * 1.0 / ret[0]));
                    ratio = (okRatio + ratio) / 2;
                    int coef = (int) (ratio * 100);//乘以100,避免因降低导致出随机概率密集
                    coef = (coef + coefficient) / 2;
                    okRatio = ratio;
                    coefficient = coef;
                }
                clean(high);
            }
        }, 10, 2, TimeUnit.SECONDS);
    }

    public int getWeight() {
        return (int) Math.max(1, weight.value * coefficient);
    }

    public void setWeight(long w) {
        if (weight.value != w) {
            weight.value = w;
        }
    }

    public int getFullWeight() {
        return (int) (weight.value / okRatio);
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
