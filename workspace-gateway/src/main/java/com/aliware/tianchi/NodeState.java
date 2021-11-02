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
    private static final long timeInterval = TimeUnit.SECONDS.toMillis(1);
    public Value weight = new Value(50);
    private final Counter<StateCounter> counter = new Counter<>(o -> new StateCounter());
    private final int windowSize = 5;
    private final Value executeTime = new Value(10);
    private final Value coefficient = new Value(100);

    public NodeState(ScheduledExecutorService scheduledExecutor) {
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                long high = offset();
                long low = high - windowSize;
                long[] ret = sum(low, high);
                if (ret[0] > 100) {//超过100的处理
                    double ratio = (1 - (ret[1] * 1.0 / ret[0]));
                    int coef = (int) (ratio * 100);//乘以100,避免因降低导致出随机概率密集
                    coef = (coef + coefficient.value) / 2;
                    coefficient.value = coef;
                }
                clean(high);
            }
        }, 10, 1, TimeUnit.SECONDS);
    }

    public int getWeight() {
        return weight.value * coefficient.value;
    }

    public void setWeight(int w) {
        weight.value = w;
    }

    public void setExecuteTime(int executeTime) {
        this.executeTime.value = executeTime;
    }

    public int getTimeout() {
        return executeTime.value;
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
