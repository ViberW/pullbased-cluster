package com.aliware.tianchi;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 节点的状态
 * @since 2021/9/10 14:00
 */
public class NodeState {

    public static final long DEFAULT_WEIGHT = 1;

    public AtomicInteger active = new AtomicInteger(0);
    public AtomicLong weight = new AtomicLong(DEFAULT_WEIGHT);
    private volatile double timeoutRatio = 1;

    public LongAdder timeoutAddr = new LongAdder();
    public AtomicLong count = new AtomicLong(0);
    private static AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
    private static long interval = TimeUnit.SECONDS.toMillis(3);

    //weight *(500/repTime) * (1000.0/(active+1000.0))*timeoutRatio
    public long getWeight() {
        return (long) Math.max(DEFAULT_WEIGHT, (100.0 / active.get()) * weight.get() * timeoutRatio);
    }

    public void setWeight(long w) {
        if (weight.get() != w) {
            weight.set(w);
        }
    }

    public void addTimeout(long timeout) {
        count.getAndIncrement();
        timeoutAddr.add(timeout);
        resetTimeout();
    }

    public void resetTimeout() {
        long l = lastTime.get();
        if (System.currentTimeMillis() >= l) {
            if (lastTime.compareAndSet(l, l + interval)) {
                if (count.get() == 0) {
                    return;
                }
                long l1 = timeoutAddr.longValue();
                timeoutRatio = (50.0 * count.get() / (l1 == 0 ? 1 : l1));
                timeoutAddr.reset();
                count.set(0);
            }
        }
    }
}
