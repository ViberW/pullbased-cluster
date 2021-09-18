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

    public static final long DEFAULT_WEIGHT = 10;
    private static final double baseActive = 1000.0;

    public AtomicInteger active = new AtomicInteger(0);
    public AtomicLong weight = new AtomicLong(DEFAULT_WEIGHT);
    private volatile double timeoutRatio = 1;

    public LongAdder serverAdder = new LongAdder();
    public LongAdder clientAdder = new LongAdder();
    public AtomicLong count = new AtomicLong(0);
    private static AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
    private static long interval = TimeUnit.SECONDS.toMillis(3);

    //weight *(500/repTime) * (1000.0/(active+1000.0))*timeoutRatio
    public long getWeight() {
        return (long) Math.max(DEFAULT_WEIGHT, weight.get() * (baseActive / (active.get() + baseActive)) * timeoutRatio);
    }

    public void setWeight(long w) {
        if (weight.get() != w) {
            weight.set(w);
        }
    }

    public void addTimeout(long serverTimeout, long clientTimeout) {
        count.getAndIncrement();
        serverAdder.add(serverTimeout);
        clientAdder.add(clientTimeout);
        resetTimeout();
    }

    public void resetTimeout() {
        long l = lastTime.get();
        if (System.currentTimeMillis() >= l) {
            if (lastTime.compareAndSet(l, l + interval)) {
                if (count.get() == 0) {
                    return;
                }
                timeoutRatio = (50.0 * count.get() / serverAdder.longValue()) * 0.6
                        + (50.0 * count.get() / clientAdder.longValue());
                serverAdder.reset();
                clientAdder.reset();
                count.set(0);
            }
        }
    }
}
