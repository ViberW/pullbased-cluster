package com.aliware.tianchi;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static java.lang.Math.exp;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 节点的状态
 * @since 2021/9/10 14:00
 */
public class NodeState {
    private static final long windowSize = 5;
    private static final long timeInterval = TimeUnit.SECONDS.toMillis(1);
    public volatile long serverActive = 1;
    public AtomicLong failure = new AtomicLong(0);
    public AtomicLong total = new AtomicLong(1);
    //    public volatile long cnt = 1;
    public AtomicLong active = new AtomicLong();
    public static final long limit = TimeUnit.MILLISECONDS.toNanos(30);
    private static final double ALPHA = 1 - exp(-5 / 60.0);
    public double failureRatio = 0;
    private AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());

    public LongAdder delay = new LongAdder();
    public LongAdder delayCnt = new LongAdder();
    private AtomicLong lastDelay = new AtomicLong(System.currentTimeMillis());
    public double delayTime = 1;
    private static final long one = TimeUnit.MILLISECONDS.toNanos(1);


    public long getWeight() {
        return (long) Math.max(1, ((serverActive * 100 - Math.min(serverActive, active.get()) * 80)
                * (1 - failureRatio)) * delayTime);
    }

    public void setServerActive(long w) {
        serverActive = w;
    }

    public void end(boolean ok) {
        total.getAndIncrement();
        if (!ok) {
            failure.getAndIncrement();
        }
        calculateTime();
    }

    public void delay(long d) {
        delay.add(d);
        delayCnt.add(1);
        calculateDelay();
    }

    public void calculateDelay() {
        long l = lastDelay.get();
        if (System.currentTimeMillis() >= l) {
            if (lastDelay.compareAndSet(l, l + timeInterval)) {
                long c = delayCnt.sumThenReset();
                long f = delay.sumThenReset();
                if (c != 0 && f != 0) {
                    int dt = (int) (f / c);
                    delayTime = Math.max(0.1, delayTime + (int) (ALPHA * (limit / dt - delayTime)));
                }
            }
        }
    }

    public void calculateTime() {
        long l = lastTime.get();
        if (System.currentTimeMillis() >= l) {
            if (lastTime.compareAndSet(l, l + timeInterval)) {
                long c = total.getAndSet(0);
                long f = failure.getAndSet(0);
                if (c != 0) {
                    int instantRate = (int) (f / c);
                    failureRatio = Math.max(1, failureRatio + (int) (ALPHA * (instantRate - failureRatio)));
                }
            }
        }
    }

}
