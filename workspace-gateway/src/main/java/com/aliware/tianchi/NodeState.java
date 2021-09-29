package com.aliware.tianchi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.exp;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 节点的状态
 * @since 2021/9/10 14:00
 */
public class NodeState {
    private final static Logger logger = LoggerFactory.getLogger(NodeState.class);
    private static final long timeInterval = TimeUnit.SECONDS.toMillis(3);
    public volatile long serverActive = 1;
    public volatile double cm = 1;
    public AtomicLong failure = new AtomicLong(0);
    public AtomicLong total = new AtomicLong(1);
    private static final double ALPHA = 1 - exp(-5 / 60.0);
    public volatile double failureRatio = 0;
    private final AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());


    public long getWeight() {
        return (long) Math.max(1, serverActive * 100 * (1 - failureRatio) * cm);
    }

    public void setServerActive(long w) {
        serverActive = w;
    }

    public void setCM(double c) {
        cm = c;
    }

    public void end(boolean error) {
        total.getAndIncrement();
        if (error) {
            failure.getAndIncrement();
        }
        calculateFailure();
    }


    public void calculateFailure() {
        long l = lastTime.get();
        if (System.currentTimeMillis() >= l) {
            if (lastTime.compareAndSet(l, l + timeInterval)) {
                long c = total.getAndSet(0);
                long f = failure.getAndSet(0);
                if (c != 0) {
                    int instantRate = (int) (f / c);
                    double fr = failureRatio;
                    failureRatio = Math.max(0, fr + (int) (ALPHA * (instantRate - fr)));
                    logger.info("calculateFailure:{}", failureRatio);
                }
            }
        }
    }

}
