package com.aliware.tianchi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 节点的状态
 * @since 2021/9/10 14:00
 */
public class NodeState {
    private final static Logger logger = LoggerFactory.getLogger(NodeState.class);
    private static final long timeInterval = TimeUnit.SECONDS.toMillis(1);
    public volatile int serverActive = 1;
    public volatile double cm = 1;
    public LongAdder failure = new LongAdder();
    public LongAdder total = new LongAdder();
    //    private static final double ALPHA = 1 - exp(-1 / 60.0);
    public volatile double failureRatio = 0;
    private final AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
//    public volatile long layTime = 30;
//    public LongAdder lays = new LongAdder();
//    private static long oneMil = TimeUnit.MILLISECONDS.toNanos(1);

    public NodeState() {
    }

    public long getWeight() {
        return (long) (this.serverActive * (1 - failureRatio) * cm);
    }

    public void setServerActive(int w) {
        serverActive = w;
    }

    public void setCM(double c) {
        cm = c;
    }

    public void end(boolean error/*, long d*/) {
        total.add(1);
        if (error) {
            failure.add(1);
        }
//        if (d > 0) {
//            lays.add(d);
//        }
        calculateFailure();
    }


    public void calculateFailure() {
        long l = lastTime.get();
        if (System.currentTimeMillis() >= l) {
            if (lastTime.compareAndSet(l, l + timeInterval)) {
                long c = total.sumThenReset();
                long f = failure.sumThenReset();
//                long lay = lays.sumThenReset();
                if (c != 0) {
                    int instantRate = (int) (f / c);
                    double fr = failureRatio;
                    failureRatio = Math.max(0, fr + (int) (0.5 * (instantRate - fr)));

//                    long curLay = lay / (c * oneMil);
//                    long lastLayTime = layTime;
//                    layTime = Math.min(Math.max(0, lastLayTime + (long) (0.5 * (curLay - lastLayTime))), 60) + 10;
//                    logger.info("calculateLayTime:{}-{}", layTime, curLay);
                }
            }
        }
    }


}
