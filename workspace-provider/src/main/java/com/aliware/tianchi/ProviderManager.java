package com.aliware.tianchi;

import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.exp;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 可以使用个Map保存不同Invoker对应的ProviderManager, 这里就先使用单独的
 * @since 2021/9/10 14:32
 */
public class ProviderManager {
    private final static Logger logger = LoggerFactory.getLogger(ProviderManager.class);
    private static SystemInfo si = new SystemInfo();
    private static HardwareAbstractionLayer hal = si.getHardware();
    private static ScheduledExecutorService scheduledExecutor;
    private static volatile boolean once = true;

    public static volatile int weight = 100;

    private static final long timeInterval = TimeUnit.MILLISECONDS.toNanos(200);
    private static final long okInterval = TimeUnit.MILLISECONDS.toNanos(8);
    public static long lastAvg = okInterval;
    private static final long windowSize = 5;
    private static final Counter<SumCounter> counter = new Counter<>(l -> new SumCounter());
    public static final AtomicLong active = new AtomicLong(1);
    private static final double ALPHA = 1 - exp(-1 / 5.0);

    public static void maybeInit(Invoker<?> invoker) {
        if (once) {
            synchronized (ProviderManager.class) {
                if (once) {
                    scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
                    scheduledExecutor.scheduleWithFixedDelay(new CalculateTask(), 1000,
                            200, TimeUnit.MILLISECONDS);
                    once = false;
                }
            }
        }
    }

    private static class CalculateTask implements Runnable {
        @Override
        public void run() {
            long high = offset();
            long low = high - windowSize;
            long[] ret = sum(low, high);
            if (ret[0] > 0) {
                long avgTime = ret[2] / ret[0];
                int concurrent = (int) (ret[1] / ret[0]);
                int nw = weight;
                if (avgTime > 0) {
                    if (avgTime < okInterval) {
                        if (concurrent > weight) {
                            nw = (int) (weight + (concurrent - weight) * ALPHA);
                        } else if (avgTime < okInterval * 0.75) {
                            nw = Math.max(weight, concurrent) + 1;
                        }
                    } else if (avgTime >= lastAvg) {
                        nw = Math.max(1, weight - 1);
                    }
                    lastAvg = avgTime;
                    weight = nw;
                }
            }
            clean(high);
        }
    }

    public static void time(long duration, long concurrent) {
        long offset = offset();
        SumCounter c = counter.get(offset);
        c.getTotal().add(1);
        c.getConcurrent().add(concurrent);
        c.getDuration().add(duration);
    }

    public static long offset() {
        return System.nanoTime() / timeInterval;
    }

    private static long[] sum(long fromOffset, long toOffset) {
        long[] result = {0, 0, 0};
        Collection<SumCounter> sub = counter.sub(fromOffset, toOffset);
        if (!sub.isEmpty()) {
            sub.forEach(state -> {
                result[0] += state.getTotal().sum();
                result[1] += state.getConcurrent().sum();
                result[2] += state.getDuration().sum();
            });
        }
        return result;
    }

    public static void clean(long high) {
        long toKey = high - (windowSize << 1);
        counter.clean(toKey);
    }

    private static double calculateMemory() {
        GlobalMemory memory = hal.getMemory();
        long total = memory.getTotal();
        return (total - memory.getAvailable()) * 1.0 / total;
    }

    private static double calculateCPURatio() {
        CentralProcessor processor = hal.getProcessor();
        long[] ticks = processor.getSystemCpuLoadTicks();
        long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] + ticks[CentralProcessor.TickType.IOWAIT.getIndex()];
        long total = 0;
        for (long tick : ticks) {
            total += tick;
        }
        return total > 0L && idle >= 0L ? (double) (total - idle) / (double) total : 0.0D;
    }
}
