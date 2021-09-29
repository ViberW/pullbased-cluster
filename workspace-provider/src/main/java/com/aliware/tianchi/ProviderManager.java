package com.aliware.tianchi;

import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 可以使用个Map保存不同Invoker对应的ProviderManager, 这里就先使用单独的
 * @since 2021/9/10 14:32
 */
public class ProviderManager {
    private static SystemInfo si = new SystemInfo();
    private static HardwareAbstractionLayer hal = si.getHardware();
    private static ScheduledExecutorService scheduledExecutor;
    private static volatile boolean once = true;
    public static volatile long weight = 10;
    private final static Logger logger = LoggerFactory.getLogger(ProviderManager.class);

    //////
    private static final long timeInterval = TimeUnit.SECONDS.toNanos(1);
    private static final long okInterval = TimeUnit.MILLISECONDS.toNanos(15);
    private static final long windowSize = 2;
    private static final Counter counter = new Counter();
    private static final Counter okCounter = new Counter();
    private static final Counter wCounter = new Counter();
    public static AtomicInteger active = new AtomicInteger(1);
    //////

    public static void maybeInit(Invoker<?> invoker) {
        if (once) {
            synchronized (ProviderManager.class) {
                if (once) {
                    scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
                    scheduledExecutor.scheduleWithFixedDelay(new SystemTask(),
                            0, 1000, TimeUnit.MILLISECONDS);
                    //这个单线程处理
                    scheduledExecutor.scheduleWithFixedDelay(new WeightTask(),
                            200, 100, TimeUnit.MILLISECONDS);
                    once = false;
                }
            }
        }
    }

    private static double mr = calculateMemory();
    private static double cr = calculateCPURatio();
    static volatile double cm = 0;

    private static class SystemTask implements Runnable {
        @Override
        public void run() {
            double lastm = mr;
            double lastc = cr;
            double m = (mr = calculateMemory()) - lastm;
            double c = (cr = calculateCPURatio()) - lastc;
            if (m > 0.15 || c > 0.15) {
                cm = (m > 0.15 && c > 0.15 ? 0.5 : 0.75);
            } else {
                cm = 1;
            }
            logger.info("SystemTask:{}", cm);
        }
    }

    private static class WeightTask implements Runnable {
        @Override
        public void run() {
            long wp = weight;
            long high = offset();
            long low = high - windowSize;

            long wCnt = wCounter.sum(low, high);
            long okCnt = okCounter.sum(low, high);
            long sum = counter.sum(low, high);
            long expectW = okCnt == 0 ? wp : wCnt / okCnt;
            double r = sum == 0 ? 0 : (okCnt * 1.0 / sum);
            long w;
            if (r < 0.8) {
                w = expectW == 0 ? wp / 2 : Math.min(wp / 2, expectW);
            } else {
                w = Math.max(wp, expectW);
            }
            logger.info("WeightTask:{}, expectW:{}", w, expectW);
            cm = 1;
            weight = Math.max(1, w);
            clean(high);
        }
    }

    public static void time(long duration, int count) {
        long offset = offset();
        counter.add(offset, 1);
        if (duration < okInterval) {
            okCounter.add(offset, 1);
            wCounter.add(offset, count);
        }
    }

    public static long offset() {
        return System.currentTimeMillis() / timeInterval;
    }

    public static void clean(long high) {
        long toKey = high - (windowSize << 1);
        counter.clean(toKey);
        okCounter.clean(toKey);
        wCounter.clean(toKey);
    }

    private static double calculateMemory() {
        GlobalMemory memory = hal.getMemory();
        long total = memory.getTotal();
        return total - memory.getAvailable() * 1.0 / total;
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
