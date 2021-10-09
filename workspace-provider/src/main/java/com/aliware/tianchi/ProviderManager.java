package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
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
    public static volatile int weight = 1;
    private final static Logger logger = LoggerFactory.getLogger(ProviderManager.class);

    //////
    private static final long timeInterval = TimeUnit.MILLISECONDS.toNanos(5);
    private static final long okLevel = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long windowSize = 2;
    private static final Counter counter = new Counter();
    private static final Counter okCounter = new Counter();
    private static final Counter okActive = new Counter();
    public static AtomicInteger active = new AtomicInteger(1);
    //////

    public static void maybeInit(Invoker<?> invoker) {
        if (once) {
            synchronized (ProviderManager.class) {
                if (once) {
                    weight = invoker.getUrl().getParameter(CommonConstants.THREADS_KEY, CommonConstants.DEFAULT_THREADS);
                    scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
                    scheduledExecutor.scheduleWithFixedDelay(new SystemTask(),
                            0, 1000, TimeUnit.MILLISECONDS);
                    //这个单线程处理
                    scheduledExecutor.scheduleWithFixedDelay(new WeightTask(),
                            200, 10, TimeUnit.MILLISECONDS);
                    once = false;
                }
            }
        }
    }

    private static volatile double mr = calculateMemory();
    private static volatile double cr = calculateCPURatio();
    static volatile double cm = 0;

    private static class SystemTask implements Runnable {
        @Override
        public void run() {
            double lastm = mr;
            double lastc = cr;
            double m = lastm - (mr = calculateMemory());
            double c = lastc - (cr = calculateCPURatio());
            if (m > 0.15 || c > 0.15) {
                cm = (m > 0.15 && c > 0.15 ? 0.5 : 0.75);
            } else {
                cm = 1;
            }
        }
    }

    private static class WeightTask implements Runnable {
        @Override
        public void run() {
            long high = offset();
            long low = high - windowSize;
            long sum = counter.sum(low, high);
            long ok = okCounter.sum(low, high);
            double r = sum == 0 ? 1 : (ok * 1.0 / sum);
            int okCurrent = ok == 0 ? weight : (int) (okActive.sum(low, high) / ok);
            int w;
            if (r < 0.9) {
                w = (int) (weight / (2 - r));
            } else if (okCurrent > weight / 2) {
                w = (int) (weight + (okCurrent - weight) * 0.5);
            } else {
                w = weight;
            }
            cm = 1;
            logger.info("WeightTask:{}", w);
            weight = Math.max(1, w);
            clean(high);

        }
    }

    public static void time(long duration, int concurrent) {
        long offset = offset();
        counter.add(offset, 1);
        if (duration < okLevel) {
            okCounter.add(offset, 1);
            okActive.add(offset, concurrent);
        }
    }

    public static long offset() {
        return System.nanoTime() / timeInterval;
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
