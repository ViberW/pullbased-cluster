package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
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
    public static long weight = 10;
    private final static Logger logger = LoggerFactory.getLogger(ProviderManager.class);

    //////
    private static final long timeInterval = TimeUnit.SECONDS.toNanos(1);
    private static final long okInterval = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long windowSize = 5;
    private static final Counter counter = new Counter();
    private static final Counter okCounter = new Counter();
    private static final Counter okActive = new Counter();
    private static int lastCPU = hal.getProcessor().getLogicalProcessorCount();
    private static long lastMemory = hal.getMemory().getTotal();
    private static double change = 1D;
    public static AtomicInteger active = new AtomicInteger(1);
    //////

    public static void maybeInit(Invoker<?> invoker) {
        if (once) {
            synchronized (ProviderManager.class) {
                if (once) {
                    weight = invoker.getUrl().getParameter(CommonConstants.THREADS_KEY, CommonConstants.DEFAULT_THREADS) / 2;
                    scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
                    //这个单线程处理
                    scheduledExecutor.scheduleWithFixedDelay(new WeightTask(),
                            1000, 1000, TimeUnit.MILLISECONDS);
                    scheduledExecutor.scheduleWithFixedDelay(new SystemTask(),
                            0, 3000, TimeUnit.MILLISECONDS);
                    once = false;
                }
            }
        }
    }

    private static class SystemTask implements Runnable {
        @Override
        public void run() {
            CentralProcessor processor = hal.getProcessor();
            int cpu = processor.getLogicalProcessorCount();
            long memory = hal.getMemory().getTotal();
            if (memory >= lastMemory && cpu >= lastCPU) {
                change = 1;
            } else {
                change = (memory >= lastMemory || cpu >= lastCPU) ? 0.75 : 0.5;
            }
            lastMemory = memory;
            lastCPU = cpu;
            logger.info("SystemTask :{}", change);
        }
    }

    private static class WeightTask implements Runnable {
        @Override
        public void run() {
            long high = offset();
            long low = high - windowSize;
            long active = okCounter.sum(low, high);
            long sum = counter.sum(low, high);
            double r = sum == 0 ? 0 : (active * 1.0 / sum);
            long avg = active == 0 ? 0 : (okCounter.sum(low, high) / active);
            if (r < 0.8) {
                weight = Math.min((long) (weight * r + 1), avg);
            } else {
                weight = Math.max(weight, avg);
            }
            weight = (long) Math.max(1, change * weight);
            logger.info("WeightTask :{}", weight);
            change = 1;
            clean(high);
        }
    }

    public static void time(long offset, long duration, int count) {
        if (duration < okInterval) {
            okActive.add(offset, count); //记录小于10ms的处于并发的数量
            okCounter.add(offset, 1);
        }
        counter.add(offset, 1);
    }

    public static long offset() {
        return System.currentTimeMillis() / timeInterval;
    }

    public static void clean(long high) {
        long toKey = high - (windowSize << 1);
        counter.clean(toKey);
        okActive.clean(toKey);
        okCounter.clean(toKey);
    }
}
