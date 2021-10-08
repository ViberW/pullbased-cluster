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
    public static volatile long weight = 1;
    private final static Logger logger = LoggerFactory.getLogger(ProviderManager.class);

    //////
    private static final long timeInterval = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long okInterval = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long levelInterval = TimeUnit.MILLISECONDS.toNanos(6);
    private static final long windowSize = 6;
    private static final Counter counter = new Counter();
    private static final Counter okCounter = new Counter();
    public static AtomicInteger active = new AtomicInteger(1);
    //////

    public static void maybeInit(Invoker<?> invoker) {
        if (once) {
            synchronized (ProviderManager.class) {
                if (once) {
                    weight = invoker.getUrl().getParameter(CommonConstants.THREADS_KEY, CommonConstants.DEFAULT_THREADS) / 2;
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
//            logger.info("SystemTask:{}", cm);
        }
    }

    private static class WeightTask implements Runnable {
        @Override
        public void run() {
            long high = offset();
            long low = high - 1 - windowSize;
            int ac = active.get();
            double overRatio = ac == 0 ? 0 : (Math.max(0, ac - counter.onlySum(high)) * 1.0 / ac);
            long okCnt = okCounter.sum(low, high - 1);
            long sum = counter.sum(low, high - 1);
            long r = sum == 0 ? 1 : (okCnt / sum);
            long w;
            if (r > okInterval || overRatio > 0.1) {
                long reduce = r > okInterval ? r / okInterval : (long) (overRatio * 10);
                w = weight - reduce;
            } else if (r > levelInterval) {
                w = weight;
            } else {
                w = weight + 1;
            }
            weight = Math.max(1, w);
            clean(high); //需要计算出一个可靠的运行量
        }
    }

    public static void time(long offset, long duration) {
        counter.add(offset, 1);
        okCounter.add(offset, duration);
    }

    public static long offset() {
        return System.nanoTime() / timeInterval;
    }

    public static void clean(long high) {
        long toKey = high - (windowSize << 1);
        counter.clean(toKey);
        okCounter.clean(toKey);
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
