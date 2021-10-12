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
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.exp;

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
    public static volatile double okRatio = 1;
    public static volatile int weight = 200;
    private final static Logger logger = LoggerFactory.getLogger(ProviderManager.class);

    private static final long timeInterval = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long okInterval = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long windowSize = 6;
    private static final Counter counter = new Counter();
    private static final Counter timeCounter = new Counter();
    private static final Counter concurrentCounter = new Counter();
    public static final AtomicLong active = new AtomicLong(1);
    private static final double ALPHA = 1 - exp(-10.0 / 100); //100毫秒, 每10ms的间隔数据
    private static int lastok = 200;
    private static double lastRatio = 1;

    public static void maybeInit(Invoker<?> invoker) {
        if (once) {
            synchronized (ProviderManager.class) {
                if (once) {
                    scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
                    //这个单线程处理
                    scheduledExecutor.scheduleWithFixedDelay(new WeightTask(),
                            200, 100, TimeUnit.MILLISECONDS);
                    once = false;
                }
            }
        }
    }

    private static class WeightTask implements Runnable {
        @Override
        public void run() {
            long high = offset();
            long low = high - windowSize;
            long sum = counter.sum(low, high);
            if (sum > 0) {
                int ok = (int) timeCounter.sum(low, high);
                double r = ok * 1.0 / sum;
                int tmp = ok;
                if (r > 0.9) {
                    if (lastRatio > 0.9 && lastok > ok) {
                        ok = lastok;
                    } else {
                        ok = (int) (lastok + (ok - lastok) * ALPHA);
                    }
                } else if (lastRatio > 0.9) {
                    ok = lastok - 1;
                } else {
                    ok = (int) (lastok + (Math.min(ok, lastok) - 1 - lastok) * ALPHA);
                }
                lastok = tmp;
                lastRatio = r;
                weight = ok;

                r = okRatio + (r - okRatio) * ALPHA;
                logger.info("WeightTask.okRatio:{}- {}", r, ok);
                okRatio = Math.max(0, r);
            }
            clean(high);
        }
    }

    public static void time(long duration) {
        long offset = ProviderManager.offset();
        if (duration < okInterval) {
            timeCounter.add(offset, 1);
        }
        counter.add(offset, 1);
    }

    public static long offset() {
        return System.nanoTime() / timeInterval;
    }

    public static void clean(long high) {
        long toKey = high - (windowSize << 1);
        counter.clean(toKey);
        timeCounter.clean(toKey);
        concurrentCounter.clean(toKey);
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
