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
import java.util.concurrent.atomic.AtomicInteger;

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

    public static Value weight = new Value(50);
    public static final AtomicInteger active = new AtomicInteger(1);//变成1?
    public static Value executeTime = new Value(10);
    private static final long windowSize = 10;
    static final long littleMillis = TimeUnit.MILLISECONDS.toNanos(1) / 100;
    static final int levelCount = 100; //能够支持统计tps的请求数
    private static final Counter<SumCounter[]> counters = new Counter<>(l -> {
        SumCounter[] sumCounters = new SumCounter[7];
        for (int i = 0; i < 7; i++) {
            sumCounters[i] = new SumCounter();
        }
        return sumCounters;
    });
    private static final long timeInterval = TimeUnit.MILLISECONDS.toNanos(100);//200?

    static {
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutor.scheduleWithFixedDelay(new CalculateTask(), 1000,
                200, TimeUnit.MILLISECONDS); //100?--数值太少, 没法合理计算感觉
    }

    private static void resetWeight(int w) {
        weight.value = w;//这个值往前偏移3个单位?
    }

    private static void resetExecuteTime(int et) {
        executeTime.value = et;
    }

    public static void time(long duration, int concurrent) {
        int w = weight.value;
        if (Math.abs(concurrent - w) <= 3) { //说明需要调整到对应的位置上去
            long offset = offset();
            SumCounter[] sumCounters = counters.get(offset);
//            SumCounter sumCounter = sumCounters[(concurrent - w + 6) >> 1];
            SumCounter sumCounter = sumCounters[concurrent - w + 3];
            sumCounter.getTotal().add(1);
            sumCounter.getDuration().add(duration);
        }
    }

    private static class CalculateTask implements Runnable {
        @Override
        public void run() {
            long high = offset();
            long low = high - windowSize;

            Collection<SumCounter[]> sub = counters.sub(low, high);
            int[] counts = {0, 0, 0, 0, 0, 0, 0};
            long[] durations = {0, 0, 0, 0, 0, 0, 0};
            if (!sub.isEmpty()) {
                sub.forEach(state -> {
                    SumCounter counter;
                    for (int i = 0; i < 7; i++) {
                        counter = state[i];
                        counts[i] += counter.getTotal().sum();
                        durations[i] += counter.getDuration().sum();
                    }
                });
            }
            long toKey = high - (windowSize << 1);
            if (counts[3] > levelCount) {
                int v = weight.value;
//                int[] weights = {v - 6, v - 4, v - 2, v, v + 2, v + 4, v + 6};
                int[] weights = {v - 3, v - 2, v - 1, v, v + 1, v + 2, v + 3};
                long[] tps = new long[7];
                int maxIndex = 0;
                long maxTps = 0;
                int targetTime = executeTime.value;
                for (int i = 0; i < 7; i++) {
                    if (counts[i] > levelCount) {
                        double avgTime = Math.max(1.0, ((int) (((durations[i] / counts[i]) / littleMillis))) / 100.0); //保证1.xx的时间
                        long t = (long) ((1000.0 / avgTime) * weights[i]);//1s时间的tps
                        tps[i] = t;
                        if (maxTps < t) {
                            maxIndex = i;
                            maxTps = t;
                        }
                        targetTime = Math.max(targetTime, (int) (Math.ceil(1.2 * avgTime)));
                    }
                }
                long curTps = tps[3];
                if (maxIndex > 3) {
                    int total = 0;
                    int most = 0;
                    for (int i = 4; i < 7; i++) {
                        if (tps[i] > 0) {
                            total++;
                            if (tps[i] >= curTps) {
                                most++;
                            }
                        }
                    }
                    if (most * 1.0 / total >= 0.5) {
                        resetWeight(v + 1);
                    }
                } else if (maxIndex < 3) {
                    int total = 0;
                    int most = 0;
                    for (int i = 0; i < 3; i++) {
                        if (tps[i] > 0) {
                            total++;
                            if (tps[i] >= curTps) {
                                most++;
                            }
                        }
                    }
                    if (most * 1.0 / total >= 0.5) {
                        resetWeight(v - 1);
                    }
                }
                //存放和合适的超时时间
                resetExecuteTime(targetTime);
            }
            counters.clean(toKey);
        }

    }

    public static long offset() {
        return System.nanoTime() / timeInterval;
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
