package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.rpc.Invoker;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

    //////
    private static final long timeInterval = TimeUnit.SECONDS.toNanos(1);
    private static final long okInterval = TimeUnit.MILLISECONDS.toNanos(20);
    private static final long avgInterval = TimeUnit.MILLISECONDS.toNanos(15);
    private static final long windowSize = 5;
    private static final Counter timeCounter = new Counter();
    private static final Counter counter = new Counter();
    private static final Counter okCounter = new Counter();
    private static double lastAvg = 1;
    private static int lastCPU = hal.getProcessor().getLogicalProcessorCount();
    private static long lastMemory = hal.getMemory().getTotal();
    private static double change = 1D;
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
        }
    }

    private static class WeightTask implements Runnable {
        @Override
        public void run() {
            long high = offset();
            long low = high - windowSize;
            long sum = counter.sum(low, high);
            double avg = sum == 0 ? 0 : (timeCounter.sum(low, high) * 1.0 / sum);
            if (avg > avgInterval) {
                weight = (long) Math.min(weight * lastAvg / avg, okCounter.max(low, high));
            } else {
                weight = Math.max(weight, okCounter.max(low, high)) + 1;
            }
            weight = (long) Math.max(1, change * weight) * 10;
            change = 1;
            lastAvg = avg;
            clean(high);
        }
    }

    public static void time(long offset, long duration) {
        timeCounter.add(offset, duration);
        if (duration < okInterval) {
            okCounter.add(offset, 1);
        }
        counter.add(offset, 1);
    }

    public static long offset() {
        return System.currentTimeMillis() / timeInterval;
    }

    public static void clean(long high) {
        long toKey = high - (windowSize << 1);
        timeCounter.clean(toKey);
        counter.clean(toKey);
        okCounter.clean(toKey);
    }

    /////////////////////////////////
   /* private static AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
    private static long interval = TimeUnit.SECONDS.toMillis(3);
    private static SystemInfo si = new SystemInfo();
    private static HardwareAbstractionLayer hal = si.getHardware();
    //记录当前的可用连接数
    public static AtomicLong activeConnect = new AtomicLong(1);
    public static long maxWeight;
    public static LongAdder longAdder = new LongAdder();
    public static AtomicLong count = new AtomicLong(1);
    public static AtomicBoolean once = new AtomicBoolean(false);

    private static LongAdder adder = new LongAdder();
    private static AtomicLong timeCount = new AtomicLong(0);
    private static final double ALPHA = 1 - exp(-5 / 60.0);
    private static AtomicLong lastCntTime = new AtomicLong(System.currentTimeMillis());
    private static int rate = 1;
    public static final int limit = 100;

    private static Timer scheduler = new HashedWheelTimer(
            new NamedThreadFactory("provider-schedule-timer", true),
            30, TimeUnit.MILLISECONDS, 40);

    public static void endTime(long elapsed) {
        adder.add(elapsed);
        timeCount.getAndIncrement();
        //为了减少CPU的影响, 需要减少线程的处理
    }


    public static long calculateTime() {
        long l = lastCntTime.get();
        if (System.currentTimeMillis() >= l) {
            if (lastCntTime.compareAndSet(l, l + interval)) {
                long c = timeCount.getAndSet(0);
                long sum = adder.sumThenReset();
                if (c == 0) {
                    return rate;
                }
                int instantRate = (int) (sum / c);
                rate = Math.max(1, rate + (int) (ALPHA * (instantRate - rate)));
                maxWeight = (long) (activeConnect.get() * limit * 1.0 / rate);
            }
        }
        return rate;
    }


    public static double calculateWeight() {
        long l = lastTime.get();
        if (System.currentTimeMillis() >= l) {
            if (lastTime.compareAndSet(l, l + interval)) {
                //一旦超过80%, 就触发记录当前的可用的连接数 -- 若是CPU和MEMORY发生变化, 则需要调整
                double ratio = Math.max(calculateCPURatio(), calculateMemory());
                if (ratio >= 0.8d) {
                    long c = count.get();
                    if (c == 0) {
                        maxWeight = activeConnect.get();
                    } else {
                        maxWeight = Math.max(longAdder.longValue() / c, activeConnect.get());
                        if (c % 100 == 99) {
                            longAdder.reset();
                            count.set(0);
                        }
                        longAdder.add(maxWeight);
                        count.getAndIncrement();
                    }
                    once.set(true);
                } else if (!once.get()) {
                    maxWeight = activeConnect.get();
                }
            }
        }
        return maxWeight;
    }*/

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
