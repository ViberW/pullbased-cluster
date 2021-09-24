package com.aliware.tianchi;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static java.lang.Math.exp;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2021/9/10 14:32
 */
public class ProviderManager {
    private static AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
    private static long interval = TimeUnit.SECONDS.toMillis(3);
    private static SystemInfo si = new SystemInfo();
    private static HardwareAbstractionLayer hal = si.getHardware();
    //记录当前的可用连接数
    public static AtomicLong activeConnect = new AtomicLong(1);
    public static volatile long maxWeight;
    public static LongAdder longAdder = new LongAdder();
    public static AtomicLong count = new AtomicLong(1);
    public static AtomicBoolean once = new AtomicBoolean(false);

    private static LongAdder adder = new LongAdder();
    private static AtomicLong timeCount = new AtomicLong(0);
    private static final double ALPHA = 1 - exp(-5 / 60.0);
    private static AtomicLong lastCntTime = new AtomicLong(System.currentTimeMillis());
    private static volatile int rate = 1;
    private static volatile boolean initialized = false;

    public static void endTime(long elapsed) {
        adder.add(elapsed);
        timeCount.getAndIncrement();
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
                if (initialized) {
                    rate = Math.max(1, rate + (int) (ALPHA * (instantRate - rate)));
                } else {
                    rate = Math.max(1, instantRate);
                    initialized = true;
                }
            }
        }
        return rate;
    }


    public static double calculateWeight() {
        long l = lastTime.get();
        if (System.currentTimeMillis() >= l) {
            if (lastTime.compareAndSet(l, l + interval)) {
                //一旦超过80%, 就触发记录当前的可用的连接数
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
    }

    private static double calculateMemory() {
        GlobalMemory memory = hal.getMemory();
        long l = (memory.getVirtualMemory().getVirtualMax() - memory.getVirtualMemory().getVirtualInUse());//MB
        return l * 1.0 / memory.getVirtualMemory().getVirtualMax();
    }

    private static double calculateCPURatio() {
        CentralProcessor processor = hal.getProcessor();
        long[] systemCpuLoadTicks = processor.getSystemCpuLoadTicks();
        long idel = systemCpuLoadTicks[CentralProcessor.TickType.IDLE.getIndex()];
        long total = 0;
        for (CentralProcessor.TickType tickType : CentralProcessor.TickType.values()) {
            total += systemCpuLoadTicks[tickType.getIndex()];
        }
        return idel * 1.0 / total;
    }
}
