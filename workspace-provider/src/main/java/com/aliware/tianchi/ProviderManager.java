package com.aliware.tianchi;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2021/9/10 14:32
 */
public class ProviderManager {
    private static AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
    private static long interval = TimeUnit.SECONDS.toMillis(2);
    private static SystemInfo si = new SystemInfo();
    private static HardwareAbstractionLayer hal = si.getHardware();
    //记录当前的可用连接数
    public static AtomicLong activeConnect = new AtomicLong(1);
    public static volatile long maxWeight;
    public static LongAdder longAdder = new LongAdder();
    public static AtomicLong count = new AtomicLong(1);
    public static AtomicBoolean once = new AtomicBoolean(false);

    private static int capacity = 2000000;
    private static int preSize = 500;
    private static long[] start = new long[capacity + preSize];
    private static long[] end = new long[capacity + preSize];
    private static final int limit = 5;
    public static AtomicInteger requestCnt = new AtomicInteger(0);

    public static void beginTime(int cur) {
        cur %= capacity;
        cur += preSize;
        long now = System.nanoTime();
        start[cur] = now;
        end[cur] = -1;
        if (cur >= capacity) {
            start[cur - capacity] = now;
            end[cur - capacity] = -1;
        }
    }

    public static int endTime(int cur) {
        cur %= capacity;
        cur += preSize;
        long now = System.nanoTime();
        end[cur] = now;
        if (cur >= capacity) {
            end[cur - capacity] = now;
        }

        int c = 0;
        int short_c = 0;
        for (int i = cur; i >= 0; i--) {
            if (start[i] == 0 || end[i] - now > 500 * 1e6) {
                break;
            }
            if (end[i] != -1) {
                c++;
                if (end[i] - start[i] <= limit * 1e6) { //5ms
                    short_c++;
                }
            } else if (now - start[i] > limit * 1e6) {
                c++;
            }
            if (c >= 500) {
                break;
            }
        }
        if (c == 0) {
            return 1000;
        }
        return Math.min(1, short_c * 1000 / c);
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
