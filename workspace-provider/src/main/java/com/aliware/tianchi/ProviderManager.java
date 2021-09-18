package com.aliware.tianchi;

import oshi.SystemInfo;
import oshi.hardware.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
    private static volatile double weight = 1.0;

    public static double calculateWeight() {
        long l = lastTime.get();
        if (System.currentTimeMillis() >= l) {
            if (lastTime.compareAndSet(l, l + interval)) {
                //memory*cpu*disk*net)
                double ratio = calculateCPURatio();
                double mRatio = calculateMemory();
                weight = Math.max(1.0, mRatio * ratio * 1000);
                return weight;
            }
        }
        return weight;
    }

    private static double calculateMemory() {
        GlobalMemory memory = hal.getMemory();
        long l = (memory.getVirtualMemory().getVirtualMax() - memory.getVirtualMemory().getVirtualInUse())
                / (1024 * 1024);//MB
        //需要一个简单的处理方式 进行压缩
        double ratio = l * 1.0 / memory.getVirtualMemory().getVirtualMax();
        //计算真实可用的内存
        return Math.max(1, ratio * l);
    }

    private static double calculateCPURatio() {
        CentralProcessor processor = hal.getProcessor();
        long[] systemCpuLoadTicks = processor.getSystemCpuLoadTicks();
        long idel = systemCpuLoadTicks[CentralProcessor.TickType.IDLE.getIndex()];
        long total = 0;
        for (CentralProcessor.TickType tickType : CentralProcessor.TickType.values()) {
            total += systemCpuLoadTicks[tickType.getIndex()];
        }
        return total < 0 ? 0.01 : Math.max(0.01, idel * 1.0 / total);
    }
}
