package com.aliware.tianchi;

import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 这里先暂定是2的n次方
 * @since 2021/10/21 10:44
 */
public class PooledTimer implements Timer {
    private final AtomicInteger idx = new AtomicInteger();
    final HashedWheelTimer[] timers;
    final int mod;

    public PooledTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit, int poolSize) {
        this(threadFactory, tickDuration, unit, 512, poolSize);
    }

    public PooledTimer(ThreadFactory threadFactory,
                       long tickDuration, TimeUnit unit, int ticksPerWheel, int poolSize) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, -1, poolSize);
    }

    public PooledTimer(ThreadFactory threadFactory,
                       long tickDuration, TimeUnit unit, int ticksPerWheel,
                       long maxPendingTimeouts, int poolSize) {
        poolSize = normalizeTicksPerWheel(poolSize);
        timers = new HashedWheelTimer[poolSize];
        for (int i = 0; i < poolSize; i++) {
            timers[i] = new HashedWheelTimer(threadFactory, tickDuration, unit, ticksPerWheel, maxPendingTimeouts);
        }
        mod = poolSize - 1;
    }

    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        // 这里参考java8 hashmap的算法，使推算的过程固定
        int n = ticksPerWheel - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= 1073741824) ? 1073741824 : n + 1;
    }

    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        return timers[idx.getAndDecrement() & mod].newTimeout(task, delay, unit);
    }

    @Override
    public Set<Timeout> stop() {
        return Arrays.stream(timers).flatMap(hashedWheelTimer -> hashedWheelTimer.stop().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public boolean isStop() {
        return Arrays.stream(timers).allMatch(HashedWheelTimer::isStop);
    }
}
