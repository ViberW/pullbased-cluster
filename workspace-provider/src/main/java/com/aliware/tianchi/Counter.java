package com.aliware.tianchi;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.LongStream;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 基于skipList构造请求的超时时间
 * @since 2021/9/28 17:02
 */
public class Counter {

    private ConcurrentSkipListMap<Long, LongAdder> data =
            new ConcurrentSkipListMap<>(Comparator.reverseOrder());

    public void add(long offset, long n) {
        getOrCreate(offset).add(n);
    }

    public long sum(long fromOffset, long toOffset) {
        return sum(fromOffset, true, toOffset, true);
    }

    public long sum(long fromOffset, boolean fromInclusive, long toOffset, boolean toInclusive) {
        return data.subMap(toOffset, toInclusive, fromOffset, fromInclusive)
                .values()
                .stream()
                .mapToLong(LongAdder::sum)
                .sum();
    }

    public long max(long fromOffset, long toOffset) {
        return data.subMap(toOffset, true, fromOffset, true)
                .values()
                .stream()
                .mapToLong(LongAdder::sum)
                .max().orElse(0L);
    }

    public void clean(long toOffset) {
        clean(toOffset, false);
    }


    public void clean(long toOffset, boolean toInclusive) {
        data.tailMap(toOffset, toInclusive).clear();
    }

    private LongAdder getOrCreate(long offset) {
        return data.computeIfAbsent(offset, k -> new LongAdder());
    }
}
