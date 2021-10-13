package com.aliware.tianchi;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 基于skipList构造请求的超时时间
 * @since 2021/9/28 17:02
 */
public class Counter<T> {

    private ConcurrentSkipListMap<Long, T> data =
            new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    Function<Long, T> function;

    public Counter(Function<Long, T> function) {
        this.function = function;
    }

    public Collection<T> sub(long fromOffset, long toOffset) {
        return sub(fromOffset, true, toOffset, true);
    }

    public Collection<T> sub(long fromOffset, boolean fromInclusive, long toOffset, boolean toInclusive) {
        return data.subMap(toOffset, toInclusive, fromOffset, fromInclusive)
                .values();
    }

    public void clean(long toOffset) {
        clean(toOffset, false);
    }


    public void clean(long toOffset, boolean toInclusive) {
        data.tailMap(toOffset, toInclusive).clear();
    }

    public T get(long offset) {
        return data.computeIfAbsent(offset, function::apply);
    }
}
