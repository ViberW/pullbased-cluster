package com.aliware.tianchi;

import java.util.concurrent.atomic.LongAdder;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2021/10/12 17:57
 */
public class SumCounter {

    private LongAdder total = new LongAdder();
    private LongAdder concurrent = new LongAdder();
    private LongAdder duration = new LongAdder();

    public LongAdder getTotal() {
        return total;
    }

    public void setTotal(LongAdder total) {
        this.total = total;
    }

    public LongAdder getConcurrent() {
        return concurrent;
    }

    public void setConcurrent(LongAdder concurrent) {
        this.concurrent = concurrent;
    }

    public LongAdder getDuration() {
        return duration;
    }

    public void setDuration(LongAdder duration) {
        this.duration = duration;
    }
}
