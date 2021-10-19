package com.aliware.tianchi;

import java.util.concurrent.atomic.LongAdder;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2021/10/13 9:48
 */
public class StateCounter {

    private LongAdder total = new LongAdder();
    //    private LongAdder duration = new LongAdder();
    private LongAdder failure = new LongAdder();

    public LongAdder getTotal() {
        return total;
    }

    public void setTotal(LongAdder total) {
        this.total = total;
    }

    public LongAdder getFailure() {
        return failure;
    }

    public void setFailure(LongAdder failure) {
        this.failure = failure;
    }
//    public LongAdder getDuration() {
//        return duration;
//    }
//
//    public void setDuration(LongAdder duration) {
//        this.duration = duration;
//    }
}
