package com.aliware.tianchi;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2021/10/22 14:09
 */
public class Value extends PrePadding {
    public volatile long value;

    protected long p9, p10, p11, p12, p13, p14, p15;

    public Value(long value) {
        this.value = value;
    }
}
