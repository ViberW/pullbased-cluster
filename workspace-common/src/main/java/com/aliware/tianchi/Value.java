package com.aliware.tianchi;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2021/10/22 14:09
 */
public class Value extends PrePadding {
    public volatile int value;
    public int pm;

    protected long p9, p10, p11, p12, p13, p14, p15;

    public Value(int value) {
        this.value = value;
    }
}
