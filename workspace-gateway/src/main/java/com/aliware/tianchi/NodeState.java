package com.aliware.tianchi;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 节点的状态
 * @since 2021/9/10 14:00
 */
public class NodeState {

    public AtomicLong serverActive = new AtomicLong(1);
    public AtomicLong clientActive = new AtomicLong(1);
    public volatile int cnt = 1000;

    public long getWeight() {
        return (serverActive.get() * 10 - Math.min(serverActive.get(), clientActive.get()) * 8) * cnt;
    }

    public void setServerActive(long w) {
        if (serverActive.get() != w) {
            serverActive.set(w);
        }
    }

    public void setCnt(int c) {
        this.cnt = c;
    }


}
