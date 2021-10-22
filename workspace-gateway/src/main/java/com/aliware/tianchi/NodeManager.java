package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.exp;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 数值范围从1~100, 值越小则越能够使用到
 * @since 2021/9/10 13:55
 */
public class NodeManager {
    private final static Logger logger = LoggerFactory.getLogger(NodeManager.class);
    //帮助定期的减少Node的信息
    private static final Map<String, NodeState> STATES = new ConcurrentHashMap<>();
    //用的时间不长, 就单个的
    private static ScheduledExecutorService scheduledExecutor;
    public static final AtomicLong active = new AtomicLong(1);
    private static final double ALPHA = 1 - exp(-2 / 10.0);

    public static volatile boolean balance = false;
    public static Value fullWeight = new Value(0);

    static {
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduledExecutor.shutdown();
        }));
        //定时统计所有的量总和
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            if (STATES.isEmpty()) {
                return;
            }
            int totalWeight = 0;
            for (Map.Entry<String, NodeState> entry : STATES.entrySet()) {
                totalWeight += entry.getValue().getFullWeight();
            }
            int value = fullWeight.value;
            if (balance) {
                totalWeight = (int) (value + (totalWeight - value) * ALPHA);
                logger.info("NodeManager:{}", totalWeight);
            } else if (Math.abs(totalWeight - value) < 0.05 * value) {
                balance = true;
            }
            fullWeight.value = totalWeight;
        }, 10, 1, TimeUnit.SECONDS);
    }

    public static NodeState state(Invoker<?> invoker) {
        return STATES.computeIfAbsent(buildString(invoker), s -> new NodeState(scheduledExecutor));
    }

    private static String buildString(Invoker<?> invoker) {
        URL url = invoker.getUrl();
        return new StringBuilder()
                .append(url.getHost())
                .append(":")
                .append(url.getPort()).toString();
    }
}
