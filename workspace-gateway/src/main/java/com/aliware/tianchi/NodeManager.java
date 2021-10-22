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

    static {
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduledExecutor.shutdown();
        }));
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
