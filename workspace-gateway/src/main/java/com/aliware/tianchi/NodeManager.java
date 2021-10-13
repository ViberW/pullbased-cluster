package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invoker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 数值范围从1~100, 值越小则越能够使用到
 * @since 2021/9/10 13:55
 */
public class NodeManager {

    //帮助定期的减少Node的信息
    private static final Map<String, NodeState> STATES = new ConcurrentHashMap<>();
    private static ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    public static NodeState state(Invoker<?> invoker) {
//        String uri = invoker.getUrl().toIdentityString();
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
