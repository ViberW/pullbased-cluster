package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        long[] serviceWeight = new long[invokers.size()];
        long totalWeight = 0;
        long weight;
        for (int index = 0, size = invokers.size(); index < size; ++index) {
            Invoker<T> invoker = invokers.get(index);
            NodeState state = NodeManager.state(invoker);
            weight = state.getWeight();
            serviceWeight[index] = weight;
            totalWeight += weight;
        }
        long expect = ThreadLocalRandom.current().nextLong(totalWeight);
        for (int i = 0, size = invokers.size(); i < size; ++i) {
            expect -= serviceWeight[i];
            if (expect < 0) {
                return invokers.get(i);
            }
        }
        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }
}
