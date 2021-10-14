package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 客户端过滤器（选址后）
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.CONSUMER)
public class TestClientFilter implements Filter, BaseFilter.Listener {

    private static final String BEGIN = "_time_begin";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        //这里也需要来个限流策略
        NodeState state = NodeManager.state(invoker);
        long concurrent = state.ACTIVE.getAndIncrement();
        long w = state.weight;
        if (concurrent > w) {
            double r = ThreadLocalRandom.current().nextDouble(1);
            if (r > 2 - (concurrent * 1.0 / w)) { //容忍度高些, 因为网络传输, weight不是及时的
                throw new RpcException(RPCCode.FAST_FAIL,
                        "fast failure by consumer to invoke method "
                                + invocation.getMethodName() + " in provider " + invoker.getUrl());
            }
        }

        RpcContext.getClientAttachment().setAttachment(CommonConstants.TIMEOUT_KEY, state.timeout);
        invocation.setObjectAttachment(BEGIN, System.nanoTime());
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        long duration = System.nanoTime() - (long) invocation.getObjectAttachment(BEGIN);
        NodeState state = NodeManager.state(invoker);
        state.ACTIVE.getAndDecrement();
        Object value = appResponse.getObjectAttachment("w");
        if (null != value) {
            state.setWeight((Integer) value);
        }
        value = appResponse.getObjectAttachment("d");
        state.end(null != value ? Math.max(0, duration - (long) value) : state.timeout);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        NodeManager.state(invoker).ACTIVE.getAndDecrement();
    }
}
