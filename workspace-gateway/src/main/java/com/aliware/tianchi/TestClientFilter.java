package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.CompletionException;
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
        RpcContext.getClientAttachment().setAttachment(CommonConstants.TIMEOUT_KEY, NodeManager.state(invoker).getTimeout());
        invocation.setObjectAttachment(BEGIN, System.nanoTime());
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        NodeState state = NodeManager.state(invoker);
        Object value = appResponse.getObjectAttachment("w");
        if (null != value) {
            state.setWeight((Integer) value);
        }
        value = appResponse.getObjectAttachment("d");
        long duration = System.nanoTime() - (long) invocation.getObjectAttachment(BEGIN);
        state.end(null != value ? Math.max(0, duration - (long) value) /*duration*/ : state.timeout);
        value = appResponse.getObjectAttachment("e");
        if (null != value) {
            state.setExecuteTime((Integer)value);
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        if (t instanceof CompletionException) {
            t = ((CompletionException) t).getCause();
        }
        if (t instanceof TimeoutException) {
            NodeState state = NodeManager.state(invoker);
            state.end(state.timeout);
        }
    }
}
