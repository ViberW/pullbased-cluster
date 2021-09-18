package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.rpc.*;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;

/**
 * 客户端过滤器（选址后）
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.CONSUMER)
public class TestClientFilter implements Filter, BaseFilter.Listener {


    private static final String CLIENT_MONITOR_START = "client_monitor_start";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        //已经明确发往一个provider, 此时的拦截
        NodeState state = NodeManager.state(invoker);
        state.active.incrementAndGet();
        invocation.put(CLIENT_MONITOR_START, System.currentTimeMillis());
        Result result = invoker.invoke(invocation);
        if (result instanceof AsyncRpcResult) {
            AsyncRpcResult asyncRpcResult = (AsyncRpcResult) result;
            asyncRpcResult.getResponseFuture().whenComplete((appResponse, throwable) -> {
                if (throwable != null) {
                    if (throwable instanceof TimeoutException) {
                        int timeout = invoker.getUrl().getPositiveParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
                        state.addTimeout(timeout, timeout);
                    } else {
                        state.setWeight(NodeState.DEFAULT_WEIGHT);
                    }
                }
            });
        }
        return result;
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        NodeState state = NodeManager.state(invoker);
        state.active.decrementAndGet();
        String value = appResponse.getAttachment("weight");
        if (null != value) {
            state.setWeight(Long.parseLong(value));
        }
        long clientTimeout = System.currentTimeMillis() - (long) invocation.get(CLIENT_MONITOR_START);
        value = appResponse.getAttachment("server_timeout");
        if (null != value) {
            state.addTimeout(Long.parseLong(value), clientTimeout);
        } else {
            state.addTimeout(clientTimeout, clientTimeout);
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}
