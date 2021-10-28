package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionException;

/**
 * 客户端过滤器（选址后）
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.CONSUMER)
public class TestClientFilter implements Filter, BaseFilter.Listener {
    private final static Logger logger = LoggerFactory.getLogger(TestClientFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        NodeState state = NodeManager.state(invoker);
        RpcContext.getClientAttachment().setAttachment(CommonConstants.TIMEOUT_KEY,
                state.getTimeout() * (int) invocation.getObjectAttachment(RPCCode.TIME_RATIO, 1));
//        invocation.setObjectAttachment(RPCCode.BEGIN, System.currentTimeMillis());
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        NodeState state = NodeManager.state(invoker);
        Object value = appResponse.getObjectAttachment("w");
        state.setWeight((Integer) value);
//        value = appResponse.getObjectAttachment("d");
//        long duration = System.nanoTime() - (long) invocation.getObjectAttachment(RPCCode.BEGIN);
//        state.end(null != value ? Math.max(0, duration - (long) value) : state.timeout);
        value = appResponse.getObjectAttachment("e");
        state.setExecuteTime((Integer) value);
        state.end(false);
        state.alive = true;
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        if (t instanceof CompletionException) {
            t = t.getCause();
        }
        NodeState state = NodeManager.state(invoker);
        state.end(t instanceof TimeoutException);
        if (t instanceof RpcException && ((RpcException) t).isNetwork()) {
            state.alive = false;
        }
    }
}
