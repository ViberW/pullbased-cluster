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
        RpcContext.getClientAttachment().setAttachment(CommonConstants.TIMEOUT_KEY,
                NodeManager.state(invoker).getTimeout()
                        * (int) invocation.getObjectAttachment(RPCCode.TIME_RATIO, 1));
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        NodeState state = NodeManager.state(invoker);
        state.setWeight((Integer) appResponse.getObjectAttachment("w"));
        state.setExecuteTime((Integer) appResponse.getObjectAttachment("e"));
        state.end(false);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        if (t instanceof CompletionException) {
            t = t.getCause();
        }
        NodeManager.state(invoker).end(t instanceof TimeoutException);
    }
}
