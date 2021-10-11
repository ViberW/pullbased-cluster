package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

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
        RpcContext.getClientAttachment().setObjectAttachment(CommonConstants.TIMEOUT_KEY,
                NodeManager.state(invoker).timeout);
        invocation.setObjectAttachment(BEGIN, System.nanoTime());
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        long duration = System.nanoTime() - (long) invocation.getObjectAttachment(BEGIN);
        NodeState state = NodeManager.state(invoker);
        Object value = appResponse.getObjectAttachment("w");
        if (null != value) {
            state.setServerActive((Integer) value);
        }
        value = appResponse.getObjectAttachment("d");
        state.end(null != value ? Math.max(0, duration - (long) value) : state.timeout + 1);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
    }
}
