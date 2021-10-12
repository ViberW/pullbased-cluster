package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
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
        Object flag = RpcContext.getClientAttachment().getObjectAttachment(NodeManager.DOUBLE_FLAG);
        RpcContext.getClientAttachment().setAttachment(CommonConstants.TIMEOUT_KEY,
                (flag != null && (boolean) flag ? 2 : 1) * NodeManager.state(invoker).timeout);
        invocation.setObjectAttachment(BEGIN, System.nanoTime());
        NodeManager.state(invoker).active.getAndIncrement();
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        long duration = System.nanoTime() - (long) invocation.getObjectAttachment(BEGIN);
        NodeState state = NodeManager.state(invoker);
        state.active.getAndIncrement();
        Object value = appResponse.getObjectAttachment("w");
        if (null != value) {
            state.setWeight((Integer) value);
        }
        value = appResponse.getObjectAttachment("d");
        state.end(null != value ? Math.max(0, duration - (long) value) : state.timeout,
                appResponse.hasException() && appResponse.getException() instanceof TimeoutException);
        if (appResponse.hasException() && appResponse.getException() instanceof RemotingException) {
            state.setWeight(1);//若是网络问题, 则将比重降低为1
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        NodeManager.state(invoker).active.getAndIncrement();
    }
}
