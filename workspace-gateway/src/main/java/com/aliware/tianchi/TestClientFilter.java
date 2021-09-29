package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
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

    private static final String TIME = "time_duration";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        NodeManager.state(invoker).active.getAndIncrement();
        invocation.setObjectAttachment(TIME, System.nanoTime());
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        NodeState state = NodeManager.state(invoker);
        String value = appResponse.getAttachment("w");
        if (null != value) {
            state.setServerActive(Long.parseLong(value));
        }
        value = appResponse.getAttachment("t");
        if (null != value) {
            //网络延迟
            long delay = Math.max(0, System.nanoTime() - (long) invocation.getObjectAttachment(TIME)
                    - Long.parseLong(value));
            NodeManager.state(invoker).delay(delay);
        }
        //仅仅记录超时的 -- 乘以weight
        NodeManager.state(invoker).active.getAndDecrement();
        if (appResponse.hasException()) {
            NodeManager.state(invoker).end(appResponse.getException() instanceof TimeoutException);
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
    }
}
