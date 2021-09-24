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


    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        NodeManager.state(invoker).clientActive.getAndIncrement();
        try {
            return invoker.invoke(invocation);
        } finally {
            NodeManager.state(invoker).clientActive.getAndDecrement();
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        NodeState state = NodeManager.state(invoker);
        String value = appResponse.getAttachment("w");
        if (null != value) {
            state.setServerActive(Long.parseLong(value));
        }
        value = appResponse.getAttachment("c");
        if (null != value) {
            state.setCnt(Integer.parseInt(value));
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}
