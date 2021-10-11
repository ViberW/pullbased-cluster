package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

/**
 * 服务端过滤器
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {

    private static final String BEGIN = "_provider_begin";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        long begin = System.nanoTime();
        RpcContext.getServerAttachment().setObjectAttachment(BEGIN, begin);
        try {
            return invoker.invoke(invocation);
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        long duration = System.nanoTime() - (long) RpcContext.getServerAttachment().getObjectAttachment(BEGIN);
        RpcContext.getServerAttachment().getObjectAttachment("d");
        ProviderManager.time(duration);
        ProviderManager.maybeInit(invoker);
        appResponse.setObjectAttachment("w", ProviderManager.responseTime);
        appResponse.setObjectAttachment("d", duration);

    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
    }
}
