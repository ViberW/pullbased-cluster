package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.ThreadLocalRandom;

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
        int maybeLimit = 100;
        RpcContext.getServerAttachment().setObjectAttachment(BEGIN, begin);
        long concurrent = ProviderManager.active.getAndIncrement();
        try {
            if (concurrent > maybeLimit) {
                double r = ThreadLocalRandom.current().nextDouble(1);
                if (r > ProviderManager.okRatio) {
                    throw new RpcException(RpcException.LIMIT_EXCEEDED_EXCEPTION,
                            "Failed to invoke method " + invocation.getMethodName() + " in provider " +
                                    invoker.getUrl() + ", cause: The service using threads greater" +
                                    " than <dubbo:service executes=\"" + maybeLimit + "\" /> limited.");
                }
            }
            return invoker.invoke(invocation);
        } catch (Exception e) {
            throw e;
        } finally {
            ProviderManager.active.getAndDecrement();
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        ProviderManager.maybeInit(invoker);
        long duration = System.nanoTime() - (long) RpcContext.getServerAttachment().getObjectAttachment(BEGIN);
        ProviderManager.time(duration);
        appResponse.setObjectAttachment("w", ProviderManager.weight);
        appResponse.setObjectAttachment("d", duration);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        t.printStackTrace();
    }
}
