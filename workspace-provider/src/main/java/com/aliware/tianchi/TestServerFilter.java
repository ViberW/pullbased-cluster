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
    private static final String ACTIVE = "_provider_active";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        invocation.setObjectAttachment(BEGIN, System.nanoTime());
        long concurrent = ProviderManager.active.getAndIncrement();
        invocation.setObjectAttachment(ACTIVE, concurrent);
        //达到服务端的最高水位的上限;
        long w = ProviderManager.weight;
        if (concurrent > w) {
            double r = ThreadLocalRandom.current().nextDouble(1);
            if (r > 1.75 - (concurrent * 1.0 / w)) { //提高容忍度
                throw new RpcException(RPCCode.FAST_FAIL,
                        "fast failure to invoke method " + invocation.getMethodName() + " in provider " + invoker.getUrl());
            }
        }
        try {
            return invoker.invoke(invocation);
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        ProviderManager.maybeInit(invoker);
        ProviderManager.active.getAndDecrement();
        long duration = System.nanoTime() - (long) invocation.getObjectAttachment(BEGIN);
        ProviderManager.time(duration, (long) invocation.getObjectAttachment(ACTIVE));
        appResponse.setObjectAttachment("w", ProviderManager.weight);
        appResponse.setObjectAttachment("d", duration);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        ProviderManager.active.getAndDecrement();
    }
}
