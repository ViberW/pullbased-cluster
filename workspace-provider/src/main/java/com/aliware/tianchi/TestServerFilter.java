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
        int w = ProviderManager.weight.value;
        int concurrent = ProviderManager.active.getAndIncrement();
        if (concurrent > w) {
            /*double r = ThreadLocalRandom.current().nextDouble(1);
            if (r > 1.25 - (concurrent * 1.0 / w)) {*/
                throw new RpcException(RPCCode.FAST_FAIL,
                        "fast failure by provider to invoke method "
                                + invocation.getMethodName() + " in provider " + invoker.getUrl());
            /*}*/
        }
        invocation.put(ACTIVE, concurrent);
        invocation.put(BEGIN, System.nanoTime());
        try {
            return invoker.invoke(invocation);
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        ProviderManager.active.getAndDecrement();
        long duration = System.nanoTime() - (long) invocation.get(BEGIN);
        ProviderManager.time(duration, (int) invocation.get(ACTIVE));
        appResponse.setObjectAttachment("w", ProviderManager.weight.value);
        appResponse.setObjectAttachment("e", ProviderManager.executeTime.value);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        ProviderManager.active.getAndDecrement();
    }
}
