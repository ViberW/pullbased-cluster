package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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
        //查看超时时间是否超过阈值, 快速失败
//        long concurrent = ProviderManager.active.getAndIncrement();
        //达到服务端的最高水位的上限;
//        long w = ProviderManager.weight;
        int concurrent = ProviderManager.capacity - ProviderManager.actualWeight.availablePermits();
        try {
            boolean b = ProviderManager.actualWeight.tryAcquire(
                    (long) invocation.getObjectAttachment(CommonConstants.TIMEOUT_KEY) / 2
                    , TimeUnit.MILLISECONDS);
            if (!b) {
                throw new RpcException(RPCCode.FAST_FAIL,
                        "fast failure by provider to invoke method "
                                + invocation.getMethodName() + " in provider " + invoker.getUrl());
            }
        } catch (InterruptedException e) {
            throw new RpcException(RPCCode.FAST_INTERRUPTED,
                    "fast interrupted by provider to invoke method "
                            + invocation.getMethodName() + " in provider " + invoker.getUrl());
        }
        /*if (concurrent > w) { //? 这里是否可以尝试使用等待呢
            double r = ThreadLocalRandom.current().nextDouble(1);
            if (r > 1.5 - (concurrent * 1.0 / w)) { //提高容忍度
                throw new RpcException(RPCCode.FAST_FAIL,
                        "fast failure by provider to invoke method "
                                + invocation.getMethodName() + " in provider " + invoker.getUrl());
            }
        }*/
        invocation.put(ACTIVE, Math.max(0, concurrent));
        invocation.put(BEGIN, System.nanoTime());
        try {
            return invoker.invoke(invocation);
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        ProviderManager.maybeInit(invoker);
//        ProviderManager.active.getAndDecrement();
        ProviderManager.actualWeight.release();
        long duration = System.nanoTime() - (long) invocation.get(BEGIN);
        ProviderManager.time(duration, (long) invocation.get(ACTIVE));
        appResponse.setObjectAttachment("w", ProviderManager.weight);
//        appResponse.setObjectAttachment("d", duration);
        appResponse.setObjectAttachment("e", ProviderManager.executeTime);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
//        ProviderManager.active.getAndDecrement();
        ProviderManager.actualWeight.release();
    }
}
