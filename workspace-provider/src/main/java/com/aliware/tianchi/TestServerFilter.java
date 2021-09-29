package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务端过滤器
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        long begin = System.nanoTime();
        try {
            return invoker.invoke(invocation);
        } catch (Exception e) {
            throw e;
        } finally {
            int count = ProviderManager.active.getAndDecrement();
            ProviderManager.time(System.nanoTime() - begin, count);
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        // 获取内存信息样例 --这些信息仅仅在访问时间较长时触发计算, 没必要每次都计算
        ProviderManager.maybeInit(invoker);
        appResponse.setAttachment("w", ProviderManager.weight);
        appResponse.setAttachment("c", ProviderManager.cm);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
    }
}
