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

    private static final String OFFSET = "_time_offset";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        long begin = System.nanoTime();
        long offset = ProviderManager.offset();
        try {
            return invoker.invoke(invocation);
        } catch (Exception e) {
            throw e;
        } finally {
            ProviderManager.time(offset, System.nanoTime() - begin);
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        // 获取内存信息样例 --这些信息仅仅在访问时间较长时触发计算, 没必要每次都计算
        ProviderManager.maybeInit(invoker);
        appResponse.setAttachment("w", ProviderManager.weight);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
    }
}
