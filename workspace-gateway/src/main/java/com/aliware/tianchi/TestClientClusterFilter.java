package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.filter.ClusterFilter;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 客户端过滤器（选址前）
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.CONSUMER)
public class TestClientClusterFilter implements ClusterFilter, BaseFilter.Listener {
    private final static Logger logger = LoggerFactory.getLogger(TestClientClusterFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        long concurrent = NodeManager.active.getAndIncrement();
        if (NodeManager.balance) {
            long w = NodeManager.fullWeight.value;
            double r = ThreadLocalRandom.current().nextDouble(1);
            if (r > 1.8 - (concurrent * 1.0 / w)) {
                CompletableFuture<AppResponse> future = new CompletableFuture<>();
                future.completeExceptionally(new RpcException(RPCCode.FAST_INTERRUPTED,
                        "fast failure by consumer to invoke method "
                                + invocation.getMethodName() + " in provider " + invoker.getUrl()));
                AsyncRpcResult rpcResult = new AsyncRpcResult(future, invocation);
                RpcContext.getServiceContext().setFuture(new FutureAdapter<>(future));
                return rpcResult;
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
        NodeManager.active.getAndDecrement();
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        NodeManager.active.getAndDecrement();
        if (t instanceof CompletionException) {
            t = ((CompletionException) t).getCause();
        }
        if (t instanceof RpcException) {
            if (((RpcException) t).getCode() == RPCCode.FAST_INTERRUPTED) {
                logger.info("TestClientClusterFilter-1:");
            } else if (((RpcException) t).getCode() == RPCCode.FAST_FAIL) {
                logger.info("TestClientClusterFilter-2:");
            }
        }
    }
}
