package com.aliware.tianchi;

import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;

/**
 * 集群实现
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的集群调度算法
 */
public class UserClusterInvoker<T> extends AbstractClusterInvoker<T> {
    private final static Logger logger = LoggerFactory.getLogger(UserClusterInvoker.class);

    public UserClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        //
        Invoker<T> invoker = this.select(loadbalance, invocation, invokers, null);
        Result result = doInvoked(invocation, invokers, loadbalance, invoker, false);
        if (result instanceof AsyncRpcResult) {
            WaitCompletableFuture future = new WaitCompletableFuture(loadbalance, invocation, invoker, invokers);
            future.register((AsyncRpcResult) result);
            AsyncRpcResult rpcResult = new AsyncRpcResult(future, invocation);
            RpcContext.getServiceContext().setFuture(new FutureAdapter<>(future));
            return rpcResult;
        }
        return result;
    }

    private Result doInvoked(Invocation invocation, List<Invoker<T>> invokers,
                             LoadBalance loadbalance, Invoker<T> invoker, boolean retry) {
        try {
            invocation.setObjectAttachment(RPCCode.TIME_RATIO, invokers.size());
            return invoker.invoke(invocation);
        } catch (RpcException e) {
            if (e.isNetwork()) {
                if (invokers.size() <= 1) {
                    throw e;
                }
                if (!retry) {
                    invokers = new ArrayList<>(invokers);
                }
                invokers.remove(invoker);
                invoker = this.select(loadbalance, invocation, invokers, null);
                return doInvoked(invocation, invokers, loadbalance, invoker, true);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void destroy() {
        super.destroy();
    }


    class WaitCompletableFuture extends CompletableFuture<AppResponse> {
        Invoker<T> invoker;
        List<Invoker<T>> invokers;
        LoadBalance loadbalance;
        Invocation invocation;
        final long time;
        long start;
        List<Invoker<T>> origin;


        public WaitCompletableFuture(LoadBalance loadbalance, Invocation invocation,
                                     Invoker<T> invoker, List<Invoker<T>> invokers) {
            this.invoker = invoker;
            this.origin = invokers;
            this.loadbalance = loadbalance;
            this.invocation = invocation;
            time = invoker.getUrl().getPositiveParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
            start = System.currentTimeMillis();
        }

        public void register(AsyncRpcResult result) {
            result.whenCompleteWithContext((appResponse, throwable) -> {
                if (WaitCompletableFuture.this.isDone()) {
                    return;
                }
                if ((null != appResponse && !appResponse.hasException())
                        || (invokers == null ? origin : invokers).size() <= 1) {
                    WaitCompletableFuture.this.complete(null == appResponse ? new AppResponse(new RpcException(RPCCode.FAST_FAIL,
                            "Invoke remote method fast failure. " + "provider: " + invocation.getInvoker().getUrl()))
                            : (AppResponse) appResponse);
                } else {
                    if (System.currentTimeMillis() - start > time) {
                        WaitCompletableFuture.this.complete(new AppResponse(new RpcException(RpcException.TIMEOUT_EXCEPTION,
                                "Invoke remote method timeout. method: " + invocation.getMethodName() + ", provider: " + getUrl())));
                        return;
                    }
                    if (invokers == null) {
                        invokers = new ArrayList<>(origin);
                    }
                    invokers.remove(invoker);
                    try {
                        invoker = select(loadbalance, invocation, invokers, null);
                        Result r = doInvoked(invocation, invokers, loadbalance, invoker, true);
                        register((AsyncRpcResult) r);
                    } catch (Exception e) {
                        WaitCompletableFuture.this.complete(new AppResponse(e));
                    }
                }
            });
        }
    }
}
