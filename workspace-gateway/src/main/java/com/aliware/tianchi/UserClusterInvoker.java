package com.aliware.tianchi;

import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 集群实现
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的集群调度算法
 */
public class UserClusterInvoker<T> extends AbstractClusterInvoker<T> {
    private final static Logger logger = LoggerFactory.getLogger(UserClusterInvoker.class);
    private final Timer checker;
    private static final long delay = 40;

    public UserClusterInvoker(Directory<T> directory) {
        super(directory);
        checker = new HashedWheelTimer(
                new NamedThreadFactory("user-cluster-check-timer", true),
                20, TimeUnit.MILLISECONDS, 40);
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        //
        Invoker<T> invoker = this.select(loadbalance, invocation, invokers, null);
        Result result = doInvoked(invocation, invokers, loadbalance, invoker);
        if (result instanceof AsyncRpcResult) {
            OnceCompletableFuture onceCompletableFuture = new OnceCompletableFuture(((AsyncRpcResult) result).getResponseFuture());
            AsyncRpcResult rpcResult = new AsyncRpcResult(onceCompletableFuture, invocation);
            RpcContext.getServiceContext().setFuture(new FutureAdapter<>(onceCompletableFuture));
            onceCompletableFuture.timeout = checker.newTimeout(new FutureTimeoutTask(loadbalance, invocation, rpcResult, onceCompletableFuture, invoker, invokers),
                    delay, TimeUnit.MILLISECONDS);
            return rpcResult;
        }
        return result;
    }

    private Result doInvoked(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance, Invoker<T> invoker) {
        try {
            return invoker.invoke(invocation);
        } catch (RpcException e) {
            if (e.isNetwork()) {
                NodeManager.state(invoker).end(true);
                if (invokers.size() <= 1) {
                    throw e;
                }
                ArrayList<Invoker<T>> newInvokers = new ArrayList<>(invokers);
                newInvokers.remove(invoker);
                invoker = this.select(loadbalance, invocation, newInvokers, null);
                return doInvoked(invocation, newInvokers, loadbalance, invoker);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        checker.stop();
    }

    class FutureTimeoutTask implements TimerTask {
        Invoker<T> invoker;
        List<Invoker<T>> invokers;
        AsyncRpcResult asyncRpcResult;
        LoadBalance loadbalance;
        Invocation invocation;
        final long time;
        OnceCompletableFuture onceCompletableFuture;
        long start;

        public FutureTimeoutTask(LoadBalance loadbalance, Invocation invocation, AsyncRpcResult asyncRpcResult,
                                 OnceCompletableFuture onceCompletableFuture, Invoker<T> invoker, List<Invoker<T>> invokers) {
            this.asyncRpcResult = asyncRpcResult;
            this.onceCompletableFuture = onceCompletableFuture;
            this.invoker = invoker;
            this.invokers = invokers;
            this.loadbalance = loadbalance;
            this.invocation = invocation;
            time = invoker.getUrl().getPositiveParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
            start = System.currentTimeMillis();
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            if (onceCompletableFuture.isDone()) {
                return;
            }
            if (System.currentTimeMillis() - start > time) {
                onceCompletableFuture.complete(new AppResponse(new RpcException(RpcException.TIMEOUT_EXCEPTION, "Invoke remote method timeout. method: " +
                        invocation.getMethodName() + ", provider: " + getUrl())));
                return;
            }
            ArrayList<Invoker<T>> newInvokers = new ArrayList<>(this.invokers);
//            newInvokers.remove(invoker);
            invoker = select(loadbalance, invocation, newInvokers, null);
            Result result = doInvoked(invocation, newInvokers, loadbalance, invoker);
            //同样将结果放置到这里
            if (onceCompletableFuture.replace(((AsyncRpcResult) result).getResponseFuture())) {
                onceCompletableFuture.timeout = timeout.timer().newTimeout(timeout.task(), delay, TimeUnit.MILLISECONDS);
            }
        }
    }

    static class OnceCompletableFuture extends CompletableFuture<AppResponse> {
        CompletableFuture<AppResponse> responseFuture;
        Timeout timeout;

        public OnceCompletableFuture(CompletableFuture<AppResponse> responseFuture) {
            register(responseFuture);
        }

        private void register(CompletableFuture<AppResponse> responseFuture) {
            /*if (null != this.responseFuture && !this.responseFuture.isDone()) {
                this.responseFuture.cancel(true);
            }*/
            this.responseFuture = responseFuture;
            this.responseFuture.whenComplete((appResponse, throwable) -> {
                if (null != appResponse && !appResponse.hasException()) {
                    OnceCompletableFuture.this.complete(appResponse);
                    if (null != timeout && timeout.isExpired()) {
                        timeout.cancel();
                    }
                }
            });
        }

        public boolean replace(CompletableFuture<AppResponse> responseFuture) {
            register(responseFuture);
            return !this.isDone();
        }
    }
}
