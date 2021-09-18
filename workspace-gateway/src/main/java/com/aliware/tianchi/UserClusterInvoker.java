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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 集群实现
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的集群调度算法
 */
public class UserClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private final Timer checker;

    public UserClusterInvoker(Directory<T> directory) {
        super(directory);
        checker = new HashedWheelTimer(
                new NamedThreadFactory("user-cluster-check-timer", true),
                20, TimeUnit.MILLISECONDS, 25);
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        Invoker<T> invoker = this.select(loadbalance, invocation, invokers, null);
        Result result = invoker.invoke(invocation);
        if (invokers.size() > 1 && result instanceof AsyncRpcResult) {
            //这里去替换CompletableFuture
            AsyncRpcResult asyncRpcResult = (AsyncRpcResult) result;
            CompletableFuture<AppResponse> responseFuture = asyncRpcResult.getResponseFuture();
            if (!responseFuture.isDone()) {
                CompletableFuture<AppResponse> reputCompletableFuture = new OnceCompletableFuture<>(responseFuture);
                asyncRpcResult.setResponseFuture(reputCompletableFuture);
                if (responseFuture.isDone()) {
                    reputCompletableFuture.complete(responseFuture.getNow(new AppResponse()));
                } else {
                    checker.newTimeout(new FutureTimeoutTask(loadbalance, invocation, asyncRpcResult, invoker, invokers),
                            50, TimeUnit.MILLISECONDS);
                }
            }
        }
        return result;
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

        public FutureTimeoutTask(LoadBalance loadbalance, Invocation invocation, AsyncRpcResult asyncRpcResult, Invoker<T> invoker, List<Invoker<T>> invokers) {
            this.asyncRpcResult = asyncRpcResult;
            this.invoker = invoker;
            this.invokers = invokers;
            this.loadbalance = loadbalance;
            this.invocation = invocation;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            CompletableFuture<AppResponse> responseFuture = asyncRpcResult.getResponseFuture();
            if (responseFuture == null || responseFuture.isDone()) {
                return;
            }
            responseFuture.cancel(true);
            NodeState state = NodeManager.state(invoker);
            state.addTimeout(50, 50);
            invoker = select(loadbalance, invocation, invokers, Collections.singletonList(invoker));
            invokeWithContext(invoker, invocation);
            rePut(timeout);
        }

        private void rePut(Timeout timeout) {
            if (timeout == null) {
                return;
            }
            Timer timer = timeout.timer();
            if (timer.isStop() || timeout.isCancelled()) {
                return;
            }
            timer.newTimeout(timeout.task(), 50, TimeUnit.MILLISECONDS);
        }
    }

    static class OnceCompletableFuture<T> extends CompletableFuture<T> {
        AtomicBoolean once = new AtomicBoolean(false);
        final CompletableFuture<T> responseFuture;

        public OnceCompletableFuture(CompletableFuture<T> responseFuture) {
            this.responseFuture = responseFuture;
            CompletableFuture<T> reputCompletableFuture = new CompletableFuture<>();
            reputCompletableFuture.whenComplete((appResponse, throwable) -> {
                if (!reputCompletableFuture.isCancelled()) {
                    if (once.compareAndSet(false, true)) {
                        responseFuture.complete(appResponse);
                    }
                }
            });
        }
    }
}
