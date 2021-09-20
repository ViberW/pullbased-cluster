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

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;

/**
 * 集群实现
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的集群调度算法
 */
public class UserClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private final Timer checker;
    private static final long delay = 50;
    private static AppResponse EMPTY = new AppResponse();

    public UserClusterInvoker(Directory<T> directory) {
        super(directory);
        checker = new HashedWheelTimer(
                new NamedThreadFactory("user-cluster-check-timer", true),
                25, TimeUnit.MILLISECONDS, 40);
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        Invoker<T> invoker = this.select(loadbalance, invocation, invokers, null);
        Result result = invoker.invoke(invocation);
        if (result instanceof AsyncRpcResult) {
            //这里去替换CompletableFuture
            AsyncRpcResult asyncRpcResult = (AsyncRpcResult) result;
            CompletableFuture<AppResponse> responseFuture = asyncRpcResult.getResponseFuture();
            if (!responseFuture.isDone()) {
                CompletableFuture<AppResponse> reputCompletableFuture = new OnceCompletableFuture(responseFuture);
                asyncRpcResult.setResponseFuture(reputCompletableFuture);
                if (responseFuture.isDone()) {
                    reputCompletableFuture.complete(responseFuture.getNow(EMPTY));
                } else {
                    checker.newTimeout(new FutureTimeoutTask(loadbalance, invocation, asyncRpcResult, invoker, invokers),
                            delay, TimeUnit.MILLISECONDS);
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
        final long time;

        public FutureTimeoutTask(LoadBalance loadbalance, Invocation invocation, AsyncRpcResult asyncRpcResult,
                                 Invoker<T> invoker, List<Invoker<T>> invokers) {
            this.asyncRpcResult = asyncRpcResult;
            this.invoker = invoker;
            this.invokers = invokers;
            this.loadbalance = loadbalance;
            this.invocation = invocation;
            time = invoker.getUrl().getPositiveParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            CompletableFuture<AppResponse> responseFuture = asyncRpcResult.getResponseFuture();
            if (responseFuture == null || responseFuture.isDone()) {
                return;
            }
            NodeState state = NodeManager.state(invoker);
            state.addTimeout(time);
            invoker = select(loadbalance, invocation, invokers, Collections.singletonList(invoker));
            Result result = invoker.invoke(invocation);
            //同样将结果放置到这里
            OnceCompletableFuture oncefuture = (OnceCompletableFuture) responseFuture;
            if (oncefuture.replace(((AsyncRpcResult) result).getResponseFuture())) {
                timeout.timer().newTimeout(timeout.task(), delay, TimeUnit.MILLISECONDS);
            }
        }
    }

    static class OnceCompletableFuture extends CompletableFuture<AppResponse> {
        CompletableFuture<AppResponse> responseFuture;

        public OnceCompletableFuture(CompletableFuture<AppResponse> responseFuture) {
            this.responseFuture = responseFuture;
        }

        public boolean replace(CompletableFuture<AppResponse> responseFuture) {
            if(this.isDone()){
                return false;
            }
            CompletableFuture<AppResponse> lastFuture = this.responseFuture;
            this.responseFuture = responseFuture;

            if (!lastFuture.cancel(true)) {
                this.complete(lastFuture.getNow(EMPTY));
                this.responseFuture.cancel(true);
                return false;
            }
            if (this.responseFuture.isDone()) {
                this.complete(this.responseFuture.getNow(EMPTY));
            } else {
                return true;
            }
            return false;
        }
    }
}
