package com.aliware.tianchi;

import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

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
    private final Timer checker;
    private final ExecutorService executor;

    public UserClusterInvoker(Directory<T> directory) {
        super(directory);
        checker = new HashedWheelTimer(
                new NamedThreadFactory("user-cluster-check-timer", true),
                10, TimeUnit.MILLISECONDS, 8);
        executor = new ThreadPoolExecutor(16, 16,
                0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1024),
                new NamedInternalThreadFactory("user-cluster-executor", true),
                /*new ThreadPoolExecutor.CallerRunsPolicy()*/new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                logger.info("UserClusterInvoker executor is full");
                if (!executor.isShutdown()) {
                    r.run();
                }
            }
        });
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        //
        Invoker<T> invoker = this.select(loadbalance, invocation, invokers, null);
        Result result = doInvoked(invocation, invokers, loadbalance, invoker, false);
        if (result instanceof AsyncRpcResult) {
            WaitCompletableFuture future = new WaitCompletableFuture();
            AsyncRpcResult rpcResult = new AsyncRpcResult(future, invocation);
            RpcContext.getServiceContext().setFuture(new FutureAdapter<>(future));

            //校验
            future.register((AsyncRpcResult) result, checker.newTimeout(
                    new FutureTimeoutTask(loadbalance, invocation, future, invoker, invokers),
                    NodeManager.state(invoker).getTimeout(), TimeUnit.MILLISECONDS));
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
        checker.stop();
        executor.shutdown();
    }

    class FutureTimeoutTask implements TimerTask {
        Invoker<T> invoker;
        List<Invoker<T>> invokers;
        LoadBalance loadbalance;
        Invocation invocation;
        final long time;
        WaitCompletableFuture waitCompletableFuture;
        long start;
        List<Invoker<T>> origin;
        RpcContextAttachment tmpContext;
        RpcContextAttachment tmpServerContext;

        public FutureTimeoutTask(LoadBalance loadbalance, Invocation invocation,
                                 WaitCompletableFuture waitCompletableFuture, Invoker<T> invoker, List<Invoker<T>> invokers) {
            this.waitCompletableFuture = waitCompletableFuture;
            this.invoker = invoker;
            this.origin = invokers;
            this.loadbalance = loadbalance;
            this.invocation = invocation;
            time = invoker.getUrl().getPositiveParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
            start = System.currentTimeMillis();
            tmpContext = RpcContext.getClientAttachment();
            tmpServerContext = RpcContext.getServerContext();
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            if (waitCompletableFuture.isDone()) {
                return;
            }
            if (System.currentTimeMillis() - start > time) {
                waitCompletableFuture.complete(new AppResponse(new RpcException(RpcException.TIMEOUT_EXCEPTION,
                        "Invoke remote method timeout. method: " + invocation.getMethodName() + ", provider: " + getUrl())));
                return;
            }
            if (this.invokers == null) {
                this.invokers = new ArrayList<>(origin);
            }
            if (this.invokers.size() == 1) {
                waitCompletableFuture.complete(new AppResponse(new RpcException(RPCCode.FAST_FAIL,
                        "Invoke remote method fast failure. " + "provider: " + invocation.getInvoker().getUrl())));
                return;
            }
            invokers.remove(invoker);
            //将任务交给Executors去执行呢? 尽量保证timer的高校
            executor.execute(() -> {
                RpcContext.restoreContext(tmpContext);
                RpcContext.restoreServerContext(tmpServerContext);
                try {
                    invoker = select(loadbalance, invocation, invokers, null);
                    Result r = doInvoked(invocation, invokers, loadbalance, invoker, true);
                    waitCompletableFuture.register((AsyncRpcResult) r, timeout.timer().newTimeout(timeout.task(),
                            NodeManager.state(invoker).getTimeout(), TimeUnit.MILLISECONDS));
                } catch (Exception e) {
                    waitCompletableFuture.complete(new AppResponse(e));
                } finally {
                    RpcContext.removeContext();
                }
            });
        }
    }

    static class WaitCompletableFuture extends CompletableFuture<AppResponse> {
        long start = System.currentTimeMillis();

        public void register(AsyncRpcResult result, Timeout timeout) {
            //不需要context,因为在task中已经保存了context上下文了
            result.getResponseFuture().whenComplete((appResponse, throwable) -> {
                if (WaitCompletableFuture.this.isDone()) {
                    return;
                }
                if (throwable != null) {
                    if (throwable instanceof CompletionException) {
                        throwable = ((CompletionException) throwable).getCause();
                    }
                    if (throwable instanceof TimeoutException) {
                        logger.info("WaitCompletableFuture:{}#{}",
                                ((TimeoutException) throwable).isClientSide(), System.currentTimeMillis() - start);
                    }
                }
                start = System.currentTimeMillis();
                if ((null != appResponse && !appResponse.hasException())) {
                    timeout.cancel();
                    WaitCompletableFuture.this.complete(appResponse);
                } else if (timeout.cancel()) {
                    try {
                        //手动执行.
                        timeout.task().run(timeout);
                    } catch (Throwable t) {
                        logger.warn("An exception was thrown by " + timeout.task().getClass().getSimpleName(), t);
                        WaitCompletableFuture.this.complete(new AppResponse(t));
                    }
                }
            });
        }
    }
}
