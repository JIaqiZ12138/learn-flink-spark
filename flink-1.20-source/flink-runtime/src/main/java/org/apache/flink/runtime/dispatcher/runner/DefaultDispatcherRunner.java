/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Runner for the {@link org.apache.flink.runtime.dispatcher.Dispatcher} which is responsible for
 * the leader election.
 */
public final class DefaultDispatcherRunner implements DispatcherRunner, LeaderContender {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDispatcherRunner.class);

    private final Object lock = new Object();

    private final LeaderElection leaderElection;

    private final FatalErrorHandler fatalErrorHandler;

    private final DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory;

    private final CompletableFuture<Void> terminationFuture;

    private final CompletableFuture<ApplicationStatus> shutDownFuture;

    private boolean running;

    private DispatcherLeaderProcess dispatcherLeaderProcess;

    private CompletableFuture<Void> previousDispatcherLeaderProcessTerminationFuture;

    private DefaultDispatcherRunner(
            LeaderElection leaderElection,
            FatalErrorHandler fatalErrorHandler,
            DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory) {
        this.leaderElection = leaderElection;
        this.fatalErrorHandler = fatalErrorHandler;
        this.dispatcherLeaderProcessFactory = dispatcherLeaderProcessFactory;
        this.terminationFuture = new CompletableFuture<>();
        this.shutDownFuture = new CompletableFuture<>();

        this.running = true;
        this.dispatcherLeaderProcess = StoppedDispatcherLeaderProcess.INSTANCE;
        this.previousDispatcherLeaderProcessTerminationFuture =
                CompletableFuture.completedFuture(null);
    }

    /**
     * ★ 只是"登记参选"，不创建任何 Dispatcher。
     *
     * <p>{@code LeaderContender} 机制：本对象把自己交给 {@link LeaderElection}，
     * 选主成功后由 {@code LeaderElection} 回调 {@link #grantLeadership(UUID)}。
     * 所以调用方（{@code DefaultDispatcherResourceManagerComponentFactory.create()}）
     * 在这一行返回时，Dispatcher 其实还不存在。
     */
    void start() throws Exception {
        leaderElection.startLeaderElection(this);
    }

    @Override
    public CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return shutDownFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (!running) {
                return terminationFuture;
            } else {
                running = false;
            }
        }

        final CompletableFuture<Void> stopLeaderElectionServiceFuture = stopLeaderElectionService();

        stopDispatcherLeaderProcess();

        FutureUtils.forward(previousDispatcherLeaderProcessTerminationFuture, terminationFuture);

        return FutureUtils.completeAll(
                Arrays.asList(terminationFuture, stopLeaderElectionServiceFuture));
    }

    // ---------------------------------------------------------------
    // Leader election
    // ---------------------------------------------------------------

    /**
     * ★★ 选主成功回调 —— 真正创建 Dispatcher 的起点。
     *
     * <p>拿到领导权后先建一个"领导进程"（{@link DispatcherLeaderProcess}），
     * 再启动它；由它内部去创建并启动具体的 Dispatcher 实例。
     * 为什么要多这一层？因为进程的启动/停止要跟着领导权走：
     * 失去领导权 → 关掉旧的领导进程 → 拿到新领导权 → 换一个新的重建。
     * {@link #previousDispatcherLeaderProcessTerminationFuture} 保证了
     * "旧的完全停干净之后，新的才开始"。
     */
    @Override
    public void grantLeadership(UUID leaderSessionID) {
        runActionIfRunning(
                () -> {
                    LOG.info(
                            "{} was granted leadership with leader id {}. Creating new {}.",
                            getClass().getSimpleName(),
                            leaderSessionID,
                            DispatcherLeaderProcess.class.getSimpleName());
                    startNewDispatcherLeaderProcess(leaderSessionID);
                });
    }

    private void startNewDispatcherLeaderProcess(UUID leaderSessionID) {
        // 先停掉上一个（如果还在），保证同一时刻只有一个领导进程在跑
        stopDispatcherLeaderProcess();

        // 由工厂产出领导进程。per-job 模式下工厂是 JobDispatcherLeaderProcessFactory，
        // 它持有 jobGraphRetriever —— JobGraph 就是从这里开始往下传的。
        dispatcherLeaderProcess = createNewDispatcherLeaderProcess(leaderSessionID);

        final DispatcherLeaderProcess newDispatcherLeaderProcess = dispatcherLeaderProcess;
        // ★ 关键顺序：必须等旧的领导进程彻底终止，才启动新的。
        //   否则可能出现两个 Dispatcher 同时持有同一份作业状态。
        FutureUtils.assertNoException(
                previousDispatcherLeaderProcessTerminationFuture.thenRun(
                        newDispatcherLeaderProcess::start));
    }

    private void stopDispatcherLeaderProcess() {
        final CompletableFuture<Void> terminationFuture = dispatcherLeaderProcess.closeAsync();
        previousDispatcherLeaderProcessTerminationFuture =
                FutureUtils.completeAll(
                        Arrays.asList(
                                previousDispatcherLeaderProcessTerminationFuture,
                                terminationFuture));
    }

    private DispatcherLeaderProcess createNewDispatcherLeaderProcess(UUID leaderSessionID) {
        final DispatcherLeaderProcess newDispatcherLeaderProcess =
                dispatcherLeaderProcessFactory.create(leaderSessionID);

        forwardShutDownFuture(newDispatcherLeaderProcess);
        forwardConfirmLeaderSessionFuture(leaderSessionID, newDispatcherLeaderProcess);

        return newDispatcherLeaderProcess;
    }

    private void forwardShutDownFuture(DispatcherLeaderProcess newDispatcherLeaderProcess) {
        newDispatcherLeaderProcess
                .getShutDownFuture()
                .whenComplete(
                        (applicationStatus, throwable) -> {
                            synchronized (lock) {
                                // ignore if no longer running or if leader processes is no longer
                                // valid
                                if (running
                                        && this.dispatcherLeaderProcess
                                                == newDispatcherLeaderProcess) {
                                    if (throwable != null) {
                                        shutDownFuture.completeExceptionally(throwable);
                                    } else {
                                        shutDownFuture.complete(applicationStatus);
                                    }
                                }
                            }
                        });
    }

    private void forwardConfirmLeaderSessionFuture(
            UUID leaderSessionID, DispatcherLeaderProcess newDispatcherLeaderProcess) {
        FutureUtils.assertNoException(
                newDispatcherLeaderProcess
                        .getLeaderAddressFuture()
                        .thenCompose(
                                leaderAddress ->
                                        leaderElection.confirmLeadershipAsync(
                                                leaderSessionID, leaderAddress)));
    }

    @Override
    public void revokeLeadership() {
        runActionIfRunning(
                () -> {
                    LOG.info(
                            "{} was revoked the leadership with leader id {}. Stopping the {}.",
                            getClass().getSimpleName(),
                            dispatcherLeaderProcess.getLeaderSessionId(),
                            DispatcherLeaderProcess.class.getSimpleName());
                    this.stopDispatcherLeaderProcess();
                });
    }

    private CompletableFuture<Void> stopLeaderElectionService() {
        try {
            leaderElection.close();
            return FutureUtils.completedVoidFuture();
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    private void runActionIfRunning(Runnable runnable) {
        synchronized (lock) {
            if (running) {
                runnable.run();
            } else {
                LOG.debug(
                        "Ignoring action because {} has already been stopped.",
                        getClass().getSimpleName());
            }
        }
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(
                new FlinkException(
                        String.format(
                                "Exception during leader election of %s occurred.",
                                getClass().getSimpleName()),
                        exception));
    }

    public static DispatcherRunner create(
            LeaderElection leaderElection,
            FatalErrorHandler fatalErrorHandler,
            DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory)
            throws Exception {
        final DefaultDispatcherRunner dispatcherRunner =
                new DefaultDispatcherRunner(
                        leaderElection, fatalErrorHandler, dispatcherLeaderProcessFactory);
        dispatcherRunner.start();
        return dispatcherRunner;
    }
}
