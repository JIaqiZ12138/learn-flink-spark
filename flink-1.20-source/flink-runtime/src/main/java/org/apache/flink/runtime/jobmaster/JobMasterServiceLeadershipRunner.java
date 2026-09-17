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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.JobCancellationFailedException;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * Leadership runner for the {@link JobMasterServiceProcess}.
 *
 * <p>The responsibility of this component is to manage the leadership of the {@link
 * JobMasterServiceProcess}. This means that the runner will create an instance of the process when
 * it obtains the leadership. The process is stopped once the leadership is revoked.
 *
 * <p>This component only accepts signals (job result completion, initialization failure) as long as
 * it is running and as long as the signals are coming from the current leader process. This ensures
 * that only the current leader can affect this component.
 *
 * <p>All leadership operations are serialized. This means that granting the leadership has to
 * complete before the leadership can be revoked and vice versa.
 *
 * <p>The {@link #resultFuture} can be completed with the following values: * *
 *
 * <ul>
 *   <li>{@link JobManagerRunnerResult} to signal an initialization failure of the {@link
 *       JobMasterService} or the completion of a job
 *   <li>{@link Exception} to signal an unexpected failure
 * </ul>
 */
// 【1.20 结构变化】JobManagerRunner 的唯一实现，也是 Dispatcher 与 JobMaster 之间的那一层。
// 老版本里 Dispatcher 直接持有 JobManagerRunnerImpl（内部即 JobMaster），1.20 改成：
//   Dispatcher.createJobMasterRunner() → 本类 → DefaultJobMasterServiceProcess → JobMaster
// ★ 为什么 Dispatcher 已经选过主，这里还要为单个 job 再选一次主？
//   1) Dispatcher 选主只说明“这个 Dispatcher 是主”，不代表某个 job 的 JobMaster 一定建得起来；
//      一个主 Dispatcher 要同时跑成百上千个 job，JobMaster 的创建（内含执行图构建）可能失败，
//      必须能按 job 独立上报与重试，不能因为某个 job 建不起来就拖垮整个 Dispatcher。
//   2) JobMaster 创建是【异步】且会抛异常的，需要一个 Future 化的可重试包装层；本类还顺便把最终
//      结果收敛成 JobManagerRunnerResult 交给 Dispatcher.runJob()。本类同时是 LeaderContender。
public class JobMasterServiceLeadershipRunner implements JobManagerRunner, LeaderContender {

    private static final Logger LOG =
            LoggerFactory.getLogger(JobMasterServiceLeadershipRunner.class);

    private final Object lock = new Object();

    private final JobMasterServiceProcessFactory jobMasterServiceProcessFactory;

    private final LeaderElection leaderElection;

    private final JobResultStore jobResultStore;

    private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

    private final FatalErrorHandler fatalErrorHandler;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    // ★ 整条链路的收敛点：作业正常完成(JobResult)、初始化失败、意外异常最终都落到这个 future；
    //   Dispatcher.runJob() 只监听它一个。
    private final CompletableFuture<JobManagerRunnerResult> resultFuture =
            new CompletableFuture<>();

    @GuardedBy("lock")
    private State state = State.RUNNING;

    // ★ 选主相关操作全部串行化在这条 future 链上：grantLeadership 完成后才轮到 revokeLeadership
    //   （反之亦然），避免新旧两个 JobMasterServiceProcess 同时存在。
    @GuardedBy("lock")
    private CompletableFuture<Void> sequentialOperation = FutureUtils.completedVoidFuture();

    // 初始值是占位进程 WaitingForLeadership —— 此时还没选到主。
    // ★ 它不是 null，但任何方法都会返回失败 future，所以调用前必须先看 isInitializedAndRunning()。
    @GuardedBy("lock")
    private JobMasterServiceProcess jobMasterServiceProcess =
            JobMasterServiceProcess.waitingForLeadership();

    @GuardedBy("lock")
    private CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = new CompletableFuture<>();

    @GuardedBy("lock")
    private boolean hasCurrentLeaderBeenCancelled = false;

    public JobMasterServiceLeadershipRunner(
            JobMasterServiceProcessFactory jobMasterServiceProcessFactory,
            LeaderElection leaderElection,
            JobResultStore jobResultStore,
            LibraryCacheManager.ClassLoaderLease classLoaderLease,
            FatalErrorHandler fatalErrorHandler) {
        this.jobMasterServiceProcessFactory = jobMasterServiceProcessFactory;
        this.leaderElection = leaderElection;
        this.jobResultStore = jobResultStore;
        this.classLoaderLease = classLoaderLease;
        this.fatalErrorHandler = fatalErrorHandler;
    }

    // 【生命周期】Dispatcher 关闭这个 job 时调用（JobManagerRunner 继承自 AutoCloseableAsync）。
    // ★ 关闭是“主动放弃”语义：即使作业还在跑，结果也被定为 SUSPENDED，所以这里必须给
    //   resultFuture 一个确定值，否则 Dispatcher 会一直等下去。顺序：先关进程，再释放类加载器与选主。
    @Override
    public CompletableFuture<Void> closeAsync() {
        final CompletableFuture<Void> processTerminationFuture;
        synchronized (lock) {
            if (state == State.STOPPED) {
                return terminationFuture;
            }

            state = State.STOPPED;

            LOG.debug("Terminating the leadership runner for job {}.", getJobID());

            jobMasterGatewayFuture.completeExceptionally(
                    new FlinkException(
                            "JobMasterServiceLeadershipRunner is closed. Therefore, the corresponding JobMaster will never acquire the leadership."));

            resultFuture.complete(
                    JobManagerRunnerResult.forSuccess(
                            createExecutionGraphInfoWithJobStatus(JobStatus.SUSPENDED)));

            // ★ 关的是“当前进程”；若还没选到主，它就是 WaitingForLeadership，closeAsync 会立即返回。
            processTerminationFuture = jobMasterServiceProcess.closeAsync();
        }

        final CompletableFuture<Void> serviceTerminationFuture =
                FutureUtils.runAfterwards(
                        processTerminationFuture,
                        () -> {
                            classLoaderLease.release();
                            leaderElection.close();
                        });

        FutureUtils.forward(serviceTerminationFuture, terminationFuture);

        terminationFuture.whenComplete(
                (unused, throwable) ->
                        LOG.debug("Leadership runner for job {} has been terminated.", getJobID()));
        return terminationFuture;
    }

    // Dispatcher.runJob() 的第一步：本方法【不创建 JobMaster】，只向 LeaderElection 报名参选。
    // ★ 真正的 JobMaster 要等 grantLeadership 回调才出现，所以 start() 返回后 gateway future 可能还是空的。
    @Override
    public void start() throws Exception {
        LOG.debug("Start leadership runner for job {}.", getJobID());
        leaderElection.startLeaderElection(this);
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
        synchronized (lock) {
            return jobMasterGatewayFuture;
        }
    }

    // ★ Dispatcher 的唯一出口：runJob() 用 handleAsync 挂在这里，拿到 JobManagerRunnerResult
    //   （内含 JobResult 或初始化失败原因）后写 JobResultStore 并清理 job 状态。它只完成一次。
    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public JobID getJobID() {
        return jobMasterServiceProcessFactory.getJobId();
    }

    @Override
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
        synchronized (lock) {
            hasCurrentLeaderBeenCancelled = true;
            return getJobMasterGateway()
                    .thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout))
                    .exceptionally(
                            e -> {
                                throw new CompletionException(
                                        new JobCancellationFailedException(
                                                "Cancellation failed.",
                                                ExceptionUtils.stripCompletionException(e)));
                            });
        }
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                executionGraphInfo.getArchivedExecutionGraph().getState());
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                JobDetails.createDetailsForJob(
                                        executionGraphInfo.getArchivedExecutionGraph()));
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
        synchronized (lock) {
            // ★ 三种回答方式：gateway 已就绪就转问 JobMaster；还没建好就临时拼一张“稀疏执行图”返回状态；
            //   已结束则直接复用 resultFuture 里的结果（此时已经没有 JobMaster 可问了）。
            if (state == State.RUNNING) {
                if (jobMasterServiceProcess.isInitializedAndRunning()) {
                    return getJobMasterGateway()
                            .thenCompose(jobMasterGateway -> jobMasterGateway.requestJob(timeout));
                } else {
                    return CompletableFuture.completedFuture(
                            createExecutionGraphInfoWithJobStatus(
                                    hasCurrentLeaderBeenCancelled
                                            ? JobStatus.CANCELLING
                                            : JobStatus.INITIALIZING));
                }
            } else {
                return resultFuture.thenApply(JobManagerRunnerResult::getExecutionGraphInfo);
            }
        }
    }

    @Override
    public boolean isInitialized() {
        synchronized (lock) {
            return jobMasterServiceProcess.isInitializedAndRunning();
        }
    }

    // ★【重点】LeaderContender 回调：本 runner 当选该 job 的 leader，由 DefaultLeaderElectionService
    //   在自己的 leaderOperation 线程上回调进来。拿到 leadership 后并不直接 new JobMaster，而是
    //   交给 startJobMasterServiceProcessAsync()：先查作业是否已完成，否则用 factory 建一个新的
    //   DefaultJobMasterServiceProcess，并转发它的 gateway / result future 到本 runner。
    // ★ 本方法非阻塞，排完队就返回，所以返回时 JobMaster 通常还没建好。
    @Override
    public void grantLeadership(UUID leaderSessionID) {
        runIfStateRunning(
                () -> startJobMasterServiceProcessAsync(leaderSessionID),
                "starting a new JobMasterServiceProcess");
    }

    @GuardedBy("lock")
    private void startJobMasterServiceProcessAsync(UUID leaderSessionId) {
        // ★ 挂到串行队列尾部：保证本操作与 revokeLeadership 不会交叉执行。
        sequentialOperation =
                sequentialOperation.thenCompose(
                        unused ->
                                jobResultStore
                                        .hasJobResultEntryAsync(getJobID())
                                        .thenCompose(
                                                hasJobResult -> {
                                                    // ★ 先查 JobResultStore：命中说明这个 job 已在别处跑完，
                                                    //   此时绝不能再建一次执行图（会重复执行作业），直接以已完成收场。
                                                    if (hasJobResult) {
                                                        return handleJobAlreadyDoneIfValidLeader(
                                                                leaderSessionId);
                                                    } else {
                                                        return createNewJobMasterServiceProcessIfValidLeader(
                                                                leaderSessionId);
                                                    }
                                                }));
        handleAsyncOperationError(sequentialOperation, "Could not start the job manager.");
    }

    private CompletableFuture<Void> handleJobAlreadyDoneIfValidLeader(UUID leaderSessionId) {
        return runIfValidLeader(
                leaderSessionId, () -> jobAlreadyDone(leaderSessionId), "check completed job");
    }

    private CompletableFuture<Void> createNewJobMasterServiceProcessIfValidLeader(
            UUID leaderSessionId) {
        return runIfValidLeader(
                leaderSessionId,
                () ->
                        // the heavy lifting of the JobMasterServiceProcess instantiation is still
                        // done asynchronously (see
                        // DefaultJobMasterServiceFactory#createJobMasterService executing the logic
                        // on the leaderOperation thread in the DefaultLeaderElectionService should
                        // be, therefore, fine
                        ThrowingRunnable.unchecked(
                                        () -> createNewJobMasterServiceProcess(leaderSessionId))
                                .run(),
                "create new job master service process");
    }

    private void printLogIfNotValidLeader(String actionDescription, UUID leaderSessionId) {
        LOG.debug(
                "Ignore leader action '{}' because the leadership runner is no longer the valid leader for {}.",
                actionDescription,
                leaderSessionId);
    }

    private ExecutionGraphInfo createExecutionGraphInfoWithJobStatus(JobStatus jobStatus) {
        return new ExecutionGraphInfo(
                jobMasterServiceProcessFactory.createArchivedExecutionGraph(jobStatus, null));
    }

    private void jobAlreadyDone(UUID leaderSessionId) {
        LOG.info(
                "{} for job {} was granted leadership with leader id {}, but job was already done.",
                getClass().getSimpleName(),
                getJobID(),
                leaderSessionId);
        resultFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        new ExecutionGraphInfo(
                                jobMasterServiceProcessFactory.createArchivedExecutionGraph(
                                        JobStatus.FAILED,
                                        new JobAlreadyDoneException(getJobID())))));
    }

    @GuardedBy("lock")
    // ★ 换 leader 的核心动作：先关旧进程，再用新的 leaderSessionId 建一个新进程。
    //   factory.create() 内部 new DefaultJobMasterServiceProcess，后者构造函数里立刻异步创建 JobMaster
    //   —— 也就是说执行图是在这一刻被构建的（见 DefaultJobMasterServiceProcess 的注释）。
    private void createNewJobMasterServiceProcess(UUID leaderSessionId) throws FlinkException {
        Preconditions.checkState(jobMasterServiceProcess.closeAsync().isDone());

        LOG.info(
                "{} for job {} was granted leadership with leader id {}. Creating new {}.",
                getClass().getSimpleName(),
                getJobID(),
                leaderSessionId,
                JobMasterServiceProcess.class.getSimpleName());

        jobMasterServiceProcess = jobMasterServiceProcessFactory.create(leaderSessionId);

        forwardIfValidLeader(
                leaderSessionId,
                jobMasterServiceProcess.getJobMasterGatewayFuture(),
                jobMasterGatewayFuture,
                "JobMasterGatewayFuture from JobMasterServiceProcess");
        forwardResultFuture(leaderSessionId, jobMasterServiceProcess.getResultFuture());
        confirmLeadership(leaderSessionId, jobMasterServiceProcess.getLeaderAddressFuture());
    }

    private void confirmLeadership(
            UUID leaderSessionId, CompletableFuture<String> leaderAddressFuture) {
        FutureUtils.assertNoException(
                leaderAddressFuture.thenCompose(
                        address ->
                                callIfRunning(
                                                () -> {
                                                    LOG.debug(
                                                            "Confirm leadership {}.",
                                                            leaderSessionId);
                                                    return leaderElection.confirmLeadershipAsync(
                                                            leaderSessionId, address);
                                                },
                                                "confirming leadership")
                                        .orElse(FutureUtils.completedVoidFuture())));
    }

    // ★ 失败路径的关键一环：把 JobMasterServiceProcess 的 resultFuture 接到本 runner 的 resultFuture 上，
    //   于是 JobMaster 的初始化失败、作业结束等信号才能冒泡到 Dispatcher。
    private void forwardResultFuture(
            UUID leaderSessionId, CompletableFuture<JobManagerRunnerResult> resultFuture) {
        resultFuture.whenComplete(
                (jobManagerRunnerResult, throwable) ->
                        runIfValidLeader(
                                leaderSessionId,
                                () -> onJobCompletion(jobManagerRunnerResult, throwable),
                                "result future forwarding"));
    }

    @GuardedBy("lock")
    // ★ 状态转换点：runner 由 RUNNING 变为 JOB_COMPLETED，此后不再理会任何选主回调。
    //   结果是异常/初始化失败时，除了让 resultFuture 异常完成，还要把 jobMasterGatewayFuture 打死
    //   —— 此时已不可能再拿到可用的 JobMaster。
    private void onJobCompletion(
            JobManagerRunnerResult jobManagerRunnerResult, Throwable throwable) {
        state = State.JOB_COMPLETED;

        LOG.debug("Completing the result for job {}.", getJobID());

        if (throwable != null) {
            resultFuture.completeExceptionally(throwable);
            jobMasterGatewayFuture.completeExceptionally(
                    new FlinkException(
                            "Could not retrieve JobMasterGateway because the JobMaster failed.",
                            throwable));
        } else {
            if (!jobManagerRunnerResult.isSuccess()) {
                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException(
                                "Could not retrieve JobMasterGateway because the JobMaster initialization failed.",
                                jobManagerRunnerResult.getInitializationFailure()));
            }

            resultFuture.complete(jobManagerRunnerResult);
        }
    }

    // 失去 leadership（例如 HA 主备切换）：不关 runner，只停掉当前的 JobMasterServiceProcess，
    // 保留 runner 等待下一次 grantLeadership 重新建一个进程。
    @Override
    public void revokeLeadership() {
        runIfStateRunning(
                this::stopJobMasterServiceProcessAsync,
                "revoke leadership from JobMasterServiceProcess");
    }

    @GuardedBy("lock")
    private void stopJobMasterServiceProcessAsync() {
        sequentialOperation =
                sequentialOperation.thenCompose(
                        ignored ->
                                callIfRunning(
                                                this::stopJobMasterServiceProcess,
                                                "stop leading JobMasterServiceProcess")
                                        .orElse(FutureUtils.completedVoidFuture()));

        handleAsyncOperationError(sequentialOperation, "Could not suspend the job manager.");
    }

    @GuardedBy("lock")
    private CompletableFuture<Void> stopJobMasterServiceProcess() {
        LOG.info(
                "{} for job {} was revoked leadership with leader id {}. Stopping current {}.",
                getClass().getSimpleName(),
                getJobID(),
                jobMasterServiceProcess.getLeaderSessionId(),
                JobMasterServiceProcess.class.getSimpleName());

        jobMasterGatewayFuture.completeExceptionally(
                new FlinkException(
                        "Cannot obtain JobMasterGateway because the JobMaster lost leadership."));
        jobMasterGatewayFuture = new CompletableFuture<>();

        hasCurrentLeaderBeenCancelled = false;

        return jobMasterServiceProcess.closeAsync();
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(exception);
    }

    private void handleAsyncOperationError(CompletableFuture<Void> operation, String message) {
        operation.whenComplete(
                (unused, throwable) -> {
                    if (throwable != null) {
                        runIfStateRunning(
                                () ->
                                        handleJobMasterServiceLeadershipRunnerError(
                                                new FlinkException(message, throwable)),
                                "handle JobMasterServiceLeadershipRunner error");
                    }
                });
    }

    private void handleJobMasterServiceLeadershipRunnerError(Throwable cause) {
        if (ExceptionUtils.isJvmFatalError(cause)) {
            fatalErrorHandler.onFatalError(cause);
        } else {
            resultFuture.completeExceptionally(cause);
        }
    }

    private void runIfStateRunning(Runnable action, String actionDescription) {
        synchronized (lock) {
            if (isRunning()) {
                action.run();
            } else {
                LOG.debug(
                        "Ignore '{}' because the leadership runner is no longer running.",
                        actionDescription);
            }
        }
    }

    private <T> Optional<T> callIfRunning(
            Supplier<? extends T> supplier, String supplierDescription) {
        synchronized (lock) {
            if (isRunning()) {
                return Optional.of(supplier.get());
            } else {
                LOG.debug(
                        "Ignore '{}' because the leadership runner is no longer running.",
                        supplierDescription);
                return Optional.empty();
            }
        }
    }

    @GuardedBy("lock")
    private boolean isRunning() {
        return state == State.RUNNING;
    }

    private CompletableFuture<Void> runIfValidLeader(
            UUID expectedLeaderId, Runnable action, Runnable noLeaderFallback) {
        synchronized (lock) {
            if (isRunning() && leaderElection != null) {
                return leaderElection
                        .hasLeadershipAsync(expectedLeaderId)
                        .thenAccept(
                                hasLeadership -> {
                                    synchronized (lock) {
                                        if (isRunning() && hasLeadership) {
                                            action.run();
                                        } else {
                                            noLeaderFallback.run();
                                        }
                                    }
                                });
            } else {
                noLeaderFallback.run();
                return FutureUtils.completedVoidFuture();
            }
        }
    }

    private CompletableFuture<Void> runIfValidLeader(
            UUID expectedLeaderId, Runnable action, String noLeaderFallbackCommandDescription) {
        return runIfValidLeader(
                expectedLeaderId,
                action,
                () ->
                        printLogIfNotValidLeader(
                                noLeaderFallbackCommandDescription, expectedLeaderId));
    }

    private <T> void forwardIfValidLeader(
            UUID expectedLeaderId,
            CompletableFuture<? extends T> source,
            CompletableFuture<T> target,
            String forwardDescription) {
        source.whenComplete(
                (t, throwable) ->
                        runIfValidLeader(
                                expectedLeaderId,
                                () -> {
                                    if (throwable != null) {
                                        target.completeExceptionally(throwable);
                                    } else {
                                        target.complete(t);
                                    }
                                },
                                forwardDescription));
    }

    enum State {
        RUNNING,
        STOPPED,
        JOB_COMPLETED,
    }
}
