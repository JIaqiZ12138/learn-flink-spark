/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.cleanup.CleanupRunnerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.DispatcherResourceCleanerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleaner;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleanerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntryPointExceptionUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.MdcUtils.MdcCloseable;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for the Dispatcher component. The Dispatcher component is responsible for receiving
 * job submissions, persisting them, spawning JobManagers to execute the jobs and to recover them in
 * case of a master failure. Furthermore, it knows about the state of the Flink session cluster.
 */
// ★ 职责边界：Dispatcher 管"作业的注册与生命周期"，不管"作业怎么跑"。
//   管：接收并持久化 JobGraph、为每个作业创建一个 JobManagerRunner（内含 JobMaster）、
//       汇总各作业状态供 Web/REST 查询、作业终态后收敛状态并清理资源。
//   不管：不做 StreamGraph → JobGraph 的转换（那是客户端 FlinkPipelineTranslationUtil 的活），
//         不做调度、不碰 ExecutionGraph 与槽位（slot）分配（那是 JobMaster 的活）。
// ★ 创建时机极易误解：DefaultDispatcherResourceManagerComponentFactory.create() 调
//   dispatcherRunnerFactory.createDispatcherRunner(...) 时，内部只做了
//   leaderElection.startLeaderElection(this) 就立刻返回，此刻 Dispatcher 实例还不存在。
//   真正的创建发生在选主成功之后的异步链路：
//     grantLeadership → JobDispatcherLeaderProcess.onStart()
//       → JobDispatcherFactory.createDispatcher(recoveredJobs) → new MiniDispatcher(...)
// ★ per-job（YARN application）模式是"先有 JobGraph、后起 Dispatcher"：AM 启动时用
//   FileJobGraphRetriever 从 YARN local resource 读回 job.graph，作为 recoveredJobs 传进来，
//   因此第一个作业走的是 ExecutionType.RECOVERY 而不是 SUBMISSION（详见 runRecoveredJob）。
public abstract class Dispatcher extends FencedRpcEndpoint<DispatcherId>
        implements DispatcherGateway {

    @VisibleForTesting @Internal
    public static final ConfigOption<Duration> CLIENT_ALIVENESS_CHECK_DURATION =
            ConfigOptions.key("$internal.dispatcher.client-aliveness-check.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1));

    public static final String DISPATCHER_NAME = "dispatcher";

    private static final int INITIAL_JOB_MANAGER_RUNNER_REGISTRY_CAPACITY = 16;

    private final Configuration configuration;

    private final JobGraphWriter jobGraphWriter;
    private final JobResultStore jobResultStore;

    private final HighAvailabilityServices highAvailabilityServices;
    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
    private final JobManagerSharedServices jobManagerSharedServices;
    private final HeartbeatServices heartbeatServices;
    private final BlobServer blobServer;

    private final FatalErrorHandler fatalErrorHandler;
    private final Collection<FailureEnricher> failureEnrichers;

    private final OnMainThreadJobManagerRunnerRegistry jobManagerRunnerRegistry;

    private final Collection<JobGraph> recoveredJobs;

    private final Collection<JobResult> recoveredDirtyJobs;

    private final DispatcherBootstrapFactory dispatcherBootstrapFactory;

    private final ExecutionGraphInfoStore executionGraphInfoStore;

    private final JobManagerRunnerFactory jobManagerRunnerFactory;
    private final CleanupRunnerFactory cleanupRunnerFactory;

    private final JobManagerMetricGroup jobManagerMetricGroup;

    private final HistoryServerArchivist historyServerArchivist;

    private final Executor ioExecutor;

    @Nullable private final String metricServiceQueryAddress;

    private final Map<JobID, CompletableFuture<Void>> jobManagerRunnerTerminationFutures;
    private final Set<JobID> submittedAndWaitingTerminationJobIDs;

    protected final CompletableFuture<ApplicationStatus> shutDownFuture;

    private DispatcherBootstrap dispatcherBootstrap;

    private final DispatcherCachedOperationsHandler dispatcherCachedOperationsHandler;

    private final ResourceCleaner localResourceCleaner;
    private final ResourceCleaner globalResourceCleaner;

    private final Time webTimeout;

    private final Map<JobID, Long> jobClientExpiredTimestamp = new HashMap<>();
    private final Map<JobID, Long> uninitializedJobClientHeartbeatTimeout = new HashMap<>();
    private final long jobClientAlivenessCheckInterval;
    private ScheduledFuture<?> jobClientAlivenessCheck;

    /**
     * Simple data-structure to keep track of pending {@link JobResourceRequirements} updates, so we
     * can prevent race conditions.
     */
    private final Set<JobID> pendingJobResourceRequirementsUpdates = new HashSet<>();

    // ★ ExecutionType 区分"首次提交"与"恢复重新拉起"。它不改变 JobGraph，只决定
    //   JobManagerRunner 初始化失败时的处理策略（见 handleJobManagerRunnerResult）：
    //   恢复中的作业初始化失败要升级为 Dispatcher 致命错误，以便重新选主后整体再恢复一次。
    /** Enum to distinguish between initial job submission and re-submission for recovery. */
    protected enum ExecutionType {
        SUBMISSION,
        RECOVERY
    }

    public Dispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobs,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices)
            throws Exception {
        this(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobs,
                dispatcherBootstrapFactory,
                dispatcherServices,
                new DefaultJobManagerRunnerRegistry(INITIAL_JOB_MANAGER_RUNNER_REGISTRY_CAPACITY));
    }

    private Dispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobs,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices,
            JobManagerRunnerRegistry jobManagerRunnerRegistry)
            throws Exception {
        this(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobs,
                dispatcherBootstrapFactory,
                dispatcherServices,
                jobManagerRunnerRegistry,
                new DispatcherResourceCleanerFactory(jobManagerRunnerRegistry, dispatcherServices));
    }

    @VisibleForTesting
    protected Dispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobs,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices,
            JobManagerRunnerRegistry jobManagerRunnerRegistry,
            ResourceCleanerFactory resourceCleanerFactory)
            throws Exception {
        super(rpcService, RpcServiceUtils.createRandomName(DISPATCHER_NAME), fencingToken);
        assertRecoveredJobsAndDirtyJobResults(recoveredJobs, recoveredDirtyJobs);

        this.configuration = dispatcherServices.getConfiguration();
        this.highAvailabilityServices = dispatcherServices.getHighAvailabilityServices();
        this.resourceManagerGatewayRetriever =
                dispatcherServices.getResourceManagerGatewayRetriever();
        this.heartbeatServices = dispatcherServices.getHeartbeatServices();
        this.blobServer = dispatcherServices.getBlobServer();
        this.fatalErrorHandler = dispatcherServices.getFatalErrorHandler();
        this.failureEnrichers = dispatcherServices.getFailureEnrichers();
        // ★ 所有协作者（含 jobGraphWriter、jobResultStore、jobManagerRunnerFactory）都来自
        //   DispatcherServices，由 DispatcherResourceManagerComponentFactory 装配好后注入。
        //   所以"构造 Dispatcher"本身不会创建任何 JobMaster —— JobMaster 是每个作业一个，
        //   要等 jobManagerRunnerFactory.createJobManagerRunner(...) 被调用时才诞生。
        this.jobGraphWriter = dispatcherServices.getJobGraphWriter();
        this.jobResultStore = dispatcherServices.getJobResultStore();
        this.jobManagerMetricGroup = dispatcherServices.getJobManagerMetricGroup();
        this.metricServiceQueryAddress = dispatcherServices.getMetricQueryServiceAddress();
        this.ioExecutor = dispatcherServices.getIoExecutor();

        this.jobManagerSharedServices =
                JobManagerSharedServices.fromConfiguration(
                        configuration, blobServer, fatalErrorHandler);

        this.jobManagerRunnerRegistry =
                new OnMainThreadJobManagerRunnerRegistry(
                        jobManagerRunnerRegistry, this.getMainThreadExecutor());

        this.historyServerArchivist = dispatcherServices.getHistoryServerArchivist();

        this.executionGraphInfoStore = dispatcherServices.getArchivedExecutionGraphStore();

        this.jobManagerRunnerFactory = dispatcherServices.getJobManagerRunnerFactory();
        this.cleanupRunnerFactory = dispatcherServices.getCleanupRunnerFactory();

        this.jobManagerRunnerTerminationFutures =
                CollectionUtil.newHashMapWithExpectedSize(
                        INITIAL_JOB_MANAGER_RUNNER_REGISTRY_CAPACITY);
        this.submittedAndWaitingTerminationJobIDs = new HashSet<>();

        this.shutDownFuture = new CompletableFuture<>();

        this.dispatcherBootstrapFactory = checkNotNull(dispatcherBootstrapFactory);

        // ★ per-job 模式下这里通常只有 1 个元素：从 job.graph 读回来的那个 JobGraph。
        //   构造期只是暂存，真正拉起发生在 onStart() → startRecoveredJobs()。
        this.recoveredJobs = new HashSet<>(recoveredJobs);

        this.recoveredDirtyJobs = new HashSet<>(recoveredDirtyJobs);

        this.blobServer.retainJobs(
                recoveredJobs.stream().map(JobGraph::getJobID).collect(Collectors.toSet()),
                dispatcherServices.getIoExecutor());

        this.dispatcherCachedOperationsHandler =
                new DispatcherCachedOperationsHandler(
                        dispatcherServices.getOperationCaches(),
                        this::triggerCheckpointAndGetCheckpointID,
                        this::triggerSavepointAndGetLocation,
                        this::stopWithSavepointAndGetLocation);

        this.localResourceCleaner =
                resourceCleanerFactory.createLocalResourceCleaner(this.getMainThreadExecutor());
        this.globalResourceCleaner =
                resourceCleanerFactory.createGlobalResourceCleaner(this.getMainThreadExecutor());

        this.webTimeout = Time.fromDuration(configuration.get(WebOptions.TIMEOUT));

        this.jobClientAlivenessCheckInterval =
                configuration.get(CLIENT_ALIVENESS_CHECK_DURATION).toMillis();
    }

    // ------------------------------------------------------
    // Getters
    // ------------------------------------------------------

    public CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return shutDownFuture;
    }

    // ------------------------------------------------------
    // Lifecycle methods
    // ------------------------------------------------------

    // ★ onStart() 由该 RpcEndpoint 自己的 RPC 主线程（mainThreadExecutor）回调，是 Dispatcher
    //   实例真正"上线"的时刻；构造函数只把依赖挂好，不做任何实际启动动作。
    // ★ 此时 jobGraphWriter / jobManagerRunnerFactory 已经在构造函数里从 DispatcherServices
    //   挂上来了，这里的意义是"正式开始使用它们"：
    //     · jobGraphWriter —— 后续把 JobGraph 持久化到 HA 存储（恢复时靠它把作业读回来）；
    //     · jobManagerRunnerFactory —— "每个作业一个 runner"的工厂，调用它才会创建 JobMaster。
    //   两者都不在这里创建，别把 onStart 误读成"创建 Dispatcher 的组件"。
    // ★ 调用顺序有讲究：先启动服务与清理重试，再拉起 recoveredJobs（per-job 模式就是那个唯一作业），
    //   最后才创建 DispatcherBootstrap —— MiniDispatcher 依赖它实现"作业结束后关掉整个集群"。
    // 通用dispatcher on start mini Dispatcher 未做扩展实现
    @Override
    public void onStart() throws Exception {
        try {
            startDispatcherServices();
        } catch (Throwable t) {
            final DispatcherException exception =
                    new DispatcherException(
                            String.format("Could not start the Dispatcher %s", getAddress()), t);
            onFatalError(exception);
            throw exception;
        }

        startCleanupRetries();
        startRecoveredJobs();

        this.dispatcherBootstrap =
                this.dispatcherBootstrapFactory.create(
                        getSelfGateway(DispatcherGateway.class),
                        this.getRpcService().getScheduledExecutor(),
                        this::onFatalError);
    }

    private void startDispatcherServices() throws Exception {
        try {
            registerDispatcherMetrics(jobManagerMetricGroup);
        } catch (Exception e) {
            handleStartDispatcherServicesException(e);
        }
    }

    private static void assertRecoveredJobsAndDirtyJobResults(
            Collection<JobGraph> recoveredJobs, Collection<JobResult> recoveredDirtyJobResults) {
        final Set<JobID> jobIdsOfFinishedJobs =
                recoveredDirtyJobResults.stream()
                        .map(JobResult::getJobId)
                        .collect(Collectors.toSet());

        final boolean noRecoveredJobGraphHasDirtyJobResult =
                recoveredJobs.stream()
                        .noneMatch(
                                recoveredJobGraph ->
                                        jobIdsOfFinishedJobs.contains(
                                                recoveredJobGraph.getJobID()));

        Preconditions.checkArgument(
                noRecoveredJobGraphHasDirtyJobResult,
                "There should be no overlap between the recovered JobGraphs and the passed dirty JobResults based on their job ID.");
    }

    // ★★ 这里是被 recoveredJobs 拉起的作业入口。recoveredJobs 从哪来？
    //    · per-job（YARN application）模式：AM 启动时用 FileJobGraphRetriever 从 YARN
    //      local resource 里读回 job.graph，再经 JobDispatcherFactory.createDispatcher(recoveredJobs)
    //      一路传进 MiniDispatcher 的构造函数 —— 即"先有 JobGraph、后起 Dispatcher"。
    //    · session / HA 模式：来自 jobGraphWriter 持久化在 HA 存储里的 JobGraph。
    //    所以这段代码不是"重新提交"，而是把已经存在的 JobGraph 直接拉起执行。
    private void startRecoveredJobs() {
        for (JobGraph recoveredJob : recoveredJobs) {
            runRecoveredJob(recoveredJob);
        }
        recoveredJobs.clear();
    }

    // ★★ 最易误解的一处：下面传的是 ExecutionType.RECOVERY，而不是 SUBMISSION。
    //    per-job 模式下连"第一个作业"也走 RECOVERY —— 因为它的 JobGraph 是从 job.graph
    //    读回来的，已经存在，不需要（也不应该）再经 submitJob 那条提交链路。
    //    与 SUBMISSION 的唯一实质差别在 handleJobManagerRunnerResult：
    //    RECOVERY 时 runner 初始化失败会升级为 Dispatcher 致命错误（重新选主后再整体恢复一次），
    //    SUBMISSION 时只把该作业判为失败。
    private void runRecoveredJob(final JobGraph recoveredJob) {
        checkNotNull(recoveredJob);

        initJobClientExpiredTime(recoveredJob);

        try (MdcCloseable ignored =
                MdcUtils.withContext(MdcUtils.asContextData(recoveredJob.getJobID()))) {
            // ★★ 顺序即语义：createJobMasterRunner(recoveredJob) 用 jobManagerRunnerFactory
            //    为该作业造出唯一的 runner，runJob 再把它启动并登记。作业从这里真正跑起来。
            //    注意这条路径不会调用 jobGraphWriter.putJobGraph —— JobGraph 本来就在。
            runJob(createJobMasterRunner(recoveredJob), ExecutionType.RECOVERY);
        } catch (Throwable throwable) {
            onFatalError(
                    new DispatcherException(
                            String.format(
                                    "Could not start recovered job %s.", recoveredJob.getJobID()),
                            throwable));
        }
    }

    private void initJobClientExpiredTime(JobGraph jobGraph) {
        JobID jobID = jobGraph.getJobID();
        long initialClientHeartbeatTimeout = jobGraph.getInitialClientHeartbeatTimeout();
        if (initialClientHeartbeatTimeout > 0) {
            log.info(
                    "Begin to detect the client's aliveness for job {}. The heartbeat timeout is {}",
                    jobID,
                    initialClientHeartbeatTimeout);
            uninitializedJobClientHeartbeatTimeout.put(jobID, initialClientHeartbeatTimeout);

            if (jobClientAlivenessCheck == null) {
                // Use the client heartbeat timeout as the check interval.
                jobClientAlivenessCheck =
                        this.getRpcService()
                                .getScheduledExecutor()
                                .scheduleWithFixedDelay(
                                        () ->
                                                getMainThreadExecutor(jobID)
                                                        .execute(this::checkJobClientAliveness),
                                        0L,
                                        jobClientAlivenessCheckInterval,
                                        TimeUnit.MILLISECONDS);
            }
        }
    }

    private void startCleanupRetries() {
        recoveredDirtyJobs.forEach(this::runCleanupRetry);
        recoveredDirtyJobs.clear();
    }

    private void runCleanupRetry(final JobResult jobResult) {
        checkNotNull(jobResult);

        try {
            runJob(createJobCleanupRunner(jobResult), ExecutionType.RECOVERY);
        } catch (Throwable throwable) {
            onFatalError(
                    new DispatcherException(
                            String.format(
                                    "Could not start cleanup retry for job %s.",
                                    jobResult.getJobId()),
                            throwable));
        }
    }

    private void handleStartDispatcherServicesException(Exception e) throws Exception {
        try {
            stopDispatcherServices();
        } catch (Exception exception) {
            e.addSuppressed(exception);
        }

        throw e;
    }

    // ★ 停止顺序有讲究：先取消客户端存活检测，再关闭所有 JobManagerRunner 并等它们真正终止
    //   （terminationFuture 全完成），此时才停 DispatcherBootstrap 与各项服务 ——
    //   保证作业是被"优雅终止"而不是随 JVM 一起消失。
    @Override
    public CompletableFuture<Void> onStop() {
        log.info("Stopping dispatcher {}.", getAddress());

        if (jobClientAlivenessCheck != null) {
            jobClientAlivenessCheck.cancel(false);
            jobClientAlivenessCheck = null;
        }

        final CompletableFuture<Void> allJobsTerminationFuture =
                terminateRunningJobsAndGetTerminationFuture();

        return FutureUtils.runAfterwards(
                allJobsTerminationFuture,
                () -> {
                    dispatcherBootstrap.stop();

                    stopDispatcherServices();

                    log.info("Stopped dispatcher {}.", getAddress());
                });
    }

    private void stopDispatcherServices() throws Exception {
        Exception exception = null;
        try {
            jobManagerSharedServices.shutdown();
        } catch (Exception e) {
            exception = e;
        }

        jobManagerMetricGroup.close();

        ExceptionUtils.tryRethrowException(exception);
    }

    // ------------------------------------------------------
    // RPCs
    // ------------------------------------------------------

    // ★ Dispatcher 不参与任何"作业转换"：这里拿到的已经是客户端编译好的 JobGraph
    //   （StreamGraph → JobGraph 的转换在客户端 FlinkPipelineTranslationUtil 中完成）。
    //   submitJob 只负责"转发 + 持久化 + 建 runner"：
    //     去重校验（已全局终态 / 已在跑 / 资源半配置）→ internalSubmitJob
    //     → jobGraphWriter 持久化 → jobManagerRunnerFactory 创建 JobManagerRunner。
    //   校验是异步的（先查 JobResultStore），最终在 getMainThreadExecutor(jobID) 上串行执行 ——
    //   用作业自己的 main thread 保证同一作业的状态变更不并发。
    // dispatcher 提交job 只做jobGraph的转发，不做进一步转换
    @Override
    public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
        final JobID jobID = jobGraph.getJobID();
        try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobID))) {
            log.info("Received JobGraph submission '{}' ({}).", jobGraph.getName(), jobID);
        }
        return isInGloballyTerminalState(jobID)
                .thenComposeAsync(
                        isTerminated -> {
                            if (isTerminated) {
                                log.warn(
                                        "Ignoring JobGraph submission '{}' ({}) because the job already "
                                                + "reached a globally-terminal state (i.e. {}) in a "
                                                + "previous execution.",
                                        jobGraph.getName(),
                                        jobID,
                                        Arrays.stream(JobStatus.values())
                                                .filter(JobStatus::isGloballyTerminalState)
                                                .map(JobStatus::name)
                                                .collect(Collectors.joining(", ")));
                                return FutureUtils.completedExceptionally(
                                        DuplicateJobSubmissionException.ofGloballyTerminated(
                                                jobID));
                            } else if (jobManagerRunnerRegistry.isRegistered(jobID)
                                    || submittedAndWaitingTerminationJobIDs.contains(jobID)) {
                                // job with the given jobID is not terminated, yet
                                return FutureUtils.completedExceptionally(
                                        DuplicateJobSubmissionException.of(jobID));
                            } else if (isPartialResourceConfigured(jobGraph)) {
                                return FutureUtils.completedExceptionally(
                                        new JobSubmissionException(
                                                jobID,
                                                "Currently jobs is not supported if parts of the vertices "
                                                        + "have resources configured. The limitation will be "
                                                        + "removed in future versions."));
                            } else {
                                return internalSubmitJob(jobGraph);
                            }
                        },
                        getMainThreadExecutor(jobID));
    }

    @Override
    public CompletableFuture<Acknowledge> submitFailedJob(
            JobID jobId, String jobName, Throwable exception) {
        final ArchivedExecutionGraph archivedExecutionGraph =
                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                        jobId,
                        jobName,
                        JobStatus.FAILED,
                        null,
                        exception,
                        null,
                        System.currentTimeMillis());
        ExecutionGraphInfo executionGraphInfo = new ExecutionGraphInfo(archivedExecutionGraph);
        writeToExecutionGraphInfoStore(executionGraphInfo);
        return archiveExecutionGraphToHistoryServer(executionGraphInfo);
    }

    /**
     * Checks whether the given job has already been executed.
     *
     * @param jobId identifying the submitted job
     * @return a successfully completed future with {@code true} if the job has already finished,
     *     either successfully or as a failure
     */
    private CompletableFuture<Boolean> isInGloballyTerminalState(JobID jobId) {
        return jobResultStore.hasJobResultEntryAsync(jobId);
    }

    private boolean isPartialResourceConfigured(JobGraph jobGraph) {
        boolean hasVerticesWithUnknownResource = false;
        boolean hasVerticesWithConfiguredResource = false;

        for (JobVertex jobVertex : jobGraph.getVertices()) {
            if (jobVertex.getMinResources() == ResourceSpec.UNKNOWN) {
                hasVerticesWithUnknownResource = true;
            } else {
                hasVerticesWithConfiguredResource = true;
            }

            if (hasVerticesWithUnknownResource && hasVerticesWithConfiguredResource) {
                return true;
            }
        }

        return false;
    }

    private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
        applyParallelismOverrides(jobGraph);
        log.info("Submitting job '{}' ({}).", jobGraph.getName(), jobGraph.getJobID());

        // track as an outstanding job
        submittedAndWaitingTerminationJobIDs.add(jobGraph.getJobID());

        // ★ 同一个 jobID 上一次的 JobManagerRunner 可能还在终止中，必须等它的 terminationFuture
        //   完成后再提交，否则会出现两个 runner 抢同一个 jobID 的竞态。
        return waitForTerminatingJob(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
                .handle((ignored, throwable) -> handleTermination(jobGraph.getJobID(), throwable))
                .thenCompose(Function.identity())
                .whenComplete(
                        (ignored, throwable) ->
                                // job is done processing, whether failed or finished
                                submittedAndWaitingTerminationJobIDs.remove(jobGraph.getJobID()));
    }

    private CompletableFuture<Acknowledge> handleTermination(
            JobID jobId, @Nullable Throwable terminationThrowable) {
        if (terminationThrowable != null) {
            return globalResourceCleaner
                    .cleanupAsync(jobId)
                    .handleAsync(
                            (ignored, cleanupThrowable) -> {
                                if (cleanupThrowable != null) {
                                    log.warn(
                                            "Cleanup didn't succeed after job submission failed for job {}.",
                                            jobId,
                                            cleanupThrowable);
                                    terminationThrowable.addSuppressed(cleanupThrowable);
                                }
                                ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(
                                        terminationThrowable);
                                final Throwable strippedThrowable =
                                        ExceptionUtils.stripCompletionException(
                                                terminationThrowable);
                                log.error("Failed to submit job {}.", jobId, strippedThrowable);
                                throw new CompletionException(
                                        new JobSubmissionException(
                                                jobId, "Failed to submit job.", strippedThrowable));
                            },
                            getMainThreadExecutor(jobId));
        }
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ★ 提交链路真正"落地"的一步：先 jobGraphWriter.putJobGraph 把 JobGraph 写进 HA 存储
    //   （JobManager 挂掉后靠它恢复，恢复时就走 runRecoveredJob 那条 RECOVERY 路径），
    //   再 createJobMasterRunner 造 runner 并以 SUBMISSION 类型启动。
    private void persistAndRunJob(JobGraph jobGraph) throws Exception {
        jobGraphWriter.putJobGraph(jobGraph);
        initJobClientExpiredTime(jobGraph);
        runJob(createJobMasterRunner(jobGraph), ExecutionType.SUBMISSION);
    }

    // ★ 与 jobManagerRunnerFactory 的关系：Dispatcher 只持有一个工厂，工厂按需为每个作业
    //   创建一个 JobManagerRunner（1.20 默认实现是 JobMasterServiceLeadershipRunnerFactory，
    //   造出 JobMasterServiceLeadershipRunner，JobMaster 再由其内部的 JobMasterServiceProcess
    //   异步创建），即"每个作业一个 runner、一个 JobMaster、一份 ExecutionGraph"。
    //   这里传入 RpcService / HA / BlobServer / 心跳 / 指标组等运行时依赖，
    //   说明 runner 是作业级隔离的，而 Dispatcher 只是它们的共同宿主。
    private JobManagerRunner createJobMasterRunner(JobGraph jobGraph) throws Exception {
        Preconditions.checkState(!jobManagerRunnerRegistry.isRegistered(jobGraph.getJobID()));
        return jobManagerRunnerFactory.createJobManagerRunner(
                jobGraph,
                configuration,
                getRpcService(),
                highAvailabilityServices,
                heartbeatServices,
                jobManagerSharedServices,
                new DefaultJobManagerJobMetricGroupFactory(jobManagerMetricGroup),
                fatalErrorHandler,
                failureEnrichers,
                System.currentTimeMillis());
    }

    private JobManagerRunner createJobCleanupRunner(JobResult dirtyJobResult) throws Exception {
        Preconditions.checkState(!jobManagerRunnerRegistry.isRegistered(dirtyJobResult.getJobId()));
        return cleanupRunnerFactory.create(
                dirtyJobResult,
                highAvailabilityServices.getCheckpointRecoveryFactory(),
                configuration,
                getIoExecutor(dirtyJobResult.getJobId()));
    }

    // ★ runJob 是"一个 Entry（JobGraph）变成真正跑起来的作业"的唯一收口点：
    //   submitJob 正常提交（SUBMISSION）与 startRecoveredJobs / runCleanupRetry（RECOVERY）
    //   都汇聚到这里。三步：
    //     第 1 步 start() —— runner 启动，内部才真正创建 JobMaster（进而建 ExecutionGraph）；
    //     第 2 步 register —— 登记进 jobManagerRunnerRegistry，此后 RPC / Web 才能查到它；
    //     第 3 步 挂 resultFuture 回调 —— 作业到终态时自动收敛状态、清理资源、摘掉登记。
    private void runJob(JobManagerRunner jobManagerRunner, ExecutionType executionType)
            throws Exception {
        jobManagerRunner.start();
        jobManagerRunnerRegistry.register(jobManagerRunner);

        final JobID jobId = jobManagerRunner.getJobID();

        final CompletableFuture<CleanupJobState> cleanupJobStateFuture =
                jobManagerRunner
                        .getResultFuture()
                        .handleAsync(
                                (jobManagerRunnerResult, throwable) -> {
                                    Preconditions.checkState(
                                            jobManagerRunnerRegistry.isRegistered(jobId)
                                                    && jobManagerRunnerRegistry.get(jobId)
                                                            == jobManagerRunner,
                                            "The job entry in runningJobs must be bound to the lifetime of the JobManagerRunner.");

                                    if (jobManagerRunnerResult != null) {
                                        return handleJobManagerRunnerResult(
                                                jobManagerRunnerResult, executionType);
                                    } else {
                                        return CompletableFuture.completedFuture(
                                                jobManagerRunnerFailed(
                                                        jobId, JobStatus.FAILED, throwable));
                                    }
                                },
                                getMainThreadExecutor(jobId))
                        .thenCompose(Function.identity());

        final CompletableFuture<Void> jobTerminationFuture =
                cleanupJobStateFuture.thenCompose(
                        cleanupJobState ->
                                removeJob(jobId, cleanupJobState)
                                        .exceptionally(
                                                throwable ->
                                                        logCleanupErrorWarning(jobId, throwable)));

        FutureUtils.handleUncaughtException(
                jobTerminationFuture,
                (thread, throwable) -> fatalErrorHandler.onFatalError(throwable));
        registerJobManagerRunnerTerminationFuture(jobId, jobTerminationFuture);
    }

    @Nullable
    private Void logCleanupErrorWarning(JobID jobId, Throwable cleanupError) {
        log.warn(
                "The cleanup of job {} failed. The job's artifacts in the different directories ('{}', '{}', '{}') and its JobResultStore entry in '{}' (in HA mode) should be checked for manual cleanup.",
                jobId,
                configuration.get(HighAvailabilityOptions.HA_STORAGE_PATH),
                configuration.get(BlobServerOptions.STORAGE_DIRECTORY),
                configuration.get(CheckpointingOptions.CHECKPOINTS_DIRECTORY),
                configuration.get(JobResultStoreOptions.STORAGE_PATH),
                cleanupError);
        return null;
    }

    // ★★ 状态收敛点：JobManagerRunner 的 resultFuture 完成（作业到达终态，或 runner 初始化就失败）
    //    时进入这里，是 ExecutionType 唯一真正起作用的地方：
    //      · RECOVERY 且初始化失败 → 不走正常终态处理，而是把 JobStatus 记为 INITIALIZING 并触发
    //        Dispatcher 致命错误，让重新选主后把所有作业整体再恢复一次（该机制仅在 HA 模式下生效）；
    //      · 其他情况 → 交给 jobReachedTerminalState 正常收敛。
    private CompletableFuture<CleanupJobState> handleJobManagerRunnerResult(
            JobManagerRunnerResult jobManagerRunnerResult, ExecutionType executionType) {
        if (jobManagerRunnerResult.isInitializationFailure()
                && executionType == ExecutionType.RECOVERY) {
            // fail fatally to make the Dispatcher fail-over and recover all jobs once more (which
            // can only happen in HA mode)
            return CompletableFuture.completedFuture(
                    jobManagerRunnerFailed(
                            jobManagerRunnerResult.getExecutionGraphInfo().getJobId(),
                            JobStatus.INITIALIZING,
                            jobManagerRunnerResult.getInitializationFailure()));
        }
        return jobReachedTerminalState(jobManagerRunnerResult.getExecutionGraphInfo());
    }

    private static class CleanupJobState {

        private final boolean globalCleanup;
        private final JobStatus jobStatus;

        public static CleanupJobState localCleanup(JobStatus jobStatus) {
            return new CleanupJobState(false, jobStatus);
        }

        public static CleanupJobState globalCleanup(JobStatus jobStatus) {
            return new CleanupJobState(true, jobStatus);
        }

        private CleanupJobState(boolean globalCleanup, JobStatus jobStatus) {
            this.globalCleanup = globalCleanup;
            this.jobStatus = jobStatus;
        }

        public boolean isGlobalCleanup() {
            return globalCleanup;
        }

        public JobStatus getJobStatus() {
            return jobStatus;
        }
    }

    private CleanupJobState jobManagerRunnerFailed(
            JobID jobId, JobStatus jobStatus, Throwable throwable) {
        jobMasterFailed(jobId, throwable);
        return CleanupJobState.localCleanup(jobStatus);
    }

    @Override
    public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
        return CompletableFuture.completedFuture(
                Collections.unmodifiableSet(jobManagerRunnerRegistry.getRunningJobIds()));
    }

    @Override
    public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath, Time timeout) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        return CompletableFuture.supplyAsync(
                () -> {
                    log.info("Disposing savepoint {}.", savepointPath);

                    try {
                        Checkpoints.disposeSavepoint(
                                savepointPath, configuration, classLoader, log);
                    } catch (IOException | FlinkException e) {
                        throw new CompletionException(
                                new FlinkException(
                                        String.format(
                                                "Could not dispose savepoint %s.", savepointPath),
                                        e));
                    }

                    return Acknowledge.get();
                },
                jobManagerSharedServices.getIoExecutor());
    }

    @Override
    public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
        Optional<JobManagerRunner> maybeJob = getJobManagerRunner(jobId);

        if (maybeJob.isPresent()) {
            return maybeJob.get().cancel(timeout);
        }

        final ExecutionGraphInfo executionGraphInfo = executionGraphInfoStore.get(jobId);
        if (executionGraphInfo != null) {
            final JobStatus jobStatus = executionGraphInfo.getArchivedExecutionGraph().getState();
            if (jobStatus == JobStatus.CANCELED) {
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                return FutureUtils.completedExceptionally(
                        new FlinkJobTerminatedWithoutCancellationException(jobId, jobStatus));
            }
        }

        log.debug("Dispatcher is unable to cancel job {}: not found", jobId);
        return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
    }

    @Override
    public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
        CompletableFuture<ResourceOverview> taskManagerOverviewFuture =
                runResourceManagerCommand(
                        resourceManagerGateway ->
                                resourceManagerGateway.requestResourceOverview(timeout));

        final List<CompletableFuture<Optional<JobStatus>>> optionalJobInformation =
                queryJobMastersForInformation(
                        jobManagerRunner -> jobManagerRunner.requestJobStatus(timeout));

        CompletableFuture<Collection<Optional<JobStatus>>> allOptionalJobsFuture =
                FutureUtils.combineAll(optionalJobInformation);

        CompletableFuture<Collection<JobStatus>> allJobsFuture =
                allOptionalJobsFuture.thenApply(this::flattenOptionalCollection);

        final JobsOverview completedJobsOverview = executionGraphInfoStore.getStoredJobsOverview();

        return allJobsFuture.thenCombine(
                taskManagerOverviewFuture,
                (Collection<JobStatus> runningJobsStatus, ResourceOverview resourceOverview) -> {
                    final JobsOverview allJobsOverview =
                            JobsOverview.create(runningJobsStatus).combine(completedJobsOverview);
                    return new ClusterOverview(resourceOverview, allJobsOverview);
                });
    }

    @Override
    public CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(Time timeout) {
        List<CompletableFuture<Optional<JobDetails>>> individualOptionalJobDetails =
                queryJobMastersForInformation(
                        jobManagerRunner -> jobManagerRunner.requestJobDetails(timeout));

        CompletableFuture<Collection<Optional<JobDetails>>> optionalCombinedJobDetails =
                FutureUtils.combineAll(individualOptionalJobDetails);

        CompletableFuture<Collection<JobDetails>> combinedJobDetails =
                optionalCombinedJobDetails.thenApply(this::flattenOptionalCollection);

        final Collection<JobDetails> completedJobDetails =
                executionGraphInfoStore.getAvailableJobDetails();

        return combinedJobDetails.thenApply(
                (Collection<JobDetails> runningJobDetails) -> {
                    final Map<JobID, JobDetails> deduplicatedJobs = new HashMap<>();

                    completedJobDetails.forEach(job -> deduplicatedJobs.put(job.getJobId(), job));
                    runningJobDetails.forEach(job -> deduplicatedJobs.put(job.getJobId(), job));

                    return new MultipleJobsDetails(new HashSet<>(deduplicatedJobs.values()));
                });
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(JobID jobId, Time timeout) {
        Optional<JobManagerRunner> maybeJob = getJobManagerRunner(jobId);
        return maybeJob.map(job -> job.requestJobStatus(timeout))
                .orElseGet(
                        () -> {
                            // is it a completed job?
                            final JobDetails jobDetails =
                                    executionGraphInfoStore.getAvailableJobDetails(jobId);
                            if (jobDetails == null) {
                                return FutureUtils.completedExceptionally(
                                        new FlinkJobNotFoundException(jobId));
                            } else {
                                return CompletableFuture.completedFuture(jobDetails.getStatus());
                            }
                        });
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestExecutionGraphInfo(
            JobID jobId, Time timeout) {
        Optional<JobManagerRunner> maybeJob = getJobManagerRunner(jobId);
        return maybeJob.map(job -> job.requestJob(timeout))
                .orElse(FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId)))
                .exceptionally(t -> getExecutionGraphInfoFromStore(t, jobId));
    }

    private ExecutionGraphInfo getExecutionGraphInfoFromStore(Throwable t, JobID jobId) {
        // check whether it is a completed job
        final ExecutionGraphInfo executionGraphInfo = executionGraphInfoStore.get(jobId);
        if (executionGraphInfo == null) {
            throw new CompletionException(ExceptionUtils.stripCompletionException(t));
        } else {
            return executionGraphInfo;
        }
    }

    @Override
    public CompletableFuture<CheckpointStatsSnapshot> requestCheckpointStats(
            JobID jobId, Time timeout) {
        return performOperationOnJobMasterGateway(
                        jobId, gateway -> gateway.requestCheckpointStats(timeout))
                .exceptionally(
                        t ->
                                getExecutionGraphInfoFromStore(t, jobId)
                                        .getArchivedExecutionGraph()
                                        .getCheckpointStatsSnapshot());
    }

    @Override
    public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
        if (!jobManagerRunnerRegistry.isRegistered(jobId)) {
            final ExecutionGraphInfo executionGraphInfo = executionGraphInfoStore.get(jobId);

            if (executionGraphInfo == null) {
                return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
            } else {
                return CompletableFuture.completedFuture(
                        JobResult.createFrom(executionGraphInfo.getArchivedExecutionGraph()));
            }
        }

        final JobManagerRunner jobManagerRunner = jobManagerRunnerRegistry.get(jobId);
        return jobManagerRunner
                .getResultFuture()
                .thenApply(
                        jobManagerRunnerResult ->
                                JobResult.createFrom(
                                        jobManagerRunnerResult
                                                .getExecutionGraphInfo()
                                                .getArchivedExecutionGraph()));
    }

    @Override
    public CompletableFuture<Collection<String>> requestMetricQueryServiceAddresses(Time timeout) {
        if (metricServiceQueryAddress != null) {
            return CompletableFuture.completedFuture(
                    Collections.singleton(metricServiceQueryAddress));
        } else {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
    }

    @Override
    public CompletableFuture<Collection<Tuple2<ResourceID, String>>>
            requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
        return runResourceManagerCommand(
                resourceManagerGateway ->
                        resourceManagerGateway.requestTaskManagerMetricQueryServiceAddresses(
                                timeout));
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Time timeout) {
        int stackTraceMaxDepth = configuration.get(ClusterOptions.THREAD_DUMP_STACKTRACE_MAX_DEPTH);
        return CompletableFuture.completedFuture(ThreadDumpInfo.dumpAndCreate(stackTraceMaxDepth));
    }

    @Override
    public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
        return CompletableFuture.completedFuture(blobServer.getPort());
    }

    @Override
    public CompletableFuture<String> triggerCheckpoint(JobID jobID, Time timeout) {
        return performOperationOnJobMasterGateway(
                jobID, gateway -> gateway.triggerCheckpoint(timeout));
    }

    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            AsynchronousJobOperationKey operationKey, CheckpointType checkpointType, Time timeout) {
        return dispatcherCachedOperationsHandler.triggerCheckpoint(
                operationKey, checkpointType, timeout);
    }

    @Override
    public CompletableFuture<OperationResult<Long>> getTriggeredCheckpointStatus(
            AsynchronousJobOperationKey operationKey) {
        return dispatcherCachedOperationsHandler.getCheckpointStatus(operationKey);
    }

    @Override
    public CompletableFuture<Long> triggerCheckpointAndGetCheckpointID(
            final JobID jobID, final CheckpointType checkpointType, final Time timeout) {
        return performOperationOnJobMasterGateway(
                jobID,
                gateway ->
                        gateway.triggerCheckpoint(checkpointType, timeout)
                                .thenApply(CompletedCheckpoint::getCheckpointID));
    }

    @Override
    public CompletableFuture<Acknowledge> triggerSavepoint(
            final AsynchronousJobOperationKey operationKey,
            final String targetDirectory,
            SavepointFormatType formatType,
            final TriggerSavepointMode savepointMode,
            final Time timeout) {
        return dispatcherCachedOperationsHandler.triggerSavepoint(
                operationKey, targetDirectory, formatType, savepointMode, timeout);
    }

    @Override
    public CompletableFuture<String> triggerSavepointAndGetLocation(
            JobID jobId,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            Time timeout) {
        return performOperationOnJobMasterGateway(
                jobId,
                gateway ->
                        gateway.triggerSavepoint(
                                targetDirectory,
                                savepointMode.isTerminalMode(),
                                formatType,
                                timeout));
    }

    @Override
    public CompletableFuture<OperationResult<String>> getTriggeredSavepointStatus(
            AsynchronousJobOperationKey operationKey) {
        return dispatcherCachedOperationsHandler.getSavepointStatus(operationKey);
    }

    @Override
    public CompletableFuture<Acknowledge> stopWithSavepoint(
            AsynchronousJobOperationKey operationKey,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            final Time timeout) {
        return dispatcherCachedOperationsHandler.stopWithSavepoint(
                operationKey, targetDirectory, formatType, savepointMode, timeout);
    }

    @Override
    public CompletableFuture<String> stopWithSavepointAndGetLocation(
            final JobID jobId,
            final String targetDirectory,
            final SavepointFormatType formatType,
            final TriggerSavepointMode savepointMode,
            final Time timeout) {
        return performOperationOnJobMasterGateway(
                jobId,
                gateway ->
                        gateway.stopWithSavepoint(
                                targetDirectory,
                                formatType,
                                savepointMode.isTerminalMode(),
                                timeout));
    }

    @Override
    public CompletableFuture<Acknowledge> shutDownCluster() {
        return shutDownCluster(ApplicationStatus.SUCCEEDED);
    }

    @Override
    public CompletableFuture<Acknowledge> shutDownCluster(
            final ApplicationStatus applicationStatus) {
        shutDownFuture.complete(applicationStatus);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            JobID jobId,
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            Time timeout) {
        return performOperationOnJobMasterGateway(
                jobId,
                gateway ->
                        gateway.deliverCoordinationRequestToCoordinator(
                                operatorId, serializedRequest, timeout));
    }

    @Override
    public CompletableFuture<Void> reportJobClientHeartbeat(
            JobID jobId, long expiredTimestamp, Time timeout) {
        if (!getJobManagerRunner(jobId).isPresent()) {
            log.warn("Fail to find job {} for client.", jobId);
        } else {
            log.debug(
                    "Job {} receives client's heartbeat which expiredTimestamp is {}.",
                    jobId,
                    expiredTimestamp);
            jobClientExpiredTimestamp.put(jobId, expiredTimestamp);
        }
        return FutureUtils.completedVoidFuture();
    }

    private void checkJobClientAliveness() {
        setClientHeartbeatTimeoutForInitializedJob();

        long currentTimestamp = System.currentTimeMillis();
        Iterator<Map.Entry<JobID, Long>> iterator = jobClientExpiredTimestamp.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<JobID, Long> entry = iterator.next();
            JobID jobID = entry.getKey();
            long expiredTimestamp = entry.getValue();

            if (!getJobManagerRunner(jobID).isPresent()) {
                iterator.remove();
            } else if (expiredTimestamp <= currentTimestamp) {
                log.warn(
                        "The heartbeat from the job client is timeout and cancel the job {}. "
                                + "You can adjust the heartbeat interval "
                                + "by 'client.heartbeat.interval' and the timeout "
                                + "by 'client.heartbeat.timeout'",
                        jobID);
                cancelJob(jobID, webTimeout);
            }
        }
    }

    @Override
    public CompletableFuture<JobResourceRequirements> requestJobResourceRequirements(JobID jobId) {
        return performOperationOnJobMasterGateway(
                jobId, JobMasterGateway::requestJobResourceRequirements);
    }

    @Override
    public CompletableFuture<Acknowledge> updateJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) {
        if (!pendingJobResourceRequirementsUpdates.add(jobId)) {
            return FutureUtils.completedExceptionally(
                    new RestHandlerException(
                            "Another update to the job [%s] resource requirements is in progress.",
                            HttpResponseStatus.CONFLICT));
        }
        return performOperationOnJobMasterGateway(jobId, gateway -> gateway.requestJob(webTimeout))
                .thenApply(
                        job -> {
                            final Map<JobVertexID, Integer> maxParallelismPerJobVertex =
                                    new HashMap<>();
                            for (ArchivedExecutionJobVertex vertex :
                                    job.getArchivedExecutionGraph().getVerticesTopologically()) {
                                maxParallelismPerJobVertex.put(
                                        vertex.getJobVertexId(), vertex.getMaxParallelism());
                            }
                            return maxParallelismPerJobVertex;
                        })
                .thenAccept(
                        maxParallelismPerJobVertex ->
                                validateMaxParallelism(
                                        jobResourceRequirements, maxParallelismPerJobVertex))
                .thenRunAsync(
                        () -> {
                            try {
                                jobGraphWriter.putJobResourceRequirements(
                                        jobId, jobResourceRequirements);
                            } catch (Exception e) {
                                throw new CompletionException(
                                        new RestHandlerException(
                                                "The resource requirements could not be persisted and have not been applied. Please retry.",
                                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                e));
                            }
                        },
                        getIoExecutor(jobId))
                .thenComposeAsync(
                        ignored ->
                                performOperationOnJobMasterGateway(
                                        jobId,
                                        jobMasterGateway ->
                                                jobMasterGateway.updateJobResourceRequirements(
                                                        jobResourceRequirements)),
                        getMainThreadExecutor(jobId))
                .whenComplete(
                        (ack, error) -> {
                            if (error != null) {
                                log.debug(
                                        "Failed to update requirements for job {}.", jobId, error);
                            }
                            pendingJobResourceRequirementsUpdates.remove(jobId);
                        });
    }

    private static void validateMaxParallelism(
            JobResourceRequirements jobResourceRequirements,
            Map<JobVertexID, Integer> maxParallelismPerJobVertex) {

        final List<String> validationErrors =
                JobResourceRequirements.validate(
                        jobResourceRequirements, maxParallelismPerJobVertex);

        if (!validationErrors.isEmpty()) {
            throw new CompletionException(
                    new RestHandlerException(
                            validationErrors.stream()
                                    .collect(Collectors.joining(System.lineSeparator())),
                            HttpResponseStatus.BAD_REQUEST));
        }
    }

    private void setClientHeartbeatTimeoutForInitializedJob() {
        Iterator<Map.Entry<JobID, Long>> iterator =
                uninitializedJobClientHeartbeatTimeout.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<JobID, Long> entry = iterator.next();
            JobID jobID = entry.getKey();
            Optional<JobManagerRunner> jobManagerRunnerOptional = getJobManagerRunner(jobID);
            if (!jobManagerRunnerOptional.isPresent()) {
                iterator.remove();
            } else if (jobManagerRunnerOptional.get().isInitialized()) {
                jobClientExpiredTimestamp.put(jobID, System.currentTimeMillis() + entry.getValue());
                iterator.remove();
            }
        }
    }

    private void registerJobManagerRunnerTerminationFuture(
            JobID jobId, CompletableFuture<Void> jobManagerRunnerTerminationFuture) {
        Preconditions.checkState(!jobManagerRunnerTerminationFutures.containsKey(jobId));
        jobManagerRunnerTerminationFutures.put(jobId, jobManagerRunnerTerminationFuture);

        // clean up the pending termination future
        jobManagerRunnerTerminationFuture.thenRunAsync(
                () -> {
                    final CompletableFuture<Void> terminationFuture =
                            jobManagerRunnerTerminationFutures.remove(jobId);

                    //noinspection ObjectEquality
                    if (terminationFuture != null
                            && terminationFuture != jobManagerRunnerTerminationFuture) {
                        jobManagerRunnerTerminationFutures.put(jobId, terminationFuture);
                    }
                },
                getMainThreadExecutor(jobId));
    }

    // 资源清理分两级，由上面收敛时算出的 CleanupJobState 决定：
    //   · globalCleanup —— 清 HA 目录、BlobServer(jar/blob)、检查点与保存点（savepoint）等全局数据，
    //     并把 JobResultStore 中的条目从 dirty 标记为 clean（标记 clean 才算真正"清干净"）；
    //   · localCleanup —— 只清 JobManagerRunner 自己占用的本地资源。
    // 清理失败只记告警不致命（见 logCleanupErrorWarning），避免"清不掉"拖垮整个集群。
    private CompletableFuture<Void> removeJob(JobID jobId, CleanupJobState cleanupJobState) {
        if (cleanupJobState.isGlobalCleanup()) {
            return globalResourceCleaner
                    .cleanupAsync(jobId)
                    .thenCompose(unused -> jobResultStore.markResultAsCleanAsync(jobId))
                    .handle(
                            (unusedVoid, e) -> {
                                if (e == null) {
                                    log.debug(
                                            "Cleanup for the job '{}' has finished. Job has been marked as clean.",
                                            jobId);
                                } else {
                                    log.warn(
                                            "Could not properly mark job {} result as clean.",
                                            jobId,
                                            e);
                                }
                                return null;
                            })
                    .thenRunAsync(
                            () ->
                                    runPostJobGloballyTerminated(
                                            jobId, cleanupJobState.getJobStatus()),
                            getMainThreadExecutor(jobId));
        } else {
            return localResourceCleaner.cleanupAsync(jobId);
        }
    }

    protected void runPostJobGloballyTerminated(JobID jobId, JobStatus jobStatus) {
        // no-op: we need to provide this method to enable the MiniDispatcher implementation to do
        // stuff after the job is cleaned up
    }

    /** Terminate all currently running {@link JobManagerRunner}s. */
    private void terminateRunningJobs() {
        log.info("Stopping all currently running jobs of dispatcher {}.", getAddress());

        final Set<JobID> jobsToRemove = jobManagerRunnerRegistry.getRunningJobIds();

        for (JobID jobId : jobsToRemove) {
            terminateJob(jobId);
        }
    }

    private void terminateJob(JobID jobId) {
        if (jobManagerRunnerRegistry.isRegistered(jobId)) {
            final JobManagerRunner jobManagerRunner = jobManagerRunnerRegistry.get(jobId);
            jobManagerRunner.closeAsync();
        }
    }

    private CompletableFuture<Void> terminateRunningJobsAndGetTerminationFuture() {
        terminateRunningJobs();
        final Collection<CompletableFuture<Void>> values =
                jobManagerRunnerTerminationFutures.values();
        return FutureUtils.completeAll(values);
    }

    protected void onFatalError(Throwable throwable) {
        fatalErrorHandler.onFatalError(throwable);
    }

    // ★★ 作业终态收敛点：先把归档后的 ExecutionGraph 写进 executionGraphInfoStore
    //    （Web UI / REST 结束后仍能查到作业信息），再按"是否全局终态"决定清理级别：
    //      · 非全局终态（如 SUSPENDED，正在故障重启）→ localCleanup：只清本作业本地资源，
    //        作业还要被恢复，绝不能清 HA 目录 / 检查点（checkpoint）等全局数据；
    //      · 全局终态（FINISHED / CANCELED / FAILED）→ 归档到 HistoryServer，并把 JobResultStore
    //        写成 dirty 条目，随后走 globalCleanup（见 removeJob）。
    @VisibleForTesting
    protected CompletableFuture<CleanupJobState> jobReachedTerminalState(
            ExecutionGraphInfo executionGraphInfo) {
        final ArchivedExecutionGraph archivedExecutionGraph =
                executionGraphInfo.getArchivedExecutionGraph();
        final JobStatus terminalJobStatus = archivedExecutionGraph.getState();
        Preconditions.checkArgument(
                terminalJobStatus.isTerminalState(),
                "Job %s is in state %s which is not terminal.",
                archivedExecutionGraph.getJobID(),
                terminalJobStatus);

        // the failureInfo contains the reason for why job was failed/suspended, but for
        // finished/canceled jobs it may contain the last cause of a restart (if there were any)
        // for finished/canceled jobs we don't want to print it because it is misleading
        final boolean isFailureInfoRelatedToJobTermination =
                terminalJobStatus == JobStatus.SUSPENDED || terminalJobStatus == JobStatus.FAILED;

        if (archivedExecutionGraph.getFailureInfo() != null
                && isFailureInfoRelatedToJobTermination) {
            log.info(
                    "Job {} reached terminal state {}.\n{}",
                    archivedExecutionGraph.getJobID(),
                    terminalJobStatus,
                    archivedExecutionGraph.getFailureInfo().getExceptionAsString().trim());
        } else {
            log.info(
                    "Job {} reached terminal state {}.",
                    archivedExecutionGraph.getJobID(),
                    terminalJobStatus);
        }

        writeToExecutionGraphInfoStore(executionGraphInfo);

        if (!terminalJobStatus.isGloballyTerminalState()) {
            return CompletableFuture.completedFuture(
                    CleanupJobState.localCleanup(terminalJobStatus));
        }

        // do not create an archive for suspended jobs, as this would eventually lead to
        // multiple archive attempts which we currently do not support
        CompletableFuture<Acknowledge> archiveFuture =
                archiveExecutionGraphToHistoryServer(executionGraphInfo);

        return archiveFuture.thenCompose(
                ignored -> registerGloballyTerminatedJobInJobResultStore(executionGraphInfo));
    }

    // 往 JobResultStore 落一条 "dirty" 结果条目：它是"作业已结束但清理尚未完成"的书签，
    // 也是 submitJob 判断"该作业是否已处于全局终态"（isInGloballyTerminalState）的依据，
    // 同时让新上任的 Dispatcher 知道还有哪些作业需要重试清理。
    // 写失败要升级为致命错误，否则作业结果可能丢失、导致同一作业被重复提交。
    private CompletableFuture<CleanupJobState> registerGloballyTerminatedJobInJobResultStore(
            ExecutionGraphInfo executionGraphInfo) {
        final JobID jobId = executionGraphInfo.getJobId();

        final AccessExecutionGraph archivedExecutionGraph =
                executionGraphInfo.getArchivedExecutionGraph();

        final JobStatus terminalJobStatus = archivedExecutionGraph.getState();
        Preconditions.checkArgument(
                terminalJobStatus.isGloballyTerminalState(),
                "Job %s is in state %s which is not globally terminal.",
                jobId,
                terminalJobStatus);

        return jobResultStore
                .hasCleanJobResultEntryAsync(jobId)
                .thenCompose(
                        hasCleanJobResultEntry ->
                                createDirtyJobResultEntryIfMissingAsync(
                                        archivedExecutionGraph, hasCleanJobResultEntry))
                .handleAsync(
                        (ignored, error) -> {
                            if (error != null) {
                                fatalErrorHandler.onFatalError(
                                        new FlinkException(
                                                String.format(
                                                        "The job %s couldn't be marked as pre-cleanup finished in JobResultStore.",
                                                        executionGraphInfo.getJobId()),
                                                error));
                            }
                            return CleanupJobState.globalCleanup(terminalJobStatus);
                        },
                        getMainThreadExecutor(jobId));
    }

    /**
     * Creates a dirty entry in the {@link #jobResultStore} if there's no entry at all for the given
     * {@code executionGraph} in the {@code JobResultStore}.
     *
     * @param executionGraph The {@link AccessExecutionGraph} for which the {@link JobResult} shall
     *     be persisted.
     * @param hasCleanJobResultEntry The decision the dirty entry check is based on.
     * @return {@code CompletableFuture} that completes as soon as the entry exists.
     */
    private CompletableFuture<Void> createDirtyJobResultEntryIfMissingAsync(
            AccessExecutionGraph executionGraph, boolean hasCleanJobResultEntry) {
        final JobID jobId = executionGraph.getJobID();
        if (hasCleanJobResultEntry) {
            log.warn("Job {} is already marked as clean but clean up was triggered again.", jobId);
            return FutureUtils.completedVoidFuture();
        } else {
            return jobResultStore
                    .hasDirtyJobResultEntryAsync(jobId)
                    .thenCompose(
                            hasDirtyJobResultEntry ->
                                    createDirtyJobResultEntryAsync(
                                            executionGraph, hasDirtyJobResultEntry));
        }
    }

    /**
     * Creates a dirty entry in the {@link #jobResultStore} based on the passed {@code
     * hasDirtyJobResultEntry} flag.
     *
     * @param executionGraph The {@link AccessExecutionGraph} that is used to generate the entry.
     * @param hasDirtyJobResultEntry The decision the entry creation is based on.
     * @return {@code CompletableFuture} that completes as soon as the entry exists.
     */
    private CompletableFuture<Void> createDirtyJobResultEntryAsync(
            AccessExecutionGraph executionGraph, boolean hasDirtyJobResultEntry) {
        if (hasDirtyJobResultEntry) {
            return FutureUtils.completedVoidFuture();
        }

        return jobResultStore.createDirtyResultAsync(
                new JobResultEntry(JobResult.createFrom(executionGraph)));
    }

    private void writeToExecutionGraphInfoStore(ExecutionGraphInfo executionGraphInfo) {
        try {
            executionGraphInfoStore.put(executionGraphInfo);
        } catch (IOException e) {
            log.info(
                    "Could not store completed job {}({}).",
                    executionGraphInfo.getArchivedExecutionGraph().getJobName(),
                    executionGraphInfo.getArchivedExecutionGraph().getJobID(),
                    e);
        }
    }

    private CompletableFuture<Acknowledge> archiveExecutionGraphToHistoryServer(
            ExecutionGraphInfo executionGraphInfo) {

        return historyServerArchivist
                .archiveExecutionGraph(executionGraphInfo)
                .handleAsync(
                        (Acknowledge ignored, Throwable throwable) -> {
                            if (throwable != null) {
                                log.info(
                                        "Could not archive completed job {}({}) to the history server.",
                                        executionGraphInfo.getArchivedExecutionGraph().getJobName(),
                                        executionGraphInfo.getArchivedExecutionGraph().getJobID(),
                                        throwable);
                            }
                            return Acknowledge.get();
                        },
                        getMainThreadExecutor(
                                executionGraphInfo.getArchivedExecutionGraph().getJobID()));
    }

    private void jobMasterFailed(JobID jobId, Throwable cause) {
        // we fail fatally in case of a JobMaster failure in order to restart the
        // dispatcher to recover the jobs again. This only works in HA mode, though
        onFatalError(
                new FlinkException(String.format("JobMaster for job %s failed.", jobId), cause));
    }

    /** Ensures that the JobMasterGateway is available. */
    private CompletableFuture<JobMasterGateway> getJobMasterGateway(JobID jobId) {
        if (!jobManagerRunnerRegistry.isRegistered(jobId)) {
            return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
        }

        final JobManagerRunner job = jobManagerRunnerRegistry.get(jobId);
        if (!job.isInitialized()) {
            return FutureUtils.completedExceptionally(
                    new UnavailableDispatcherOperationException(
                            "Unable to get JobMasterGateway for initializing job. "
                                    + "The requested operation is not available while the JobManager is initializing."));
        }
        return job.getJobMasterGateway();
    }

    private <T> CompletableFuture<T> performOperationOnJobMasterGateway(
            JobID jobId, Function<JobMasterGateway, CompletableFuture<T>> operation) {
        return getJobMasterGateway(jobId).thenCompose(operation);
    }

    private CompletableFuture<ResourceManagerGateway> getResourceManagerGateway() {
        return resourceManagerGatewayRetriever.getFuture();
    }

    private Optional<JobManagerRunner> getJobManagerRunner(JobID jobId) {
        return jobManagerRunnerRegistry.isRegistered(jobId)
                ? Optional.of(jobManagerRunnerRegistry.get(jobId))
                : Optional.empty();
    }

    private <T> CompletableFuture<T> runResourceManagerCommand(
            Function<ResourceManagerGateway, CompletableFuture<T>> resourceManagerCommand) {
        return getResourceManagerGateway()
                .thenApply(resourceManagerCommand)
                .thenCompose(Function.identity());
    }

    private <T> List<T> flattenOptionalCollection(Collection<Optional<T>> optionalCollection) {
        return optionalCollection.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    @Nonnull
    private <T> List<CompletableFuture<Optional<T>>> queryJobMastersForInformation(
            Function<JobManagerRunner, CompletableFuture<T>> queryFunction) {

        List<CompletableFuture<Optional<T>>> optionalJobInformation =
                new ArrayList<>(jobManagerRunnerRegistry.size());

        for (JobManagerRunner job : jobManagerRunnerRegistry.getJobManagerRunners()) {
            final CompletableFuture<Optional<T>> queryResult =
                    queryFunction
                            .apply(job)
                            .handle((T value, Throwable t) -> Optional.ofNullable(value));
            optionalJobInformation.add(queryResult);
        }
        return optionalJobInformation;
    }

    private CompletableFuture<Void> waitForTerminatingJob(
            JobID jobId, JobGraph jobGraph, ThrowingConsumer<JobGraph, ?> action) {
        final CompletableFuture<Void> jobManagerTerminationFuture =
                getJobTerminationFuture(jobId)
                        .exceptionally(
                                (Throwable throwable) -> {
                                    throw new CompletionException(
                                            new DispatcherException(
                                                    String.format(
                                                            "Termination of previous JobManager for job %s failed. Cannot submit job under the same job id.",
                                                            jobId),
                                                    throwable));
                                });

        return FutureUtils.thenAcceptAsyncIfNotDone(
                jobManagerTerminationFuture,
                getMainThreadExecutor(jobId),
                FunctionUtils.uncheckedConsumer(
                        (ignored) -> {
                            jobManagerRunnerTerminationFutures.remove(jobId);
                            action.accept(jobGraph);
                        }));
    }

    @VisibleForTesting
    CompletableFuture<Void> getJobTerminationFuture(JobID jobId) {
        return jobManagerRunnerTerminationFutures.getOrDefault(
                jobId, CompletableFuture.completedFuture(null));
    }

    private void registerDispatcherMetrics(MetricGroup jobManagerMetricGroup) {
        jobManagerMetricGroup.gauge(
                MetricNames.NUM_RUNNING_JOBS,
                // metrics can be called from anywhere and therefore, have to run without the main
                // thread safeguard being triggered. For metrics, we can afford to be not 100%
                // accurate
                () -> (long) jobManagerRunnerRegistry.getWrappedDelegate().size());
    }

    public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
        return CompletableFuture.runAsync(() -> terminateJob(jobId), getMainThreadExecutor(jobId));
    }

    private void applyParallelismOverrides(JobGraph jobGraph) {
        Map<String, String> overrides = new HashMap<>();
        overrides.putAll(configuration.get(PipelineOptions.PARALLELISM_OVERRIDES));
        overrides.putAll(jobGraph.getJobConfiguration().get(PipelineOptions.PARALLELISM_OVERRIDES));
        for (JobVertex vertex : jobGraph.getVertices()) {
            String override = overrides.get(vertex.getID().toHexString());
            if (override != null) {
                int currentParallelism = vertex.getParallelism();
                int overrideParallelism = Integer.parseInt(override);
                log.info(
                        "Changing job vertex {} parallelism from {} to {}",
                        vertex.getID(),
                        currentParallelism,
                        overrideParallelism);
                vertex.setParallelism(overrideParallelism);
            }
        }
    }

    private Executor getIoExecutor(JobID jobID) {
        // todo: consider caching
        return MdcUtils.scopeToJob(jobID, ioExecutor);
    }
}
