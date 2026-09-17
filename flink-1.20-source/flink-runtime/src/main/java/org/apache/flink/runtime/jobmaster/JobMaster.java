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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistContext;
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.blocklist.BlocklistUtils;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatReceiver;
import org.apache.flink.runtime.heartbeat.HeartbeatSender;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.BlocklistDeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.JobShuffleContextImpl;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation.ResolutionMode;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.TaskStateSnapshot.deserializeTaskStateSnapshot;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

// ============================================================================
// 【JobMaster 是什么】单个作业运行期的"大脑" —— 一个作业一个实例。
// ----------------------------------------------------------------------------
// 职责边界：它只管"一个已经确定下来的作业怎么跑起来"——向 ResourceManager 申请
// 槽位(slot)、把执行图铺到 TaskManager 上、协调检查点、收集执行状态，并把 JobStatus
// 的变化上报给上游的 Dispatcher（由 Dispatcher 决定作业记录的最终清理）。
// 它【不管】"作业从哪来"：JobGraph 的解析、提交、去重都在 Dispatcher 侧完成，
// JobGraph 是作为构造参数被塞进来的。
// 生命周期约束：一个 JobMaster 实例自始至终只服务这一个 JobGraph，
// 因此它的绝大多数字段都不需要按 JobID 分桶，也不存在"切换作业"的语义。
// 对外身份有二：既是 RPC 端点（实现 JobMasterGateway，供 TaskManager / Dispatcher /
// ResourceManager 远程调用），也是进程内服务（实现 JobMasterService）。
// ★ 最容易误解的一点：ExecutionGraph 在本类【构造函数】里就已经建好了，
//   见构造期的 createScheduler() 与 schedulerNG 赋值处。
//   start()/onStart() 只是"开机"，不是"建图"。
// ============================================================================
/**
 * JobMaster implementation. The job master is responsible for the execution of a single {@link
 * JobGraph}.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 *
 * <ul>
 *   <li>{@link #updateTaskExecutionState} updates the task execution state for given task
 * </ul>
 */
public class JobMaster extends FencedRpcEndpoint<JobMasterId>
        implements JobMasterGateway, JobMasterService {

    /** Default names for Flink's distributed components. */
    public static final String JOB_MANAGER_NAME = "jobmanager";

    // ------------------------------------------------------------------------

    private final JobMasterConfiguration jobMasterConfiguration;

    private final ResourceID resourceId;

    private final JobGraph jobGraph;

    private final Time rpcTimeout;

    private final HighAvailabilityServices highAvailabilityServices;

    private final BlobWriter blobWriter;

    private final HeartbeatServices heartbeatServices;

    private final ScheduledExecutorService futureExecutor;

    private final Executor ioExecutor;

    private final OnCompletionActions jobCompletionActions;

    private final FatalErrorHandler fatalErrorHandler;

    private final ClassLoader userCodeLoader;

    private final SlotPoolService slotPoolService;

    private final long initializationTimestamp;

    private final boolean retrieveTaskManagerHostName;

    // --------- ResourceManager --------

    private final LeaderRetrievalService resourceManagerLeaderRetriever;

    // --------- TaskManagers --------

    private final Map<ResourceID, TaskManagerRegistration> registeredTaskManagers;

    private final ShuffleMaster<?> shuffleMaster;

    // --------- Scheduler --------

    private final SchedulerNG schedulerNG;

    private final JobManagerJobStatusListener jobStatusListener;

    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    // -------- Misc ---------

    private final Map<String, Object> accumulators;

    private final JobMasterPartitionTracker partitionTracker;

    private final ExecutionDeploymentTracker executionDeploymentTracker;
    private final ExecutionDeploymentReconciler executionDeploymentReconciler;
    private final Collection<FailureEnricher> failureEnrichers;

    // -------- Mutable fields ---------

    @Nullable private ResourceManagerAddress resourceManagerAddress;

    @Nullable private ResourceManagerConnection resourceManagerConnection;

    @Nullable private EstablishedResourceManagerConnection establishedResourceManagerConnection;

    private HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport>
            taskManagerHeartbeatManager;

    private HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;

    private final BlocklistHandler blocklistHandler;

    private final Map<ResultPartitionID, PartitionWithMetrics> fetchedPartitionsWithMetrics =
            new HashMap<>();

    /**
     * A flag that indicates whether to fetch and retain partitions on task managers. This will
     * apply to future TaskManager registrations as well as already registered TaskManagers. The
     * flag will be set to true when starting batch job recovery and set to false after all required
     * partitions, as defined in {@code requireToFetchPartitions}, are either fetched or when a
     * timeout occurs.
     */
    private boolean fetchAndRetainPartitions = false;

    private Set<ResultPartitionID> partitionsToFetch;

    private CompletableFuture<Collection<PartitionWithMetrics>> fetchPartitionsFuture;

    // ------------------------------------------------------------------------

    // ★【构造期就是重活期】构造函数不只是做依赖注入，它在这里一口气把运行期需要的
    //   重组件全部装配好，其中最关键的就是 schedulerNG（内部持有 ExecutionGraph）。
    //   构造顺序即依赖顺序：
    //     第 1 步：executionDeploymentTracker / DeploymentReconciler、配置、线程池、BlobWriter
    //     第 2 步：blocklistHandler（节点黑名单）、slotPoolService（JobMaster 侧的槽位账本）
    //     第 3 步：partitionTracker（结果分区追踪）、shuffleMaster（Shuffle 服务）
    //     第 4 步：jobManagerJobMetricGroup、jobStatusListener（作业状态回调入口）
    //     第 5 步：★ createScheduler(...) —— 就在这里把 JobGraph 变成了 ExecutionGraph
    //   此时心跳管理器还只是 NoOp 实现、与 ResourceManager 的连接也尚未建立，
    //   真正的启动动作在 onStart() → startJobExecution() 里。

    // 创建 JobMaster 的内部过程就解析好了ExecutionGraph
    public JobMaster(
            RpcService rpcService,
            JobMasterId jobMasterId,
            JobMasterConfiguration jobMasterConfiguration,
            ResourceID resourceId,
            JobGraph jobGraph,
            HighAvailabilityServices highAvailabilityService,
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            JobManagerSharedServices jobManagerSharedServices,
            HeartbeatServices heartbeatServices,
            JobManagerJobMetricGroupFactory jobMetricGroupFactory,
            OnCompletionActions jobCompletionActions,
            FatalErrorHandler fatalErrorHandler,
            ClassLoader userCodeLoader,
            ShuffleMaster<?> shuffleMaster,
            PartitionTrackerFactory partitionTrackerFactory,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ExecutionDeploymentReconciler.Factory executionDeploymentReconcilerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            Collection<FailureEnricher> failureEnrichers,
            long initializationTimestamp)
            throws Exception {

        super(
                rpcService,
                RpcServiceUtils.createRandomName(JOB_MANAGER_NAME),
                jobMasterId,
                MdcUtils.asContextData(jobGraph.getJobID()));

        final ExecutionDeploymentReconciliationHandler executionStateReconciliationHandler =
                new ExecutionDeploymentReconciliationHandler() {

                    @Override
                    public void onMissingDeploymentsOf(
                            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                        log.debug(
                                "Failing deployments {} due to no longer being deployed.",
                                executionAttemptIds);
                        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                            schedulerNG.updateTaskExecutionState(
                                    new TaskExecutionState(
                                            executionAttemptId,
                                            ExecutionState.FAILED,
                                            new FlinkException(
                                                    String.format(
                                                            "Execution %s is unexpectedly no longer running on task executor %s.",
                                                            executionAttemptId, host))));
                        }
                    }

                    @Override
                    public void onUnknownDeploymentsOf(
                            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                        log.debug(
                                "Canceling left-over deployments {} on task executor {}.",
                                executionAttemptIds,
                                host);
                        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                            TaskManagerRegistration taskManagerRegistration =
                                    registeredTaskManagers.get(host);
                            if (taskManagerRegistration != null) {
                                taskManagerRegistration
                                        .getTaskExecutorGateway()
                                        .cancelTask(executionAttemptId, rpcTimeout);
                            }
                        }
                    }
                };
        final String jobName = jobGraph.getName();
        final JobID jid = jobGraph.getJobID();

        log.info("Initializing job '{}' ({}).", jobName, jid);

        this.executionDeploymentTracker = executionDeploymentTracker;
        this.executionDeploymentReconciler =
                executionDeploymentReconcilerFactory.create(executionStateReconciliationHandler);

        this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
        this.resourceId = checkNotNull(resourceId);
        this.jobGraph = checkNotNull(jobGraph);
        this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
        this.highAvailabilityServices = checkNotNull(highAvailabilityService);
        this.blobWriter = jobManagerSharedServices.getBlobWriter();
        this.futureExecutor =
                MdcUtils.scopeToJob(jid, jobManagerSharedServices.getFutureExecutor());
        this.ioExecutor = MdcUtils.scopeToJob(jid, jobManagerSharedServices.getIoExecutor());
        this.jobCompletionActions = checkNotNull(jobCompletionActions);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.initializationTimestamp = initializationTimestamp;
        this.retrieveTaskManagerHostName =
                jobMasterConfiguration
                        .getConfiguration()
                        .get(JobManagerOptions.RETRIEVE_TASK_MANAGER_HOSTNAME);

        resourceManagerLeaderRetriever =
                highAvailabilityServices.getResourceManagerLeaderRetriever();

        this.registeredTaskManagers = new HashMap<>();
        this.blocklistHandler =
                blocklistHandlerFactory.create(
                        new JobMasterBlocklistContext(),
                        this::getNodeIdOfTaskManager,
                        getMainThreadExecutor(),
                        log);

        // JobMaster 侧的"槽位账本"：记录作业声明需要多少槽位、已从 ResourceManager
        // 拿到哪些槽位、每个槽位分配给了哪个 ExecutionVertex。
        // ★ 注意它此刻只是【被创建】，还没有 start()，因此还不接受任何针对本 leader 的消息；
        //   是否启用黑名单(blocklist)决定了这里选哪个 DeclarativeSlotPoolFactory。
        this.slotPoolService =
                checkNotNull(slotPoolServiceSchedulerFactory)
                        .createSlotPoolService(
                                jid,
                                createDeclarativeSlotPoolFactory(
                                        jobMasterConfiguration.getConfiguration()));

        // 结果分区（上下游算子之间的数据通道）追踪器：作业结束/失败时由它决定
        // 释放哪些分区、把哪些分区 promote 成集群级分区（供后续作业复用）。
        // 注意传进去的是一个 ResourceID → TaskExecutorGateway 的查询函数，
        // 说明它后续要直接指挥 TaskManager 释放分区。
        this.partitionTracker =
                checkNotNull(partitionTrackerFactory)
                        .create(
                                resourceID -> {
                                    return Optional.ofNullable(
                                                    registeredTaskManagers.get(resourceID))
                                            .map(TaskManagerRegistration::getTaskExecutorGateway);
                                });

        this.shuffleMaster = checkNotNull(shuffleMaster);

        this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
        this.jobStatusListener = new JobManagerJobStatusListener();

        this.failureEnrichers = checkNotNull(failureEnrichers);

        // ★ 执行图就是在这一行、在构造期被构建出来的。
        //   调用链：本处 → createScheduler() → SlotPoolServiceSchedulerFactory.createScheduler()
        //     → DefaultSchedulerFactory.createInstance()
        //     → DefaultExecutionGraphBuilder.buildGraph()
        //   也就是说，"new JobMaster(...)" 返回时执行图已经存在，
        //   后面的 start()/startScheduling() 只是让它开始跑，而不是让它存在。
        this.schedulerNG =
                createScheduler(
                        slotPoolServiceSchedulerFactory,
                        executionDeploymentTracker,
                        jobManagerJobMetricGroup,
                        jobStatusListener);

        this.heartbeatServices = checkNotNull(heartbeatServices);
        this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
        this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

        this.resourceManagerConnection = null;
        this.establishedResourceManagerConnection = null;

        this.accumulators = new HashMap<>();
    }

    // ★ 方法名有误导性：createScheduler 不只是"造一个调度器"，它顺带把 JobGraph
    //   构建成了 ExecutionGraph —— 真正的建图发生在工厂内部
    //   （DefaultSchedulerFactory.createInstance → DefaultExecutionGraphBuilder.buildGraph）。
    //   之所以要绕一层工厂：Flink 允许替换调度实现（DefaultScheduler / AdaptiveScheduler 等），
    //   JobMaster 只面向 SchedulerNG 接口编程，不关心建图细节。
    //   传进去的 slotPoolService / shuffleMaster / partitionTracker 都是构造期刚建好的组件，
    //   之后会被 Scheduler 与 ExecutionGraph 长期持有。
    private SchedulerNG createScheduler(
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            ExecutionDeploymentTracker executionDeploymentTracker,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            JobStatusListener jobStatusListener)
            throws Exception {
        final SchedulerNG scheduler =
                slotPoolServiceSchedulerFactory.createScheduler(
                        log,
                        jobGraph,
                        ioExecutor,
                        jobMasterConfiguration.getConfiguration(),
                        slotPoolService,
                        futureExecutor,
                        userCodeLoader,
                        highAvailabilityServices.getCheckpointRecoveryFactory(),
                        rpcTimeout,
                        blobWriter,
                        jobManagerJobMetricGroup,
                        jobMasterConfiguration.getSlotRequestTimeout(),
                        shuffleMaster,
                        partitionTracker,
                        executionDeploymentTracker,
                        initializationTimestamp,
                        getMainThreadExecutor(),
                        fatalErrorHandler,
                        jobStatusListener,
                        failureEnrichers,
                        blocklistHandler::addNewBlockedNodes);

        return scheduler;
    }

    private HeartbeatManager<Void, Void> createResourceManagerHeartbeatManager(
            HeartbeatServices heartbeatServices) {
        return heartbeatServices.createHeartbeatManager(
                resourceId, new ResourceManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport>
            createTaskManagerHeartbeatManager(HeartbeatServices heartbeatServices) {
        return heartbeatServices.createHeartbeatManagerSender(
                resourceId, new TaskManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private DeclarativeSlotPoolFactory createDeclarativeSlotPoolFactory(
            Configuration configuration) {
        if (BlocklistUtils.isBlocklistEnabled(configuration)) {
            return new BlocklistDeclarativeSlotPoolFactory(blocklistHandler::isBlockedTaskManager);
        } else {
            return new DefaultDeclarativeSlotPoolFactory();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Lifecycle management
    // ----------------------------------------------------------------------------------------------

    // ★ JobMaster 自己并不重写 start()：父类 RpcEndpoint.start() 负责把 RPC 端点挂起来，
    //   挂好之后回调本方法。所以"启动 JobMaster"的实质逻辑全在 startJobExecution() 里。
    @Override
    protected void onStart() throws JobMasterException {
        try {
            startJobExecution();
        } catch (Exception e) {
            final JobMasterException jobMasterException =
                    new JobMasterException("Could not start the JobMaster.", e);
            handleJobMasterError(jobMasterException);
            throw jobMasterException;
        }
    }

    // 生命周期终点（RPC 端点关闭时的回调）：先优雅地停掉作业执行，再拆掉所有连接。
    // 具体动作见 stopJobExecution()。
    /** Suspend the job and shutdown all other services including rpc. */
    @Override
    public CompletableFuture<Void> onStop() {
        log.info(
                "Stopping the JobMaster for job '{}' ({}).",
                jobGraph.getName(),
                jobGraph.getJobID());

        // make sure there is a graceful exit
        return stopJobExecution(
                        new FlinkException(
                                String.format(
                                        "Stopping JobMaster for job '%s' (%s).",
                                        jobGraph.getName(), jobGraph.getJobID())))
                .exceptionally(
                        exception -> {
                            throw new CompletionException(
                                    new JobMasterException(
                                            "Could not properly stop the JobMaster.", exception));
                        });
    }

    // ----------------------------------------------------------------------------------------------
    // RPC methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
        schedulerNG.cancel();

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // TaskManager 上报"某次执行尝试(ExecutionAttempt)状态变化"的入口。
    // JobMaster 在此不做任何判断，直接交给 SchedulerNG，由它驱动 ExecutionGraph 的状态机
    // 以及失败恢复(failover)；返回 false 表示这个 attempt 已经不在执行图里了（例如已被取消或重启过）。
    /**
     * Updates the task execution state for a given task.
     *
     * @param taskExecutionState New task execution state for a given task
     * @return Acknowledge the task execution state update
     */
    @Override
    public CompletableFuture<Acknowledge> updateTaskExecutionState(
            final TaskExecutionState taskExecutionState) {
        FlinkException taskExecutionException;
        try {
            checkNotNull(taskExecutionState, "taskExecutionState");

            if (schedulerNG.updateTaskExecutionState(taskExecutionState)) {
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                taskExecutionException =
                        new ExecutionGraphException(
                                "The execution attempt "
                                        + taskExecutionState.getID()
                                        + " was not found.");
            }
        } catch (Exception e) {
            taskExecutionException =
                    new JobMasterException(
                            "Could not update the state of task execution for JobMaster.", e);
            handleJobMasterError(taskExecutionException);
        }
        return FutureUtils.completedExceptionally(taskExecutionException);
    }

    @Override
    public void notifyEndOfData(final ExecutionAttemptID executionAttempt) {
        schedulerNG.notifyEndOfData(executionAttempt);
    }

    @Override
    public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
            final JobVertexID vertexID, final ExecutionAttemptID executionAttempt) {

        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestNextInputSplit(vertexID, executionAttempt));
        } catch (IOException e) {
            log.warn("Error while requesting next input split", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<ExecutionState> requestPartitionState(
            final IntermediateDataSetID intermediateResultId,
            final ResultPartitionID resultPartitionId) {

        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestPartitionState(intermediateResultId, resultPartitionId));
        } catch (PartitionProducerDisposedException e) {
            log.info("Error while requesting partition state", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> disconnectTaskManager(
            final ResourceID resourceID, final Exception cause) {

        taskManagerHeartbeatManager.unmonitorTarget(resourceID);
        slotPoolService.releaseTaskManager(resourceID, cause);
        partitionTracker.stopTrackingPartitionsFor(resourceID);

        TaskManagerRegistration taskManagerRegistration = registeredTaskManagers.remove(resourceID);

        if (taskManagerRegistration != null) {
            log.info(
                    "Disconnect TaskExecutor {} because: {}",
                    resourceID.getStringWithMetadata(),
                    cause.getMessage(),
                    ExceptionUtils.returnExceptionIfUnexpected(cause.getCause()));
            ExceptionUtils.logExceptionIfExcepted(cause.getCause(), log);

            taskManagerRegistration
                    .getTaskExecutorGateway()
                    .disconnectJobManager(jobGraph.getJobID(), cause);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // TODO: This method needs a leader session ID
    @Override
    public void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            @Nullable final SerializedValue<TaskStateSnapshot> checkpointState) {
        schedulerNG.acknowledgeCheckpoint(
                jobID,
                executionAttemptID,
                checkpointId,
                checkpointMetrics,
                deserializeTaskStateSnapshot(checkpointState, getClass().getClassLoader()));
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {

        schedulerNG.reportCheckpointMetrics(
                jobID, executionAttemptID, checkpointId, checkpointMetrics);
    }

    @Override
    public void reportInitializationMetrics(
            JobID jobId, SubTaskInitializationMetrics initializationMetrics) {
        schedulerNG.reportInitializationMetrics(jobId, initializationMetrics);
    }

    // TODO: This method needs a leader session ID
    @Override
    public void declineCheckpoint(DeclineCheckpoint decline) {
        schedulerNG.declineCheckpoint(decline);
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(
            final ExecutionAttemptID task,
            final OperatorID operatorID,
            final SerializedValue<OperatorEvent> serializedEvent) {

        try {
            final OperatorEvent evt = serializedEvent.deserializeValue(userCodeLoader);
            schedulerNG.deliverOperatorEventToCoordinator(task, operatorID, evt);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OperatorID operatorID, SerializedValue<CoordinationRequest> serializedRequest) {
        try {
            final CoordinationRequest request = serializedRequest.deserializeValue(userCodeLoader);
            return schedulerNG.deliverCoordinationRequestToCoordinator(operatorID, request);
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<KvStateLocation> requestKvStateLocation(
            final JobID jobId, final String registrationName) {
        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestKvStateLocation(jobId, registrationName));
        } catch (UnknownKvStateLocation | FlinkJobNotFoundException e) {
            log.info("Error while request key-value state location", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateRegistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName,
            final KvStateID kvStateId,
            final InetSocketAddress kvStateServerAddress) {

        try {
            schedulerNG.notifyKvStateRegistered(
                    jobId,
                    jobVertexId,
                    keyGroupRange,
                    registrationName,
                    kvStateId,
                    kvStateServerAddress);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName) {
        try {
            schedulerNG.notifyKvStateUnregistered(
                    jobId, jobVertexId, keyGroupRange, registrationName);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state de-registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    // ★ TaskExecutor 把本地空闲槽位"献"给 JobMaster 的入口（JobMasterGateway 的 RPC 方法）。
    //   JobMaster 自己不实现任何槽位分配策略：它把 TaskManagerLocation 与
    //   RpcTaskManagerGateway 一起转交给 slotPoolService（槽位账本），
    //   由 SlotPool 决定是立刻分配给某个 ExecutionVertex，还是先存起来备用。
    //   查不到注册记录说明对方是过期/未注册的 TaskManager，直接拒绝。
    @Override
    public CompletableFuture<Collection<SlotOffer>> offerSlots(
            final ResourceID taskManagerId, final Collection<SlotOffer> slots, final Time timeout) {

        TaskManagerRegistration taskManagerRegistration = registeredTaskManagers.get(taskManagerId);

        if (taskManagerRegistration == null) {
            return FutureUtils.completedExceptionally(
                    new Exception("Unknown TaskManager " + taskManagerId));
        }

        final RpcTaskManagerGateway rpcTaskManagerGateway =
                new RpcTaskManagerGateway(
                        taskManagerRegistration.getTaskExecutorGateway(), getFencingToken());

        return CompletableFuture.completedFuture(
                slotPoolService.offerSlots(
                        taskManagerRegistration.getTaskManagerLocation(),
                        rpcTaskManagerGateway,
                        slots));
    }

    // 槽位分配失败的处理：让槽位账本把该 allocation 标记为失败，并回收它。
    // 若该 TaskManager 上既没有可用槽位、也没有被追踪的结果分区，
    // 就顺手把这个空 TaskManager 断开，避免白占资源。
    @Override
    public void failSlot(
            final ResourceID taskManagerId,
            final AllocationID allocationId,
            final Exception cause) {

        if (registeredTaskManagers.containsKey(taskManagerId)) {
            internalFailAllocation(taskManagerId, allocationId, cause);
        } else {
            log.warn(
                    "Cannot fail slot "
                            + allocationId
                            + " because the TaskManager "
                            + taskManagerId
                            + " is unknown.");
        }
    }

    private void internalFailAllocation(
            @Nullable ResourceID resourceId, AllocationID allocationId, Exception cause) {
        final Optional<ResourceID> resourceIdOptional =
                slotPoolService.failAllocation(resourceId, allocationId, cause);
        resourceIdOptional.ifPresent(
                taskManagerId -> {
                    if (!partitionTracker.isTrackingPartitionsFor(taskManagerId)) {
                        releaseEmptyTaskManager(taskManagerId);
                    }
                });
    }

    private void releaseEmptyTaskManager(ResourceID resourceId) {
        disconnectTaskManager(
                resourceId,
                new FlinkException(
                        String.format(
                                "No more slots registered at JobMaster %s.",
                                resourceId.getStringWithMetadata())));
    }

    // ★ TaskManager 向本 JobMaster 注册的入口。校验 jobId 之后依次做三件事：
    //   1) 解析 TaskManager 地址（是否取主机名由 RETRIEVE_TASK_MANAGER_HOSTNAME 决定）；
    //   2) 处理重复注册：sessionId 相同则幂等返回成功；sessionId 变了说明 TM 重启过，
    //      先断开旧连接再重新登记；
    //   3) RPC 连接建好之后，才把 TaskManager 写进 registeredTaskManagers，纳入心跳监控，
    //      并同步登记到槽位账本。
    //   ★ offerSlots 只对已注册的 TaskManager 生效，所以注册是"能收槽位"的前置条件。
    @Override
    public CompletableFuture<RegistrationResponse> registerTaskManager(
            final JobID jobId,
            final TaskManagerRegistrationInformation taskManagerRegistrationInformation,
            final Time timeout) {

        if (!jobGraph.getJobID().equals(jobId)) {
            log.debug(
                    "Rejecting TaskManager registration attempt because of wrong job id {}.",
                    jobId);
            return CompletableFuture.completedFuture(
                    new JMTMRegistrationRejection(
                            String.format(
                                    "The JobManager is not responsible for job %s. Maybe the TaskManager used outdated connection information.",
                                    jobId)));
        }

        final TaskManagerLocation taskManagerLocation;
        try {
            taskManagerLocation =
                    resolveTaskManagerLocation(
                            taskManagerRegistrationInformation.getUnresolvedTaskManagerLocation());
        } catch (FlinkException exception) {
            log.error("Could not accept TaskManager registration.", exception);
            return CompletableFuture.completedFuture(new RegistrationResponse.Failure(exception));
        }

        final ResourceID taskManagerId = taskManagerLocation.getResourceID();
        final UUID sessionId = taskManagerRegistrationInformation.getTaskManagerSession();
        final TaskManagerRegistration taskManagerRegistration =
                registeredTaskManagers.get(taskManagerId);

        if (taskManagerRegistration != null) {
            if (taskManagerRegistration.getSessionId().equals(sessionId)) {
                log.debug(
                        "Ignoring registration attempt of TaskManager {} with the same session id {}.",
                        taskManagerId,
                        sessionId);
                final RegistrationResponse response = new JMTMRegistrationSuccess(resourceId);
                return CompletableFuture.completedFuture(response);
            } else {
                disconnectTaskManager(
                        taskManagerId,
                        new FlinkException(
                                String.format(
                                        "A registered TaskManager %s re-registered with a new session id. This indicates a restart of the TaskManager. Closing the old connection.",
                                        taskManagerId)));
            }
        }

        CompletableFuture<RegistrationResponse> registrationResponseFuture =
                getRpcService()
                        .connect(
                                taskManagerRegistrationInformation.getTaskManagerRpcAddress(),
                                TaskExecutorGateway.class)
                        .handleAsync(
                                (TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
                                    if (throwable != null) {
                                        return new RegistrationResponse.Failure(throwable);
                                    }

                                    slotPoolService.registerTaskManager(taskManagerId);
                                    registeredTaskManagers.put(
                                            taskManagerId,
                                            TaskManagerRegistration.create(
                                                    taskManagerLocation,
                                                    taskExecutorGateway,
                                                    sessionId));

                                    // monitor the task manager as heartbeat target
                                    taskManagerHeartbeatManager.monitorTarget(
                                            taskManagerId,
                                            new TaskExecutorHeartbeatSender(taskExecutorGateway));

                                    return new JMTMRegistrationSuccess(resourceId);
                                },
                                getMainThreadExecutor());

        if (fetchAndRetainPartitions) {
            registrationResponseFuture.whenComplete(
                    (ignored, throwable) ->
                            fetchAndRetainPartitionWithMetricsOnTaskManager(taskManagerId));
        }

        return registrationResponseFuture;
    }

    @Nonnull
    private TaskManagerLocation resolveTaskManagerLocation(
            UnresolvedTaskManagerLocation unresolvedTaskManagerLocation) throws FlinkException {
        try {
            if (retrieveTaskManagerHostName) {
                return TaskManagerLocation.fromUnresolvedLocation(
                        unresolvedTaskManagerLocation, ResolutionMode.RETRIEVE_HOST_NAME);
            } else {
                return TaskManagerLocation.fromUnresolvedLocation(
                        unresolvedTaskManagerLocation, ResolutionMode.USE_IP_ONLY);
            }
        } catch (Throwable throwable) {
            final String errMsg =
                    String.format(
                            "TaskManager address %s cannot be resolved. %s",
                            unresolvedTaskManagerLocation.getExternalAddress(),
                            throwable.getMessage());
            throw new FlinkException(errMsg, throwable);
        }
    }

    @Override
    public void disconnectResourceManager(
            final ResourceManagerId resourceManagerId, final Exception cause) {

        if (resourceManagerAddress == null) {
            log.debug(
                    "Disconnecting ResourceManager {} was triggered with no ResourceManager address "
                            + "being set (anymore). That either indicates that the ResourceManager "
                            + "lost leadership in the mean time or the message was received while "
                            + "shutting down the JobMaster. No reconnect will be initiated.",
                    resourceManagerId);
        } else if (!resourceManagerAddress.getResourceManagerId().equals(resourceManagerId)) {
            log.debug(
                    "Disconnecting ResourceManager {} was received while this instance is currently "
                            + "connected to another ResourceManager {} indicating that a ResourceManager "
                            + "leader change happened. No reconnect will be initiated.",
                    resourceManagerId,
                    resourceManagerAddress.getResourceManagerId());
        } else {
            reconnectToResourceManager(cause);
        }
    }

    private void shutdownResourceManagerConnection(Exception cause) {
        // unsetting the resourceManagerAddress will prevent reconnection
        resourceManagerAddress = null;
        closeResourceManagerConnection(cause);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromTaskManager(
            final ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
        return taskManagerHeartbeatManager.receiveHeartbeat(resourceID, payload);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromResourceManager(final ResourceID resourceID) {
        return resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJobStatus());
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJob());
    }

    @Override
    public CompletableFuture<CheckpointStatsSnapshot> requestCheckpointStats(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestCheckpointStats());
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            final CheckpointType checkpointType, final Time timeout) {
        return schedulerNG.triggerCheckpoint(checkpointType);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable final String targetDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType,
            final Time timeout) {

        return schedulerNG.triggerSavepoint(targetDirectory, cancelJob, formatType);
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final SavepointFormatType formatType,
            final boolean terminate,
            final Time timeout) {

        return schedulerNG.stopWithSavepoint(targetDirectory, terminate, formatType);
    }

    @Override
    public void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {
        slotPoolService.notifyNotEnoughResourcesAvailable(acquiredResources);
    }

    @Override
    public CompletableFuture<Object> updateGlobalAggregate(
            String aggregateName, Object aggregand, byte[] serializedAggregateFunction) {

        AggregateFunction aggregateFunction = null;
        try {
            aggregateFunction =
                    InstantiationUtil.deserializeObject(
                            serializedAggregateFunction, userCodeLoader);
        } catch (Exception e) {
            log.error("Error while attempting to deserialize user AggregateFunction.");
            return FutureUtils.completedExceptionally(e);
        }

        Object accumulator = accumulators.get(aggregateName);
        if (null == accumulator) {
            accumulator = aggregateFunction.createAccumulator();
        }
        accumulator = aggregateFunction.add(aggregand, accumulator);
        accumulators.put(aggregateName, accumulator);
        return CompletableFuture.completedFuture(aggregateFunction.getResult(accumulator));
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            Time timeout) {
        return this.sendRequestToCoordinator(operatorId, serializedRequest);
    }

    @Override
    public CompletableFuture<?> stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> partitionIds) {
        CompletableFuture<?> future = new CompletableFuture<>();
        try {
            partitionTracker.stopTrackingAndReleasePartitions(partitionIds, false);
            future.complete(null);
        } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
        }
        return future;
    }

    @Override
    public CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
            Duration timeout, Set<ResultPartitionID> expectedPartitions) {
        this.partitionsToFetch = expectedPartitions;
        this.fetchPartitionsFuture = new CompletableFuture<>();

        // check already fetched partitions
        checkPartitionOnTaskManagerReportFinished();

        return FutureUtils.orTimeout(
                        fetchPartitionsFuture, timeout.toMillis(), TimeUnit.MILLISECONDS, null)
                .handleAsync(
                        (metrics, throwable) -> {
                            stopFetchAndRetainPartitionWithMetricsOnTaskManager();

                            if (throwable != null) {
                                if (throwable instanceof TimeoutException) {
                                    log.warn(
                                            "Timeout occurred after {} ms "
                                                    + "while fetching partition(s) ({}) from task managers.",
                                            timeout.toMillis(),
                                            expectedPartitions);

                                    return new ArrayList<>(fetchedPartitionsWithMetrics.values());
                                }
                                throw new CompletionException(throwable);
                            }

                            return new ArrayList<>(fetchedPartitionsWithMetrics.values());
                        },
                        getMainThreadExecutor());
    }

    @VisibleForTesting
    Map<ResultPartitionID, PartitionWithMetrics> getPartitionWithMetricsOnTaskManagers() {
        return fetchedPartitionsWithMetrics;
    }

    @Override
    public void startFetchAndRetainPartitionWithMetricsOnTaskManager() {
        fetchAndRetainPartitions = true;

        // process all already registered task managers
        registeredTaskManagers
                .keySet()
                .forEach(this::fetchAndRetainPartitionWithMetricsOnTaskManager);
    }

    private void fetchAndRetainPartitionWithMetricsOnTaskManager(ResourceID resourceId) {
        TaskManagerRegistration taskManager = registeredTaskManagers.get(resourceId);
        checkNotNull(taskManager);

        taskManager
                .getTaskExecutorGateway()
                .getAndRetainPartitionWithMetrics(jobGraph.getJobID())
                .thenAccept(
                        partitionWithMetrics -> {
                            if (fetchAndRetainPartitions) {
                                for (PartitionWithMetrics partitionWithMetric :
                                        partitionWithMetrics) {
                                    log.debug(
                                            "Received partition metrics for {} from Task Manager {}.",
                                            partitionWithMetric
                                                    .getPartition()
                                                    .getResultPartitionID(),
                                            resourceId);
                                    fetchedPartitionsWithMetrics.put(
                                            partitionWithMetric
                                                    .getPartition()
                                                    .getResultPartitionID(),
                                            partitionWithMetric);
                                }
                                checkPartitionOnTaskManagerReportFinished();
                            } else {
                                log.info(
                                        "Received late report of partition metrics from {}. Release the partitions.",
                                        resourceId);

                                taskManager
                                        .getTaskExecutorGateway()
                                        .releasePartitions(
                                                jobGraph.getJobID(),
                                                partitionWithMetrics.stream()
                                                        .map(PartitionWithMetrics::getPartition)
                                                        .map(
                                                                ShuffleDescriptor
                                                                        ::getResultPartitionID)
                                                        .collect(Collectors.toSet()));
                            }
                        });
    }

    private void stopFetchAndRetainPartitionWithMetricsOnTaskManager() {
        fetchAndRetainPartitions = false;
    }

    private void checkPartitionOnTaskManagerReportFinished() {
        if (fetchPartitionsFuture != null) {
            if (fetchedPartitionsWithMetrics.keySet().containsAll(partitionsToFetch)
                    && !fetchPartitionsFuture.isDone()) {
                fetchPartitionsFuture.complete(fetchedPartitionsWithMetrics.values());
            }
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyNewBlockedNodes(Collection<BlockedNode> newNodes) {
        blocklistHandler.addNewBlockedNodes(newNodes);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<JobResourceRequirements> requestJobResourceRequirements() {
        return CompletableFuture.completedFuture(schedulerNG.requestJobResourceRequirements());
    }

    @Override
    public CompletableFuture<Acknowledge> updateJobResourceRequirements(
            JobResourceRequirements jobResourceRequirements) {
        schedulerNG.updateJobResourceRequirements(jobResourceRequirements);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ----------------------------------------------------------------------------------------------
    // Internal methods
    // ----------------------------------------------------------------------------------------------

    // -- job starting and stopping
    // -----------------------------------------------------------------

    // ★ JobMaster 的启动顺序都在这里，且顺序不能随意调换：
    //     第 1 步：向 ShuffleMaster 注册本作业的 JobShuffleContext；
    //     第 2 步：startJobMasterServices() —— 建心跳管理器、启动槽位账本，
    //             并激活 ResourceManager 的选主监听（随后才会去建立 RM 连接）；
    //     第 3 步：startScheduling() —— 到这里才真正开始调度。
    //   一句话：先"能通消息"，再"能要资源"，最后才"派活"。
    private void startJobExecution() throws Exception {
        validateRunsInMainThread();

        JobShuffleContext context = new JobShuffleContextImpl(jobGraph.getJobID(), this);
        shuffleMaster.registerJob(context);

        startJobMasterServices();

        log.info(
                "Starting execution of job '{}' ({}) under job master id {}.",
                jobGraph.getName(),
                jobGraph.getJobID(),
                getFencingToken());

        // 开始调度
        startScheduling();
    }

    // ★ 这里的顺序同样有讲究：先 start() 槽位账本（此后它才开始接受针对本 leader 的消息），
    //   再监听 ResourceManager 选主。选主通知是异步到达的，因此本方法不会阻塞在"等 RM"上。
    private void startJobMasterServices() throws Exception {
        try {
            this.taskManagerHeartbeatManager = createTaskManagerHeartbeatManager(heartbeatServices);
            this.resourceManagerHeartbeatManager =
                    createResourceManagerHeartbeatManager(heartbeatServices);

            // start the slot pool make sure the slot pool now accepts messages for this leader
            slotPoolService.start(getFencingToken(), getAddress(), getMainThreadExecutor());

            // job is ready to go, try to establish connection with resource manager
            //   - activate leader retrieval for the resource manager
            //   - on notification of the leader, the connection will be established and
            //     the slot pool will start requesting slots
            resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
        } catch (Exception e) {
            handleStartJobMasterServicesError(e);
        }
    }

    private void handleStartJobMasterServicesError(Exception e) throws Exception {
        try {
            stopJobMasterServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    private void stopJobMasterServices() throws Exception {
        Exception resultingException = null;

        try {
            resourceManagerLeaderRetriever.stop();
        } catch (Exception e) {
            resultingException = e;
        }

        // TODO: Distinguish between job termination which should free all slots and a loss of
        // leadership which should keep the slots
        slotPoolService.close();

        stopHeartbeatServices();

        ExceptionUtils.tryRethrowException(resultingException);
    }

    // 停止方向与启动相反：先让调度停下（stopScheduling() 会 closeAsync 掉 schedulerNG，
    // 从而取消执行图里所有 Execution），再注销 Shuffle、断开与 TaskManager /
    // ResourceManager 的连接。用 runAfterwardsAsync 串联，保证"先停调度、后拆连接"的次序。
    private CompletableFuture<Void> stopJobExecution(final Exception cause) {
        validateRunsInMainThread();

        final CompletableFuture<Void> terminationFuture = stopScheduling();

        return FutureUtils.runAfterwardsAsync(
                terminationFuture,
                () -> {
                    shuffleMaster.unregisterJob(jobGraph.getJobID());
                    disconnectTaskManagerResourceManagerConnections(cause);
                    stopJobMasterServices();
                },
                getMainThreadExecutor());
    }

    private void disconnectTaskManagerResourceManagerConnections(Exception cause) {
        // disconnect from all registered TaskExecutors
        final Set<ResourceID> taskManagerResourceIds =
                new HashSet<>(registeredTaskManagers.keySet());

        for (ResourceID taskManagerResourceId : taskManagerResourceIds) {
            disconnectTaskManager(taskManagerResourceId, cause);
        }

        shutdownResourceManagerConnection(cause);
    }

    private void stopHeartbeatServices() {
        taskManagerHeartbeatManager.stop();
        resourceManagerHeartbeatManager.stop();
    }

    // ★ 调度真正开始的时刻。前面所有动作（建图、连 RM、收槽位）都只是准备，
    //   控制权在这里交给 SchedulerNG.startScheduling()：它检查作业是否满足启动条件
    //   （例如所需资源是否到位），然后按拓扑顺序把算子链部署到 TaskManager 上。
    //   ★ 注意它是"非阻塞"的：资源不够时调度只是挂起等待，不会抛异常。
    private void startScheduling() {
        schedulerNG.startScheduling();
    }

    // 先关掉指标组和状态监听（避免停止过程中又被回调进 JobMaster），
    // 再 closeAsync() 掉调度器 —— 执行图里的所有 Execution 会被取消并做资源清理。
    private CompletableFuture<Void> stopScheduling() {
        jobManagerJobMetricGroup.close();
        jobStatusListener.stop();

        return schedulerNG.closeAsync();
    }

    // ----------------------------------------------------------------------------------------------

    // JobMaster 出错的统一出口：JVM 级致命错误交给 fatalErrorHandler 直接终止进程；
    // 其余错误走 jobCompletionActions.jobMasterFailed() 通知 Dispatcher，
    // 由 Dispatcher 决定是否重启/恢复这个 JobMaster。
    private void handleJobMasterError(final Throwable cause) {
        if (ExceptionUtils.isJvmFatalError(cause)) {
            log.error("Fatal error occurred on JobManager.", cause);
            // The fatal error handler implementation should make sure that this call is
            // non-blocking
            fatalErrorHandler.onFatalError(cause);
        } else {
            jobCompletionActions.jobMasterFailed(cause);
        }
    }

    // ★ 作业状态收敛到终态时的收尾动作，由 Scheduler 经 JobStatusListener 回调过来。
    //   只有"全局终态"（FINISHED / CANCELED / FAILED / SUSPENDED）才处理：
    //     - FINISHED：非集群分区直接释放；集群级分区 stopTracking 后 promote，保留给后续复用；
    //     - 其他终态：所有被追踪的分区一律释放。
    //   随后在 futureExecutor 里等分区处理完，再把 ExecutionGraphInfo 交给
    //   jobCompletionActions.jobReachedGloballyTerminalState() 上报 Dispatcher，
    //   由 Dispatcher 去清理作业记录并关掉 JobMaster 本身。
    //   ★ 分区释放/提升失败不会让作业失败，只打 warning：TaskManager 最终会自行清理。
    private void jobStatusChanged(final JobStatus newJobStatus) {
        validateRunsInMainThread();
        if (newJobStatus.isGloballyTerminalState()) {
            CompletableFuture<Void> partitionPromoteFuture;
            if (newJobStatus == JobStatus.FINISHED) {
                Collection<ResultPartitionID> jobPartitions =
                        partitionTracker.getAllTrackedNonClusterPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionTracker.stopTrackingAndReleasePartitions(jobPartitions);
                Collection<ResultPartitionID> clusterPartitions =
                        partitionTracker.getAllTrackedClusterPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionPromoteFuture =
                        partitionTracker.stopTrackingAndPromotePartitions(clusterPartitions);
            } else {
                Collection<ResultPartitionID> allTracked =
                        partitionTracker.getAllTrackedPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionTracker.stopTrackingAndReleasePartitions(allTracked);
                partitionPromoteFuture = CompletableFuture.completedFuture(null);
            }

            final ExecutionGraphInfo executionGraphInfo = schedulerNG.requestJob();

            futureExecutor.execute(
                    () -> {
                        try {
                            partitionPromoteFuture.get();
                        } catch (Throwable e) {
                            // We do not want to fail the job in case of partition releasing and
                            // promoting fail. The TaskExecutors will release the partitions
                            // eventually when they find out the JobMaster is closed.
                            log.warn("Fail to release or promote partitions", e);
                        }
                        jobCompletionActions.jobReachedGloballyTerminalState(executionGraphInfo);
                    });
        }
    }

    // ResourceManager 选主变化的回调（已经 runAsync 切回 RPC 主线程）。
    // ★ 套路是"先断后连"：不管之前连着谁，先关掉旧连接再重连，
    //   从而保证任意时刻只与一个 ResourceManager 保持连接。
    private void notifyOfNewResourceManagerLeader(
            final String newResourceManagerAddress, final ResourceManagerId resourceManagerId) {
        resourceManagerAddress =
                createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);

        reconnectToResourceManager(
                new FlinkException(
                        String.format(
                                "ResourceManager leader changed to new address %s",
                                resourceManagerAddress)));
    }

    @Nullable
    private ResourceManagerAddress createResourceManagerAddress(
            @Nullable String newResourceManagerAddress,
            @Nullable ResourceManagerId resourceManagerId) {
        if (newResourceManagerAddress != null) {
            // the contract is: address == null <=> id == null
            checkNotNull(resourceManagerId);
            return new ResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
        } else {
            return null;
        }
    }

    private void reconnectToResourceManager(Exception cause) {
        closeResourceManagerConnection(cause);
        tryConnectToResourceManager();
    }

    private void tryConnectToResourceManager() {
        if (resourceManagerAddress != null) {
            connectToResourceManager();
        }
    }

    private void connectToResourceManager() {
        assert (resourceManagerAddress != null);
        assert (resourceManagerConnection == null);
        assert (establishedResourceManagerConnection == null);

        log.info("Connecting to ResourceManager {}", resourceManagerAddress);

        resourceManagerConnection =
                new ResourceManagerConnection(
                        log,
                        jobGraph.getJobID(),
                        resourceId,
                        getAddress(),
                        getFencingToken(),
                        resourceManagerAddress.getAddress(),
                        resourceManagerAddress.getResourceManagerId(),
                        futureExecutor);

        resourceManagerConnection.start();
    }

    // ★ 与 ResourceManager 注册真正成功后的动作 —— 走到这里 JobMaster 才有能力要资源。
    //   依次发生：记录已建立的连接 → blocklistHandler / slotPoolService /
    //   partitionTracker 分别接入 RM（★ slotPoolService.connectToResourceManager()
    //   之后才会真正开始为作业申请槽位）→ 把 RM 纳入心跳监控。
    //   在此之前调度其实已经开始，只是"没资源可要"，会停在等待资源的状态。
    //   如果此时已经切了 leader（连接的 targetLeaderId 对不上），则丢弃这个过期响应。
    private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
        final ResourceManagerId resourceManagerId = success.getResourceManagerId();

        // verify the response with current connection
        if (resourceManagerConnection != null
                && Objects.equals(
                        resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

            log.info(
                    "JobManager successfully registered at ResourceManager, leader id: {}.",
                    resourceManagerId);

            final ResourceManagerGateway resourceManagerGateway =
                    resourceManagerConnection.getTargetGateway();

            final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();

            establishedResourceManagerConnection =
                    new EstablishedResourceManagerConnection(
                            resourceManagerGateway, resourceManagerResourceId);

            blocklistHandler.registerBlocklistListener(resourceManagerGateway);
            slotPoolService.connectToResourceManager(resourceManagerGateway);
            partitionTracker.connectToResourceManager(resourceManagerGateway);

            resourceManagerHeartbeatManager.monitorTarget(
                    resourceManagerResourceId,
                    new ResourceManagerHeartbeatReceiver(resourceManagerGateway));
        } else {
            log.debug(
                    "Ignoring resource manager connection to {} because it's duplicated or outdated.",
                    resourceManagerId);
        }
    }

    private void closeResourceManagerConnection(Exception cause) {
        if (establishedResourceManagerConnection != null) {
            dissolveResourceManagerConnection(establishedResourceManagerConnection, cause);
            establishedResourceManagerConnection = null;
        }

        if (resourceManagerConnection != null) {
            // stop a potentially ongoing registration process
            resourceManagerConnection.close();
            resourceManagerConnection = null;
        }
    }

    private void dissolveResourceManagerConnection(
            EstablishedResourceManagerConnection establishedResourceManagerConnection,
            Exception cause) {
        final ResourceID resourceManagerResourceID =
                establishedResourceManagerConnection.getResourceManagerResourceID();

        if (log.isDebugEnabled()) {
            log.debug(
                    "Close ResourceManager connection {}.",
                    resourceManagerResourceID.getStringWithMetadata(),
                    cause);
        } else {
            log.info(
                    "Close ResourceManager connection {}: {}",
                    resourceManagerResourceID.getStringWithMetadata(),
                    cause.getMessage());
        }

        resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceID);

        ResourceManagerGateway resourceManagerGateway =
                establishedResourceManagerConnection.getResourceManagerGateway();
        resourceManagerGateway.disconnectJobManager(
                jobGraph.getJobID(), schedulerNG.requestJobStatus(), cause);
        blocklistHandler.deregisterBlocklistListener(resourceManagerGateway);
        slotPoolService.disconnectResourceManager();
    }

    private String getNodeIdOfTaskManager(ResourceID taskManagerId) {
        checkState(registeredTaskManagers.containsKey(taskManagerId));
        return registeredTaskManagers.get(taskManagerId).getTaskManagerLocation().getNodeId();
    }

    // ----------------------------------------------------------------------------------------------
    // Service methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public JobMasterGateway getGateway() {
        return getSelfGateway(JobMasterGateway.class);
    }

    // ----------------------------------------------------------------------------------------------
    // Utility classes
    // ----------------------------------------------------------------------------------------------

    private static final class TaskExecutorHeartbeatSender
            extends HeartbeatSender<AllocatedSlotReport> {
        private final TaskExecutorGateway taskExecutorGateway;

        private TaskExecutorHeartbeatSender(TaskExecutorGateway taskExecutorGateway) {
            this.taskExecutorGateway = taskExecutorGateway;
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(
                ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
            return taskExecutorGateway.heartbeatFromJobManager(resourceID, allocatedSlotReport);
        }
    }

    private static final class ResourceManagerHeartbeatReceiver extends HeartbeatReceiver<Void> {
        private final ResourceManagerGateway resourceManagerGateway;

        private ResourceManagerHeartbeatReceiver(ResourceManagerGateway resourceManagerGateway) {
            this.resourceManagerGateway = resourceManagerGateway;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(ResourceID resourceID, Void payload) {
            return resourceManagerGateway.heartbeatFromJobManager(resourceID);
        }
    }

    private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

        @Override
        public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
            runAsync(
                    () ->
                            notifyOfNewResourceManagerLeader(
                                    leaderAddress,
                                    ResourceManagerId.fromUuidOrNull(leaderSessionID)));
        }

        @Override
        public void handleError(final Exception exception) {
            handleJobMasterError(
                    new Exception("Fatal error in the ResourceManager leader service", exception));
        }
    }

    // ----------------------------------------------------------------------------------------------

    private class ResourceManagerConnection
            extends RegisteredRpcConnection<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    JobMasterRegistrationSuccess,
                    RegistrationResponse.Rejection> {
        private final JobID jobID;

        private final ResourceID jobManagerResourceID;

        private final String jobManagerRpcAddress;

        private final JobMasterId jobMasterId;

        ResourceManagerConnection(
                final Logger log,
                final JobID jobID,
                final ResourceID jobManagerResourceID,
                final String jobManagerRpcAddress,
                final JobMasterId jobMasterId,
                final String resourceManagerAddress,
                final ResourceManagerId resourceManagerId,
                final Executor executor) {
            super(log, resourceManagerAddress, resourceManagerId, executor);
            this.jobID = checkNotNull(jobID);
            this.jobManagerResourceID = checkNotNull(jobManagerResourceID);
            this.jobManagerRpcAddress = checkNotNull(jobManagerRpcAddress);
            this.jobMasterId = checkNotNull(jobMasterId);
        }

        @Override
        protected RetryingRegistration<
                        ResourceManagerId,
                        ResourceManagerGateway,
                        JobMasterRegistrationSuccess,
                        RegistrationResponse.Rejection>
                generateRegistration() {
            return new RetryingRegistration<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    JobMasterRegistrationSuccess,
                    RegistrationResponse.Rejection>(
                    log,
                    getRpcService(),
                    "ResourceManager",
                    ResourceManagerGateway.class,
                    getTargetAddress(),
                    getTargetLeaderId(),
                    jobMasterConfiguration.getRetryingRegistrationConfiguration()) {

                @Override
                protected CompletableFuture<RegistrationResponse> invokeRegistration(
                        ResourceManagerGateway gateway,
                        ResourceManagerId fencingToken,
                        long timeoutMillis) {
                    Time timeout = Time.milliseconds(timeoutMillis);

                    return gateway.registerJobMaster(
                            jobMasterId,
                            jobManagerResourceID,
                            jobManagerRpcAddress,
                            jobID,
                            timeout);
                }
            };
        }

        @Override
        protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
            runAsync(
                    () -> {
                        // filter out outdated connections
                        //noinspection ObjectEquality
                        if (this == resourceManagerConnection) {
                            establishResourceManagerConnection(success);
                        }
                    });
        }

        @Override
        protected void onRegistrationRejection(RegistrationResponse.Rejection rejection) {
            handleJobMasterError(
                    new IllegalStateException(
                            "The ResourceManager should never reject a JobMaster registration."));
        }

        @Override
        protected void onRegistrationFailure(final Throwable failure) {
            handleJobMasterError(failure);
        }
    }

    // ----------------------------------------------------------------------------------------------

    // Scheduler 中 ExecutionGraph 的 JobStatus 变化出口。
    // ★ 这里必须 runAsync 切回主线程：ExecutionGraph 的回调不在 RPC 线程上，
    //   而 jobStatusChanged() 要操作 partitionTracker 等非线程安全的状态。
    //   stop() 用来在 JobMaster 关闭时关掉这个"水龙头"，避免关闭过程中又被回调。
    private class JobManagerJobStatusListener implements JobStatusListener {

        private volatile boolean running = true;

        @Override
        public void jobStatusChanges(
                final JobID jobId, final JobStatus newJobStatus, final long timestamp) {

            if (running) {
                // run in rpc thread to avoid concurrency
                runAsync(() -> jobStatusChanged(newJobStatus));
            }
        }

        private void stop() {
            running = false;
        }
    }

    // 与 TaskManager 的心跳。三个作用：
    //   - 超时 / 不可达 → 断开该 TaskManager，并让它上面的槽位失败；
    //   - reportPayload 带回"执行部署报告"和累加器：前者用于与 JobMaster 侧的部署记录
    //     对账，发现 TaskManager 上多跑或少跑了哪些 ExecutionAttempt；
    //   - retrievePayload 把 JobMaster 侧的 AllocatedSlotReport 捎回 TaskManager。
    private class TaskManagerHeartbeatListener
            implements HeartbeatListener<
                    TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport> {

        @Override
        public void notifyHeartbeatTimeout(ResourceID resourceID) {
            final String message =
                    String.format(
                            "Heartbeat of TaskManager with id %s timed out.",
                            resourceID.getStringWithMetadata());

            log.info(message);
            handleTaskManagerConnectionLoss(resourceID, new TimeoutException(message));
        }

        private void handleTaskManagerConnectionLoss(ResourceID resourceID, Exception cause) {
            validateRunsInMainThread();
            disconnectTaskManager(resourceID, cause);
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "TaskManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());

            log.info(message);
            handleTaskManagerConnectionLoss(resourceID, new JobMasterException(message));
        }

        @Override
        public void reportPayload(
                ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
            validateRunsInMainThread();
            executionDeploymentReconciler.reconcileExecutionDeployments(
                    resourceID,
                    payload.getExecutionDeploymentReport(),
                    executionDeploymentTracker.getExecutionsOn(resourceID));
            for (AccumulatorSnapshot snapshot :
                    payload.getAccumulatorReport().getAccumulatorSnapshots()) {
                schedulerNG.updateAccumulators(snapshot);
            }
        }

        @Override
        public AllocatedSlotReport retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return slotPoolService.createAllocatedSlotReport(resourceID);
        }
    }

    // 与 ResourceManager 之间的心跳。
    // ★ 心跳丢失不会让作业失败，只是重连 ResourceManager —— 作业已经拿到的槽位仍然有效，
    //   这一点与 TaskManager 心跳超时（会连带槽位失败）不同。
    private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceId) {
            final String message =
                    String.format(
                            "The heartbeat of ResourceManager with id %s timed out.",
                            resourceId.getStringWithMetadata());
            log.info(message);

            handleResourceManagerConnectionLoss(resourceId, new TimeoutException(message));
        }

        private void handleResourceManagerConnectionLoss(ResourceID resourceId, Exception cause) {
            validateRunsInMainThread();
            if (establishedResourceManagerConnection != null
                    && establishedResourceManagerConnection
                            .getResourceManagerResourceID()
                            .equals(resourceId)) {
                reconnectToResourceManager(cause);
            }
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "ResourceManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());
            log.info(message);

            handleResourceManagerConnectionLoss(resourceID, new JobMasterException(message));
        }

        @Override
        public void reportPayload(ResourceID resourceID, Void payload) {
            // nothing to do since the payload is of type Void
        }

        @Override
        public Void retrievePayload(ResourceID resourceID) {
            return null;
        }
    }

    private class JobMasterBlocklistContext implements BlocklistContext {

        @Override
        public void blockResources(Collection<BlockedNode> blockedNodes) {
            Set<String> blockedNodeIds =
                    blockedNodes.stream().map(BlockedNode::getNodeId).collect(Collectors.toSet());

            Collection<ResourceID> blockedTaskMangers =
                    registeredTaskManagers.keySet().stream()
                            .filter(
                                    taskManagerId ->
                                            blockedNodeIds.contains(
                                                    getNodeIdOfTaskManager(taskManagerId)))
                            .collect(Collectors.toList());

            blockedTaskMangers.forEach(
                    taskManagerId -> {
                        Exception cause =
                                new FlinkRuntimeException(
                                        String.format(
                                                "TaskManager %s is blocked.",
                                                taskManagerId.getStringWithMetadata()));
                        slotPoolService.releaseFreeSlotsOnTaskManager(taskManagerId, cause);
                    });
        }

        @Override
        public void unblockResources(Collection<BlockedNode> unblockedNodes) {}
    }
}
