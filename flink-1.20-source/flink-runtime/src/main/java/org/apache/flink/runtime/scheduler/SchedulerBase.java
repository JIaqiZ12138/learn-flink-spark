/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.JobInfoImpl;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.DefaultVertexAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.JobStatusProvider;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.executiongraph.metrics.DownTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.UpTimeGauge;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.scheduler.exceptionhistory.FailureHandlingResultSnapshot;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.metrics.DeploymentStateTimeMetrics;
import org.apache.flink.runtime.scheduler.metrics.JobStatusMetrics;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointTerminationHandlerImpl;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointTerminationManager;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.util.BoundedFIFOQueue;
import org.apache.flink.runtime.util.IntArrayList;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphUtils.isAnyOutputBlocking;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Base class which can be used to implement {@link SchedulerNG}. */
// ★ SchedulerBase —— 调度器的"公共骨架"：SchedulerNG 的实现被拆成两层，骨架负责一切与"决策"无关的事情：
//   持有 ExecutionGraph、托管 OperatorCoordinator、承接 TM/REST 的状态与检查点上报、管理 cancel/close/savepoint 生命周期。
//   子类（DefaultScheduler，以及批模式下的 AdaptiveBatchScheduler）只需实现决策部分的模板方法：
//   startSchedulingInternal() / cancelAllPendingSlotRequestsInternal() / onTaskFinished() / onTaskFailed() 等。
//   易误解点：slot 池（SlotPool）并不在这里，它归 JobMaster 的 SlotPoolService；调度器只拿到 slot 分配器。
public abstract class SchedulerBase implements SchedulerNG, CheckpointScheduling {

    private final Logger log;

    private final JobGraph jobGraph;

    protected final JobInfo jobInfo;

    // ★ 作业的完整运行时状态都在这里；SchedulerBase 持有它，子类只通过下面的访问器间接读写。
    private final ExecutionGraph executionGraph;

    // SchedulingTopology 是 ExecutionGraph 的"调度只读视图"：调度策略只看得到顶点/分区/region，看不到 Execution。
    private final SchedulingTopology schedulingTopology;

    protected final StateLocationRetriever stateLocationRetriever;

    protected final InputsLocationsRetriever inputsLocationsRetriever;

    private final CompletedCheckpointStore completedCheckpointStore;

    private final CheckpointsCleaner checkpointsCleaner;

    private final CheckpointIDCounter checkpointIdCounter;

    protected final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    // ★ 顶点版本号记录器：部署/取消前先记录版本，之后只要版本变了就说明这批操作已作废。
    //   它是"过期部署/过期重启不生效"的关键机制（另见 SchedulerBase.incrementVersionsOfAllVertices）。
    protected final ExecutionVertexVersioner executionVertexVersioner;

    private final KvStateHandler kvStateHandler;

    private final ExecutionGraphHandler executionGraphHandler;

    protected final OperatorCoordinatorHandler operatorCoordinatorHandler;

    // JobMaster 的主线程 executor：所有状态变更都必须在这上面串行执行，所以下面大量出现 assertRunningInMainThread。
    private final ComponentMainThreadExecutor mainThreadExecutor;

    private final BoundedFIFOQueue<RootExceptionHistoryEntry> exceptionHistory;

    private RootExceptionHistoryEntry latestRootExceptionEntry;

    // ★ ExecutionGraph 的工厂由 JobMaster 构造期注入，它内部带着 ExecutionDeploymentTracker 适配器，
    //   用于记录"哪个 execution attempt 当前部署在哪个 TM 上"，作业恢复时据此清理 TM 上的残留实例。
    private final ExecutionGraphFactory executionGraphFactory;

    private final MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings;

    private final DeploymentStateTimeMetrics deploymentStateTimeMetrics;

    private final VertexEndOfDataListener vertexEndOfDataListener;

    public SchedulerBase(
            final Logger log,
            final JobGraph jobGraph,
            final Executor ioExecutor,
            final Configuration jobMasterConfiguration,
            final CheckpointsCleaner checkpointsCleaner,
            final CheckpointRecoveryFactory checkpointRecoveryFactory,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup,
            final ExecutionVertexVersioner executionVertexVersioner,
            long initializationTimestamp,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final JobStatusListener jobStatusListener,
            final ExecutionGraphFactory executionGraphFactory,
            final VertexParallelismStore vertexParallelismStore)
            throws Exception {

        this.log = checkNotNull(log);
        this.jobGraph = checkNotNull(jobGraph);
        this.jobInfo = new JobInfoImpl(jobGraph.getJobID(), jobGraph.getName());
        this.executionGraphFactory = executionGraphFactory;

        this.jobManagerJobMetricGroup = checkNotNull(jobManagerJobMetricGroup);
        this.executionVertexVersioner = checkNotNull(executionVertexVersioner);
        this.mainThreadExecutor = mainThreadExecutor;

        this.checkpointsCleaner = checkpointsCleaner;
        // 检查点相关服务（已完成检查点存储、检查点 ID 计数器）只在开启检查点的作业上真正创建，
        // SchedulerUtils 内部做判断，未开启检查点时会退化成 NoOp 实现。
        this.completedCheckpointStore =
                SchedulerUtils.createCompletedCheckpointStoreIfCheckpointingIsEnabled(
                        jobGraph,
                        jobMasterConfiguration,
                        checkNotNull(checkpointRecoveryFactory),
                        ioExecutor,
                        log);
        this.checkpointIdCounter =
                SchedulerUtils.createCheckpointIDCounterIfCheckpointingIsEnabled(
                        jobGraph, checkNotNull(checkpointRecoveryFactory));

        this.jobStatusMetricsSettings =
                MetricOptions.JobStatusMetricsSettings.fromConfiguration(jobMasterConfiguration);
        this.deploymentStateTimeMetrics =
                new DeploymentStateTimeMetrics(jobGraph.getJobType(), jobStatusMetricsSettings);

        // ★ 调度器的构造期就把 ExecutionGraph 建好并恢复完毕（savepoint/checkpoint 恢复也在这里发生），
        //   也就是说构造一结束，作业拓扑、历史执行尝试、恢复出来的状态就已经全部就绪。
        this.executionGraph =
                createAndRestoreExecutionGraph(
                        completedCheckpointStore,
                        checkpointsCleaner,
                        checkpointIdCounter,
                        initializationTimestamp,
                        mainThreadExecutor,
                        jobStatusListener,
                        vertexParallelismStore);

        // 注意 SchedulingTopology 不是新建的，而是从 ExecutionGraph 上取到的视图，二者始终同步。
        this.schedulingTopology = executionGraph.getSchedulingTopology();

        stateLocationRetriever =
                executionVertexId ->
                        getExecutionVertex(executionVertexId).getPreferredLocationBasedOnState();
        // 这两个查询器是调度决策的输入源：前者提供本地恢复需要的状态位置，后者提供上游 TM 位置与分区位置。
        inputsLocationsRetriever =
                new ExecutionGraphToInputsLocationsRetrieverAdapter(executionGraph);

        this.kvStateHandler = new KvStateHandler(executionGraph);
        this.executionGraphHandler =
                new ExecutionGraphHandler(executionGraph, log, ioExecutor, this.mainThreadExecutor);

        // OperatorCoordinator（算子协调器）由调度器托管，这里完成创建与初始化，它的事件入口也挂在 SchedulerNG 上。
        this.operatorCoordinatorHandler =
                new DefaultOperatorCoordinatorHandler(executionGraph, this::handleGlobalFailure);
        operatorCoordinatorHandler.initializeOperatorCoordinators(this.mainThreadExecutor);

        // 异常历史用有界 FIFO 队列保存，上限由 WebOptions.MAX_EXCEPTION_HISTORY_SIZE 控制，供 Web UI 展示。
        this.exceptionHistory =
                new BoundedFIFOQueue<>(
                        jobMasterConfiguration.get(WebOptions.MAX_EXCEPTION_HISTORY_SIZE));

        this.vertexEndOfDataListener = new VertexEndOfDataListener(executionGraph);
    }

    // 关闭两个检查点服务：两次 try/catch 是为了不因第一个失败而漏掉第二个，两个异常会被合并保留。
    private void shutDownCheckpointServices(JobStatus jobStatus) {
        Exception exception = null;

        try {
            completedCheckpointStore.shutdown(jobStatus, checkpointsCleaner);
        } catch (Exception e) {
            exception = e;
        }

        try {
            checkpointIdCounter.shutdown(jobStatus).get();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            log.error("Error while shutting down checkpoint services.", exception);
        }
    }

    private static int normalizeParallelism(int parallelism) {
        if (parallelism == ExecutionConfig.PARALLELISM_DEFAULT) {
            return 1;
        }
        return parallelism;
    }

    /**
     * Get a default value to use for a given vertex's max parallelism if none was specified.
     *
     * @param vertex the vertex to compute a default max parallelism for
     * @return the computed max parallelism
     */
    public static int getDefaultMaxParallelism(JobVertex vertex) {
        return KeyGroupRangeAssignment.computeDefaultMaxParallelism(
                normalizeParallelism(vertex.getParallelism()));
    }

    public static VertexParallelismStore computeVertexParallelismStore(
            Iterable<JobVertex> vertices, Function<JobVertex, Integer> defaultMaxParallelismFunc) {
        return computeVertexParallelismStore(
                vertices, defaultMaxParallelismFunc, SchedulerBase::normalizeParallelism);
    }

    /**
     * Compute the {@link VertexParallelismStore} for all given vertices, which will set defaults
     * and ensure that the returned store contains valid parallelisms, with a custom function for
     * default max parallelism calculation and a custom function for normalizing vertex parallelism.
     *
     * @param vertices the vertices to compute parallelism for
     * @param defaultMaxParallelismFunc a function for computing a default max parallelism if none
     *     is specified on a given vertex
     * @param normalizeParallelismFunc a function for normalizing vertex parallelism
     * @return the computed parallelism store
     */
    // 并发度（parallelism）与最大并发度在这一层被规范化：用户没显式设置 maxParallelism 时按并发度推算，
    // 并且只有【未被用户显式设置】的 maxParallelism 才允许之后被改写（见下面的 autoConfigured 开关）。
    public static VertexParallelismStore computeVertexParallelismStore(
            Iterable<JobVertex> vertices,
            Function<JobVertex, Integer> defaultMaxParallelismFunc,
            Function<Integer, Integer> normalizeParallelismFunc) {
        DefaultVertexParallelismStore store = new DefaultVertexParallelismStore();

        for (JobVertex vertex : vertices) {
            int parallelism = normalizeParallelismFunc.apply(vertex.getParallelism());

            int maxParallelism = vertex.getMaxParallelism();
            final boolean autoConfigured;
            // if no max parallelism was configured by the user, we calculate and set a default
            if (maxParallelism == JobVertex.MAX_PARALLELISM_DEFAULT) {
                maxParallelism = defaultMaxParallelismFunc.apply(vertex);
                autoConfigured = true;
            } else {
                autoConfigured = false;
            }

            VertexParallelismInformation parallelismInfo =
                    new DefaultVertexParallelismInfo(
                            parallelism,
                            maxParallelism,
                            // Allow rescaling if the max parallelism was not set explicitly by the
                            // user
                            (newMax) ->
                                    autoConfigured
                                            ? Optional.empty()
                                            : Optional.of(
                                                    "Cannot override a configured max parallelism."));
            store.setParallelismInfo(vertex.getID(), parallelismInfo);
        }

        return store;
    }

    /**
     * Compute the {@link VertexParallelismStore} for all given vertices, which will set defaults
     * and ensure that the returned store contains valid parallelisms.
     *
     * @param vertices the vertices to compute parallelism for
     * @return the computed parallelism store
     */
    public static VertexParallelismStore computeVertexParallelismStore(
            Iterable<JobVertex> vertices) {
        return computeVertexParallelismStore(vertices, SchedulerBase::getDefaultMaxParallelism);
    }

    /**
     * Compute the {@link VertexParallelismStore} for all vertices of a given job graph, which will
     * set defaults and ensure that the returned store contains valid parallelisms.
     *
     * @param jobGraph the job graph to retrieve vertices from
     * @return the computed parallelism store
     */
    public static VertexParallelismStore computeVertexParallelismStore(JobGraph jobGraph) {
        return computeVertexParallelismStore(jobGraph.getVertices());
    }

    // ★ ExecutionGraph 的真正创建点。默认走 DefaultExecutionGraphFactory，它内部有三步容易忽略的动作：
    //   ① 建出图本身（默认 DefaultExecutionGraph，可由自定义 ExecutionGraphFactory 替换）；
    //   ② 挂上 InternalTaskFailuresListener —— ExecutionGraph 内部报出的失败会绕回调度器
    //      （notifyTaskFailure → updateTaskExecutionState，notifyGlobalFailure → handleGlobalFailure）；
    //   ③ registerJobStatusListener + start()，把图的状态机接到主线程 executor 上。
    private ExecutionGraph createAndRestoreExecutionGraph(
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            long initializationTimestamp,
            ComponentMainThreadExecutor mainThreadExecutor,
            JobStatusListener jobStatusListener,
            VertexParallelismStore vertexParallelismStore)
            throws Exception {

        final ExecutionGraph newExecutionGraph =
                executionGraphFactory.createAndRestoreExecutionGraph(
                        jobGraph,
                        completedCheckpointStore,
                        checkpointsCleaner,
                        checkpointIdCounter,
                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint.fromJobType(
                                jobGraph.getJobType()),
                        initializationTimestamp,
                        new DefaultVertexAttemptNumberStore(),
                        vertexParallelismStore,
                        deploymentStateTimeMetrics,
                        getMarkPartitionFinishedStrategy(),
                        log);

        newExecutionGraph.setInternalTaskFailuresListener(
                new UpdateSchedulerNgOnInternalFailuresListener(this));
        newExecutionGraph.registerJobStatusListener(jobStatusListener);
        newExecutionGraph.start(mainThreadExecutor);

        return newExecutionGraph;
    }

    protected void resetForNewExecutions(final Collection<ExecutionVertexID> vertices) {
        vertices.stream().forEach(this::resetForNewExecution);
    }

    protected void resetForNewExecution(final ExecutionVertexID executionVertexId) {
        getExecutionVertex(executionVertexId).resetForNewExecution();
    }

    // ★ failover 之后的状态恢复：流作业从最近一次检查点恢复（全局恢复恢复所有顶点，局部恢复只恢复涉及的 subtask），
    //   批作业没有 CheckpointCoordinator，因此不恢复数据、只通知 OperatorCoordinator 重置。
    //   注意开头会 abort 掉所有 pending 的检查点，避免旧检查点污染恢复出来的状态。
    protected void restoreState(
            final Set<ExecutionVertexID> vertices, final boolean isGlobalRecovery)
            throws Exception {
        vertexEndOfDataListener.restoreVertices(vertices);

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();

        if (checkpointCoordinator == null) {
            // batch failover case - we only need to notify the OperatorCoordinators,
            // not do any actual state restore
            if (isGlobalRecovery) {
                notifyCoordinatorsOfEmptyGlobalRestore();
            } else {
                notifyCoordinatorsOfSubtaskRestore(
                        getInvolvedExecutionJobVerticesAndSubtasks(vertices),
                        OperatorCoordinator.NO_CHECKPOINT);
            }
            return;
        }

        // if there is checkpointed state, reload it into the executions

        // abort pending checkpoints to
        // i) enable new checkpoint triggering without waiting for last checkpoint expired.
        // ii) ensure the EXACTLY_ONCE semantics if needed.
        checkpointCoordinator.abortPendingCheckpoints(
                new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION));

        if (isGlobalRecovery) {
            final Set<ExecutionJobVertex> jobVerticesToRestore =
                    getInvolvedExecutionJobVertices(vertices);

            checkpointCoordinator.restoreLatestCheckpointedStateToAll(jobVerticesToRestore, true);

        } else {
            final Map<ExecutionJobVertex, IntArrayList> subtasksToRestore =
                    getInvolvedExecutionJobVerticesAndSubtasks(vertices);

            final OptionalLong restoredCheckpointId =
                    checkpointCoordinator.restoreLatestCheckpointedStateToSubtasks(
                            subtasksToRestore.keySet());

            // Ideally, the Checkpoint Coordinator would call OperatorCoordinator.resetSubtask, but
            // the Checkpoint Coordinator is not aware of subtasks in a local failover. It always
            // assigns state to all subtasks, and for the subtask execution attempts that are still
            // running (or not waiting to be deployed) the state assignment has simply no effect.
            // Because of that, we need to do the "subtask restored" notification here.
            // Once the Checkpoint Coordinator is properly aware of partial (region) recovery,
            // this code should move into the Checkpoint Coordinator.
            final long checkpointId =
                    restoredCheckpointId.orElse(OperatorCoordinator.NO_CHECKPOINT);
            notifyCoordinatorsOfSubtaskRestore(subtasksToRestore, checkpointId);
        }
    }

    private void notifyCoordinatorsOfSubtaskRestore(
            final Map<ExecutionJobVertex, IntArrayList> restoredSubtasks, final long checkpointId) {

        for (final Map.Entry<ExecutionJobVertex, IntArrayList> vertexSubtasks :
                restoredSubtasks.entrySet()) {
            final ExecutionJobVertex jobVertex = vertexSubtasks.getKey();
            final IntArrayList subtasks = vertexSubtasks.getValue();

            final Collection<OperatorCoordinatorHolder> coordinators =
                    jobVertex.getOperatorCoordinators();
            if (coordinators.isEmpty()) {
                continue;
            }

            while (!subtasks.isEmpty()) {
                final int subtask =
                        subtasks.removeLast(); // this is how IntArrayList implements iterations
                for (final OperatorCoordinatorHolder opCoordinator : coordinators) {
                    opCoordinator.subtaskReset(subtask, checkpointId);
                }
            }
        }
    }

    private void notifyCoordinatorsOfEmptyGlobalRestore() throws Exception {
        for (final ExecutionJobVertex ejv : getExecutionGraph().getAllVertices().values()) {
            if (!ejv.isInitialized()) {
                continue;
            }
            for (final OperatorCoordinatorHolder coordinator : ejv.getOperatorCoordinators()) {
                coordinator.resetToCheckpoint(OperatorCoordinator.NO_CHECKPOINT, null);
            }
        }
    }

    private Set<ExecutionJobVertex> getInvolvedExecutionJobVertices(
            final Set<ExecutionVertexID> executionVertices) {

        final Set<ExecutionJobVertex> tasks = new HashSet<>();
        for (ExecutionVertexID executionVertexID : executionVertices) {
            final ExecutionVertex executionVertex = getExecutionVertex(executionVertexID);
            tasks.add(executionVertex.getJobVertex());
        }
        return tasks;
    }

    private Map<ExecutionJobVertex, IntArrayList> getInvolvedExecutionJobVerticesAndSubtasks(
            final Set<ExecutionVertexID> executionVertices) {

        final HashMap<ExecutionJobVertex, IntArrayList> result = new HashMap<>();

        for (ExecutionVertexID executionVertexID : executionVertices) {
            final ExecutionVertex executionVertex = getExecutionVertex(executionVertexID);
            final IntArrayList subtasks =
                    result.computeIfAbsent(
                            executionVertex.getJobVertex(), (key) -> new IntArrayList(32));
            subtasks.add(executionVertex.getParallelSubtaskIndex());
        }

        return result;
    }

    // ★ 由 ExecutionDeployer 在申请 slot 之前调用，把执行状态推进到 SCHEDULED。
    //   注意 SCHEDULED 只表示"已进入调度流程"，此时 Task 还没下发（下发后才是 DEPLOYING/RUNNING）。
    protected void transitionToScheduled(final List<ExecutionVertexID> verticesToDeploy) {
        verticesToDeploy.forEach(
                executionVertexId ->
                        getExecutionVertex(executionVertexId)
                                .getCurrentExecutionAttempt()
                                .transitionState(ExecutionState.SCHEDULED));
    }

    protected void setGlobalFailureCause(@Nullable final Throwable cause, long timestamp) {
        if (cause != null) {
            executionGraph.initFailureCause(cause, timestamp);
        }
    }

    protected ComponentMainThreadExecutor getMainThreadExecutor() {
        return mainThreadExecutor;
    }

    // ★ 判定作业彻底失败：先推高所有顶点的版本号（让在途的部署与重启全部作废），再撤销所有 pending slot 请求，
    //   最后让 ExecutionGraph 自己进入失败状态；收尾的归档动作是【异步】的 —— 等作业真正终止后才记录全局失败。
    protected void failJob(
            Throwable cause, long timestamp, CompletableFuture<Map<String, String>> failureLabels) {
        incrementVersionsOfAllVertices();
        cancelAllPendingSlotRequestsInternal();
        executionGraph.failJob(cause, timestamp);
        getJobTerminationFuture().thenRun(() -> archiveGlobalFailure(cause, failureLabels));
    }

    protected final SchedulingTopology getSchedulingTopology() {
        return schedulingTopology;
    }

    protected final ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker() {
        return executionGraph.getResultPartitionAvailabilityChecker();
    }

    protected final void transitionToRunning() {
        executionGraph.transitionToRunning();
    }

    public ExecutionVertex getExecutionVertex(final ExecutionVertexID executionVertexId) {
        return executionGraph
                .getAllVertices()
                .get(executionVertexId.getJobVertexId())
                .getTaskVertices()[executionVertexId.getSubtaskIndex()];
    }

    public ExecutionJobVertex getExecutionJobVertex(final JobVertexID jobVertexId) {
        return executionGraph.getAllVertices().get(jobVertexId);
    }

    protected JobGraph getJobGraph() {
        return jobGraph;
    }

    // 重启次数由子类提供（DefaultScheduler 从 ExecutionFailureHandler 取），用于 NUM_RESTARTS 指标。
    protected abstract long getNumberOfRestarts();

    // 只有阻塞型（blocking）结果分区才需要"标记完成"：下游必须等上游整批数据写完才能消费；
    // 而流水线型（pipelined）分区是边写边读，不存在"写完了"这个时刻。
    protected MarkPartitionFinishedStrategy getMarkPartitionFinishedStrategy() {
        // blocking partition always need mark finished.
        return ResultPartitionType::isBlockingOrBlockingPersistentResultPartition;
    }

    // ★ "让所有在途部署作废"的通用手段：把每个顶点的版本号 +1，之前记录下的版本就对不上了，
    //   ExecutionDeployer 部署前会校验版本并静默丢弃。cancel / failJob / closeAsync 都会先调它。
    private Map<ExecutionVertexID, ExecutionVertexVersion> incrementVersionsOfAllVertices() {
        return executionVertexVersioner.recordVertexModifications(
                IterableUtils.toStream(schedulingTopology.getVertices())
                        .map(SchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet()));
    }

    // ★ slot 请求的取消被设计成抽象方法，原因是 SchedulerBase 不持有 slot 池、也不认识 ExecutionSlotAllocator，
    //   真正的"撤单"由子类实现（DefaultScheduler 转交 executionSlotAllocator.cancel()）。
    //   想知道 slot 请求的记账与超时判定在哪：SlotPool（由 JobMaster 的 SlotPoolService 持有）。
    protected abstract void cancelAllPendingSlotRequestsInternal();

    // 状态转换只是转发给 ExecutionGraph，转换的合法性校验在 ExecutionGraph 自己的状态机里。
    protected void transitionExecutionGraphState(
            final JobStatus current, final JobStatus newState) {
        executionGraph.transitionState(current, newState);
    }

    @VisibleForTesting
    CheckpointCoordinator getCheckpointCoordinator() {
        return executionGraph.getCheckpointCoordinator();
    }

    /**
     * ExecutionGraph is exposed to make it easier to rework tests to be based on the new scheduler.
     * ExecutionGraph is expected to be used only for state check. Yet at the moment, before all the
     * actions are factored out from ExecutionGraph and its sub-components, some actions may still
     * be performed directly on it.
     */
    @VisibleForTesting
    public ExecutionGraph getExecutionGraph() {
        return executionGraph;
    }

    // ------------------------------------------------------------------------
    // SchedulerNG
    // ------------------------------------------------------------------------

    // ★★ JobMaster.startScheduling() 最终落到的就是这里（SchedulerNG 的接口方法，且被声明成 final）。
    //   骨架先把"与决策无关的准备工作"做完，再交给子类的 startSchedulingInternal()：
    //     ① 注册作业指标（uptime / downtime / NUM_RESTARTS / 各 JobStatus 时长等）；
    //     ② 启动所有 OperatorCoordinator —— 必须早于任何 Task 部署，否则 TM 侧发来的算子事件没有接收者；
    //     ③ 调 startSchedulingInternal()，由子类去决策（DefaultScheduler → SchedulingStrategy）。
    //   之所以是 final，就是为了保证"指标 + 协调器"这两步在任何调度器实现里都不会被漏掉。
    @Override
    public final void startScheduling() {
        mainThreadExecutor.assertRunningInMainThread();
        registerJobMetrics(
                jobManagerJobMetricGroup,
                executionGraph,
                this::getNumberOfRestarts,
                deploymentStateTimeMetrics,
                executionGraph::registerJobStatusListener,
                executionGraph.getStatusTimestamp(JobStatus.INITIALIZING),
                jobStatusMetricsSettings);
        operatorCoordinatorHandler.startAllOperatorCoordinators();
        startSchedulingInternal();
    }

    // 指标注册做成 static，是为了让其他调度器实现能复用同一套指标定义；
    // NUM_RESTARTS / FULL_RESTARTS 直接读子类提供的重启计数 Gauge。
    public static void registerJobMetrics(
            MetricGroup metrics,
            JobStatusProvider jobStatusProvider,
            Gauge<Long> numberOfRestarts,
            DeploymentStateTimeMetrics deploymentTimeMetrics,
            Consumer<JobStatusListener> jobStatusListenerRegistrar,
            long initializationTimestamp,
            MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings) {
        metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(jobStatusProvider));
        metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(jobStatusProvider));
        metrics.gauge(MetricNames.NUM_RESTARTS, numberOfRestarts::getValue);
        metrics.gauge(MetricNames.FULL_RESTARTS, numberOfRestarts::getValue);

        final JobStatusMetrics jobStatusMetrics =
                new JobStatusMetrics(initializationTimestamp, jobStatusMetricsSettings);
        jobStatusMetrics.registerMetrics(metrics);
        jobStatusListenerRegistrar.accept(jobStatusMetrics);

        deploymentTimeMetrics.registerMetrics(metrics);
    }

    // 决策的抽象点：由子类实现（DefaultScheduler 委托给 SchedulingStrategy，AdaptiveBatchScheduler 另有逻辑）。
    protected abstract void startSchedulingInternal();

    // 关闭顺序有讲究：先让 ExecutionGraph 终止并据此关掉检查点服务（用 FutureUtils 组合成异步链），
    // 同时推高所有顶点版本号 + 撤销所有 pending slot 请求，避免关闭过程中出现"幽灵部署"；
    // 返回的 future 完成才算真正关闭完毕。
    @Override
    public CompletableFuture<Void> closeAsync() {
        mainThreadExecutor.assertRunningInMainThread();

        final FlinkException cause = new FlinkException("Scheduler is being stopped.");

        final CompletableFuture<Void> checkpointServicesShutdownFuture =
                FutureUtils.composeAfterwardsAsync(
                        executionGraph
                                .getTerminationFuture()
                                .thenAcceptAsync(
                                        this::shutDownCheckpointServices, getMainThreadExecutor()),
                        checkpointsCleaner::closeAsync,
                        getMainThreadExecutor());

        FutureUtils.assertNoException(checkpointServicesShutdownFuture);

        incrementVersionsOfAllVertices();
        cancelAllPendingSlotRequestsInternal();
        executionGraph.suspend(cause);
        operatorCoordinatorHandler.disposeAllOperatorCoordinators();
        return checkpointServicesShutdownFuture;
    }

    // 取消作业：与 failJob 一样，先"作废在途操作"（推版本号 + 撤 slot 请求），再让 ExecutionGraph 走取消状态机。
    @Override
    public void cancel() {
        mainThreadExecutor.assertRunningInMainThread();

        incrementVersionsOfAllVertices();
        cancelAllPendingSlotRequestsInternal();
        executionGraph.cancel();
    }

    @Override
    public CompletableFuture<JobStatus> getJobTerminationFuture() {
        return executionGraph.getTerminationFuture();
    }

    protected final void archiveGlobalFailure(
            Throwable failure, CompletableFuture<Map<String, String>> failureLabels) {
        archiveGlobalFailure(
                failure,
                executionGraph.getStatusTimestamp(JobStatus.FAILED),
                failureLabels,
                StreamSupport.stream(executionGraph.getAllExecutionVertices().spliterator(), false)
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toSet()));
    }

    private void archiveGlobalFailure(
            Throwable failure,
            long timestamp,
            CompletableFuture<Map<String, String>> failureLabels,
            Iterable<Execution> executions) {
        latestRootExceptionEntry =
                RootExceptionHistoryEntry.fromGlobalFailure(
                        failure, timestamp, failureLabels, executions);
        exceptionHistory.add(latestRootExceptionEntry);
        log.debug("Archive global failure.", failure);
    }

    // 失败归档的关键判断：这次失败是不是"根因" ——
    // 是根因就新建一条 RootExceptionHistoryEntry；不是根因则挂到已有根因条目下，作为并发异常记录下来。
    // 这既是 Web UI 故障历史的数据来源，也是"一次失败引发一串连锁失败"的归并逻辑。
    protected final void archiveFromFailureHandlingResult(
            FailureHandlingResultSnapshot failureHandlingResult) {
        if (!failureHandlingResult.isRootCause()) {
            // Handle all subsequent exceptions as the concurrent exceptions when it's not a new
            // attempt.
            checkState(
                    latestRootExceptionEntry != null,
                    "A root exception entry should exist if failureHandlingResult wasn't "
                            + "generated as part of a new error handling cycle.");
            List<Execution> concurrentlyExecutions = new ArrayList<>();
            failureHandlingResult.getRootCauseExecution().ifPresent(concurrentlyExecutions::add);
            concurrentlyExecutions.addAll(failureHandlingResult.getConcurrentlyFailedExecution());

            latestRootExceptionEntry.addConcurrentExceptions(concurrentlyExecutions);
        } else if (failureHandlingResult.getRootCauseExecution().isPresent()) {
            final Execution rootCauseExecution =
                    failureHandlingResult.getRootCauseExecution().get();

            latestRootExceptionEntry =
                    RootExceptionHistoryEntry.fromFailureHandlingResultSnapshot(
                            failureHandlingResult);
            exceptionHistory.add(latestRootExceptionEntry);

            log.debug(
                    "Archive local failure causing attempt {} to fail: {}",
                    rootCauseExecution.getAttemptId(),
                    latestRootExceptionEntry.getExceptionAsString());
        } else {
            archiveGlobalFailure(
                    failureHandlingResult.getRootCause(),
                    failureHandlingResult.getTimestamp(),
                    failureHandlingResult.getFailureLabels(),
                    failureHandlingResult.getConcurrentlyFailedExecution());
        }
    }

    // ★ TM 上报 Task 状态变化的入口（JobMaster 收到 RPC 后转发到这里）。注意两点：
    //   attemptId 对应的 Execution 查不到时直接返回 false（迟到的上报被丢掉），
    //   只有真正生效的状态变更才会触发后面的业务钩子。
    @Override
    public boolean updateTaskExecutionState(final TaskExecutionStateTransition taskExecutionState) {

        final ExecutionAttemptID attemptId = taskExecutionState.getID();
        final Execution execution = executionGraph.getRegisteredExecutions().get(attemptId);
        if (execution != null && executionGraph.updateState(taskExecutionState)) {
            onTaskExecutionStateUpdate(execution, taskExecutionState);
            return true;
        }

        return false;
    }

    // ★ 这里解释了"调度器为什么是事件驱动的"：只有 FINISHED / FAILED 两种状态会被转成业务钩子，
    //   其余状态（RUNNING、CANCELED 等）只更新 ExecutionGraph 本身。
    //   下游流水线 region 的调度、失败后的重跑，都分别从这两个分支发起。
    private void onTaskExecutionStateUpdate(
            final Execution execution, final TaskExecutionStateTransition taskExecutionState) {

        // only notifies a state update if it's effective, namely it successfully
        // turns the execution state to the expected value.
        if (execution.getState() != taskExecutionState.getExecutionState()) {
            return;
        }

        // only notifies FINISHED and FAILED states which are needed at the moment.
        // can be refined in FLINK-14233 after the actions are factored out from ExecutionGraph.
        switch (taskExecutionState.getExecutionState()) {
            case FINISHED:
                onTaskFinished(execution, taskExecutionState.getIOMetrics());
                break;
            case FAILED:
                onTaskFailed(execution);
                break;
        }
    }

    protected abstract void onTaskFinished(final Execution execution, final IOMetrics ioMetrics);

    protected abstract void onTaskFailed(final Execution execution);

    @Override
    public SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
        mainThreadExecutor.assertRunningInMainThread();

        return executionGraphHandler.requestNextInputSplit(vertexID, executionAttempt);
    }

    @Override
    public ExecutionState requestPartitionState(
            final IntermediateDataSetID intermediateResultId,
            final ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException {

        mainThreadExecutor.assertRunningInMainThread();

        return executionGraphHandler.requestPartitionState(intermediateResultId, resultPartitionId);
    }

    @VisibleForTesting
    public Iterable<RootExceptionHistoryEntry> getExceptionHistory() {
        return exceptionHistory.toArrayList();
    }

    // Web/REST 查询拿到的是【归档副本】：这里实时快照成 ArchivedExecutionGraph，避免把活的图暴露出去。
    @Override
    public ExecutionGraphInfo requestJob() {
        mainThreadExecutor.assertRunningInMainThread();
        return new ExecutionGraphInfo(
                ArchivedExecutionGraph.createFrom(executionGraph), getExceptionHistory());
    }

    @Override
    public CheckpointStatsSnapshot requestCheckpointStats() {
        mainThreadExecutor.assertRunningInMainThread();
        return executionGraph.getCheckpointStatsSnapshot();
    }

    @Override
    public JobStatus requestJobStatus() {
        return executionGraph.getState();
    }

    @Override
    public KvStateLocation requestKvStateLocation(final JobID jobId, final String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException {
        mainThreadExecutor.assertRunningInMainThread();

        return kvStateHandler.requestKvStateLocation(jobId, registrationName);
    }

    @Override
    public void notifyKvStateRegistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName,
            final KvStateID kvStateId,
            final InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException {
        mainThreadExecutor.assertRunningInMainThread();

        kvStateHandler.notifyKvStateRegistered(
                jobId,
                jobVertexId,
                keyGroupRange,
                registrationName,
                kvStateId,
                kvStateServerAddress);
    }

    @Override
    public void notifyKvStateUnregistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName)
            throws FlinkJobNotFoundException {
        mainThreadExecutor.assertRunningInMainThread();

        kvStateHandler.notifyKvStateUnregistered(
                jobId, jobVertexId, keyGroupRange, registrationName);
    }

    @Override
    public void updateAccumulators(final AccumulatorSnapshot accumulatorSnapshot) {
        mainThreadExecutor.assertRunningInMainThread();

        executionGraph.updateAccumulators(accumulatorSnapshot);
    }

    // 触发保存点（savepoint）：cancelJob=true 时是 cancel-with-savepoint —— 先停掉周期性检查点调度器，
    // 万一触发失败要把它恢复回来，只有保存点成功落盘后才真正 cancel 作业。
    @Override
    public CompletableFuture<String> triggerSavepoint(
            final String targetDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType) {
        mainThreadExecutor.assertRunningInMainThread();

        if (isAnyOutputBlocking(executionGraph)) {
            // TODO: Introduce a more general solution to mark times when
            //  checkpoints are disabled, as well as the detailed reason.
            //  https://issues.apache.org/jira/browse/FLINK-34519
            return FutureUtils.completedExceptionally(
                    new CheckpointException(CheckpointFailureReason.BLOCKING_OUTPUT_EXIST));
        }

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();
        StopWithSavepointTerminationManager.checkSavepointActionPreconditions(
                checkpointCoordinator, targetDirectory, getJobId(), log);

        log.info(
                "Triggering {}savepoint for job {}.",
                cancelJob ? "cancel-with-" : "",
                jobGraph.getJobID());

        if (cancelJob) {
            stopCheckpointScheduler();
        }

        return checkpointCoordinator
                .triggerSavepoint(targetDirectory, formatType)
                .thenApply(CompletedCheckpoint::getExternalPointer)
                .handleAsync(
                        (path, throwable) -> {
                            if (throwable != null) {
                                if (cancelJob) {
                                    startCheckpointScheduler();
                                }
                                throw new CompletionException(throwable);
                            } else if (cancelJob) {
                                log.info(
                                        "Savepoint stored in {}. Now cancelling {}.",
                                        path,
                                        jobGraph.getJobID());
                                cancel();
                            }
                            return path;
                        },
                        mainThreadExecutor);
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType) {
        mainThreadExecutor.assertRunningInMainThread();

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();
        final JobID jobID = jobGraph.getJobID();
        if (checkpointCoordinator == null) {
            throw new IllegalStateException(String.format("Job %s is not a streaming job.", jobID));
        }
        log.info("Triggering a manual checkpoint for job {}.", jobID);

        return checkpointCoordinator
                .triggerCheckpoint(checkpointType)
                .handleAsync(
                        (path, throwable) -> {
                            if (throwable != null) {
                                throw new CompletionException(throwable);
                            }
                            return path;
                        },
                        mainThreadExecutor);
    }

    @Override
    public void stopCheckpointScheduler() {
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator();
        if (checkpointCoordinator == null) {
            log.info(
                    "Periodic checkpoint scheduling could not be stopped due to the CheckpointCoordinator being shutdown.");
        } else {
            checkpointCoordinator.stopCheckpointScheduler();
        }
    }

    @Override
    public void startCheckpointScheduler() {
        mainThreadExecutor.assertRunningInMainThread();
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator();

        if (checkpointCoordinator == null) {
            log.info(
                    "Periodic checkpoint scheduling could not be started due to the CheckpointCoordinator being shutdown.");
        } else if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
            try {
                checkpointCoordinator.startCheckpointScheduler();
            } catch (IllegalStateException ignored) {
                // Concurrent shut down of the coordinator
            }
        }
    }

    @Override
    public void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            final TaskStateSnapshot checkpointState) {

        executionGraphHandler.acknowledgeCheckpoint(
                jobID, executionAttemptID, checkpointId, checkpointMetrics, checkpointState);
    }

    @Override
    public void declineCheckpoint(final DeclineCheckpoint decline) {

        executionGraphHandler.declineCheckpoint(decline);
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID, ExecutionAttemptID attemptId, long id, CheckpointMetrics metrics) {
        executionGraphHandler.reportCheckpointMetrics(attemptId, id, metrics);
    }

    @Override
    public void reportInitializationMetrics(
            JobID jobId, SubTaskInitializationMetrics initializationMetrics) {
        executionGraphHandler.reportInitializationMetrics(initializationMetrics);
    }

    // ★ stop-with-savepoint（优雅停止流作业）：先停周期性检查点调度，再触发【同步】保存点，
    //   然后等所有 Execution 终止；三件事由 StopWithSavepointTerminationManager 编排，
    //   以保证只有同步保存点之前的数据被提交。
    @Override
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final boolean terminate,
            final SavepointFormatType formatType) {
        mainThreadExecutor.assertRunningInMainThread();

        if (isAnyOutputBlocking(executionGraph)) {
            return FutureUtils.completedExceptionally(
                    new CheckpointException(CheckpointFailureReason.BLOCKING_OUTPUT_EXIST));
        }

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();

        StopWithSavepointTerminationManager.checkSavepointActionPreconditions(
                checkpointCoordinator, targetDirectory, executionGraph.getJobID(), log);

        log.info("Triggering stop-with-savepoint for job {}.", jobGraph.getJobID());

        // we stop the checkpoint coordinator so that we are guaranteed
        // to have only the data of the synchronous savepoint committed.
        // in case of failure, and if the job restarts, the coordinator
        // will be restarted by the CheckpointCoordinatorDeActivator.
        stopCheckpointScheduler();

        final CompletableFuture<Collection<ExecutionState>> executionTerminationsFuture =
                getCombinedExecutionTerminationFuture();

        final CompletableFuture<CompletedCheckpoint> savepointFuture =
                checkpointCoordinator.triggerSynchronousSavepoint(
                        terminate, targetDirectory, formatType);

        final StopWithSavepointTerminationManager stopWithSavepointTerminationManager =
                new StopWithSavepointTerminationManager(
                        new StopWithSavepointTerminationHandlerImpl(
                                jobGraph.getJobID(), this, log));

        return stopWithSavepointTerminationManager.stopWithSavepoint(
                savepointFuture, executionTerminationsFuture, mainThreadExecutor);
    }

    /**
     * Returns a {@code CompletableFuture} collecting the termination states of all {@link Execution
     * Executions} of the underlying {@link ExecutionGraph}.
     *
     * @return a {@code CompletableFuture} that completes after all underlying {@code Executions}
     *     have been terminated.
     */
    private CompletableFuture<Collection<ExecutionState>> getCombinedExecutionTerminationFuture() {
        return FutureUtils.combineAll(
                StreamSupport.stream(executionGraph.getAllExecutionVertices().spliterator(), false)
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .map(Execution::getTerminalStateFuture)
                        .collect(Collectors.toList()));
    }

    // ------------------------------------------------------------------------
    //  Operator Coordinators
    //
    //  Note: It may be worthwhile to move the OperatorCoordinators out
    //        of the scheduler (have them owned by the JobMaster directly).
    //        Then we could avoid routing these events through the scheduler and
    //        doing this lazy initialization dance. However, this would require
    //        that the Scheduler does not eagerly construct the CheckpointCoordinator
    //        in the ExecutionGraph and does not eagerly restore the savepoint while
    //        doing that. Because during savepoint restore, the OperatorCoordinators
    //        (or at least their holders) already need to exist, to accept the restored
    //        state. But some components they depend on (Scheduler and MainThreadExecutor)
    //        are not fully usable and accessible at that point.
    // ------------------------------------------------------------------------

    // ★ 跨组件透传点：TM 侧算子发来的 OperatorEvent 经 JobMaster 转到这里，再交给对应的 OperatorCoordinator。
    //   上面那段长注释解释了"为什么算子协调器暂时由调度器托管"，属于历史包袱性的设计。
    @Override
    public void deliverOperatorEventToCoordinator(
            final ExecutionAttemptID taskExecutionId,
            final OperatorID operatorId,
            final OperatorEvent evt)
            throws FlinkException {

        operatorCoordinatorHandler.deliverOperatorEventToCoordinator(
                taskExecutionId, operatorId, evt);
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {

        return operatorCoordinatorHandler.deliverCoordinationRequestToCoordinator(
                operator, request);
    }

    // ★ 有界流的"数据结束"通知：条件是流作业 + 开启检查点 + 允许"任务结束后继续做检查点"。
    //   若某个 JobVertex 的所有 subtask 都收到 EndOfData，就解除该顶点的 backlog 处理标记；
    //   若全作业都结束了，再补触发一次检查点，把"数据已处理完"这个事实固化进检查点。
    @Override
    public void notifyEndOfData(ExecutionAttemptID executionAttemptID) {
        if (jobGraph.getJobType() == JobType.STREAMING
                && jobGraph.isCheckpointingEnabled()
                && jobGraph.getCheckpointingSettings()
                        .getCheckpointCoordinatorConfiguration()
                        .isEnableCheckpointsAfterTasksFinish()) {
            vertexEndOfDataListener.recordTaskEndOfData(executionAttemptID);
            if (vertexEndOfDataListener.areAllTasksOfJobVertexEndOfData(
                    executionAttemptID.getJobVertexId())) {
                List<OperatorIDPair> operatorIDPairs =
                        executionGraph
                                .getJobVertex(executionAttemptID.getJobVertexId())
                                .getOperatorIDs();
                CheckpointCoordinator checkpointCoordinator =
                        executionGraph.getCheckpointCoordinator();
                if (checkpointCoordinator != null) {
                    for (OperatorIDPair operatorIDPair : operatorIDPairs) {
                        checkpointCoordinator.setIsProcessingBacklog(
                                operatorIDPair.getGeneratedOperatorID(), false);
                    }
                }
            }
            if (vertexEndOfDataListener.areAllTasksEndOfData()) {
                triggerCheckpoint(CheckpointType.CONFIGURED);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  access utils for testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    protected JobID getJobId() {
        return jobGraph.getJobID();
    }

    @VisibleForTesting
    VertexEndOfDataListener getVertexEndOfDataListener() {
        return vertexEndOfDataListener;
    }
}
