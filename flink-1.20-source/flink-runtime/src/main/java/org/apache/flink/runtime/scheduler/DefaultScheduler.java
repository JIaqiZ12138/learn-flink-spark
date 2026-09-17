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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricher.Context;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.failover.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.failure.DefaultFailureEnricherContext;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.scheduler.exceptionhistory.FailureHandlingResultSnapshot;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The future default scheduler. */
// ★ DefaultScheduler —— Flink 1.20 的默认调度器，回答"哪个 ExecutionVertex 在何时被部署到哪个 slot 上"。
//   入口链：JobMaster.startScheduling() → SchedulerNG.startScheduling()（final 实现在 SchedulerBase）
//        → startSchedulingInternal() → SchedulingStrategy → allocateSlotsAndDeploy() → ExecutionDeployer。
//   与 AdaptiveScheduler 的差别：本类按提交时确定的并发度调度，运行期不做自适应扩缩容；AdaptiveScheduler
//   自己实现 SchedulerNG 并带一套 Created/WaitingForResources 状态机，并不复用 SchedulerBase 这套骨架。
public class DefaultScheduler extends SchedulerBase implements SchedulerOperations {

    protected final Logger log;

    private final ClassLoader userCodeLoader;

    // ★ 逻辑 slot 分配器：背后是 PhysicalSlotProvider → SlotPool；真正的 slot 账本在 JobMaster 的 SlotPoolService 手里。
    protected final ExecutionSlotAllocator executionSlotAllocator;

    private final ExecutionFailureHandler executionFailureHandler;

    private final ScheduledExecutor delayExecutor;

    // ★ 调度策略：决定"下一批该部署哪些顶点"，默认实现是 PipelinedRegionSchedulingStrategy（按流水线 region 整体调度）。
    protected final SchedulingStrategy schedulingStrategy;

    private final ExecutionOperations executionOperations;

    private final Set<ExecutionVertexID> verticesWaitingForRestart;

    protected final ShuffleMaster<?> shuffleMaster;

    private final Map<AllocationID, Long> reservedAllocationRefCounters;

    // once an execution vertex is assigned an allocation/slot, it will reserve the allocation
    // until it is assigned a new allocation, or it finishes and does not need the allocation
    // anymore. The reserved allocation information is needed for local recovery.
    private final Map<ExecutionVertexID, AllocationID> reservedAllocationByExecutionVertex;

    // ★ 部署协调器：真正把 Execution 推给 TM 的一层（申请 slot、等 slot 到位、校验顶点版本后调 deploy()）。
    protected final ExecutionDeployer executionDeployer;

    // 失败恢复策略：决定失败后要重跑"哪些顶点"（region failover / full failover 等），按调度拓扑创建。
    protected final FailoverStrategy failoverStrategy;

    protected DefaultScheduler(
            final Logger log,
            final JobGraph jobGraph,
            final Executor ioExecutor,
            final Configuration jobMasterConfiguration,
            final Consumer<ComponentMainThreadExecutor> startUpAction,
            final ScheduledExecutor delayExecutor,
            final ClassLoader userCodeLoader,
            final CheckpointsCleaner checkpointsCleaner,
            final CheckpointRecoveryFactory checkpointRecoveryFactory,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup,
            final SchedulingStrategyFactory schedulingStrategyFactory,
            final FailoverStrategy.Factory failoverStrategyFactory,
            final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
            final ExecutionOperations executionOperations,
            final ExecutionVertexVersioner executionVertexVersioner,
            final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory,
            long initializationTimestamp,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final JobStatusListener jobStatusListener,
            final Collection<FailureEnricher> failureEnrichers,
            final ExecutionGraphFactory executionGraphFactory,
            final ShuffleMaster<?> shuffleMaster,
            final Time rpcTimeout,
            final VertexParallelismStore vertexParallelismStore,
            final ExecutionDeployer.Factory executionDeployerFactory)
            throws Exception {

        // ★ 先由 SchedulerBase 建好 ExecutionGraph / SchedulingTopology 等公共状态（图的创建与恢复就在这次 super 里）。
        super(
                log,
                jobGraph,
                ioExecutor,
                jobMasterConfiguration,
                checkpointsCleaner,
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                executionVertexVersioner,
                initializationTimestamp,
                mainThreadExecutor,
                jobStatusListener,
                executionGraphFactory,
                vertexParallelismStore);

        this.log = log;

        this.delayExecutor = checkNotNull(delayExecutor);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.executionOperations = checkNotNull(executionOperations);
        this.shuffleMaster = checkNotNull(shuffleMaster);

        this.reservedAllocationRefCounters = new HashMap<>();
        this.reservedAllocationByExecutionVertex = new HashMap<>();

        this.failoverStrategy =
                failoverStrategyFactory.create(
                        getSchedulingTopology(), getResultPartitionAvailabilityChecker());
        log.info(
                "Using failover strategy {} for {} ({}).",
                failoverStrategy,
                jobGraph.getName(),
                jobGraph.getJobID());

        final Context taskFailureCtx =
                DefaultFailureEnricherContext.forTaskFailure(
                        this.jobInfo, jobManagerJobMetricGroup, ioExecutor, userCodeLoader);

        final Context globalFailureCtx =
                DefaultFailureEnricherContext.forGlobalFailure(
                        this.jobInfo, jobManagerJobMetricGroup, ioExecutor, userCodeLoader);

        this.executionFailureHandler =
                new ExecutionFailureHandler(
                        jobMasterConfiguration,
                        getSchedulingTopology(),
                        failoverStrategy,
                        restartBackoffTimeStrategy,
                        mainThreadExecutor,
                        failureEnrichers,
                        taskFailureCtx,
                        globalFailureCtx,
                        jobManagerJobMetricGroup);

        this.schedulingStrategy =
                schedulingStrategyFactory.createInstance(this, getSchedulingTopology());

        this.executionSlotAllocator =
                checkNotNull(executionSlotAllocatorFactory)
                        .createInstance(new DefaultExecutionSlotAllocationContext());

        this.verticesWaitingForRestart = new HashSet<>();
        // startUpAction 实际是 PhysicalSlotRequestBulkChecker.start()（见 DefaultSchedulerComponents），构造中途就启动。
        startUpAction.accept(mainThreadExecutor);

        // executionDeployer 放在最后创建：它依赖上面已建好的 executionSlotAllocator 与 startReserveAllocation 回调。
        this.executionDeployer =
                executionDeployerFactory.createInstance(
                        log,
                        executionSlotAllocator,
                        executionOperations,
                        executionVertexVersioner,
                        rpcTimeout,
                        this::startReserveAllocation,
                        mainThreadExecutor);
    }

    // ------------------------------------------------------------------------
    // SchedulerNG
    // ------------------------------------------------------------------------

    @Override
    protected long getNumberOfRestarts() {
        return executionFailureHandler.getNumberOfRestarts();
    }

    @Override
    protected void cancelAllPendingSlotRequestsInternal() {
        getSchedulingTopology()
                .getVertices()
                .forEach(ev -> cancelAllPendingSlotRequestsForVertex(ev.getId()));
    }

    // ★ 调度的真正起点（由 SchedulerBase.startScheduling() 调用，那一层已注册好指标并启动了 OperatorCoordinator）。
    //   本方法自身不含任何决策逻辑，只做两件事：① transitionToRunning() 把作业状态推到 RUNNING，
    //   Web UI 上"作业开始运行"就是这一步；② 把"调度谁、按什么顺序"全权委托给 SchedulingStrategy。
    //   默认策略 PipelinedRegionSchedulingStrategy 会找出源 region（没有跨 region 输入的流水线 region），
    //   按拓扑序 scheduleRegion() → SchedulerOperations.allocateSlotsAndDeploy()，于是又回到本类。
    @Override
    protected void startSchedulingInternal() {
        log.info(
                "Starting scheduling with scheduling strategy [{}]",
                schedulingStrategy.getClass().getName());
        transitionToRunning();
        schedulingStrategy.startScheduling();
    }

    // ★ 事件驱动入口：调度器没有轮询线程，"什么时候调度下一批"完全靠 TM 上报的状态变化推着走。
    //   TM 的上报要经 SchedulerBase.updateTaskExecutionState() → onTaskExecutionStateUpdate() 过滤后才会走到这里。
    @Override
    protected void onTaskFinished(final Execution execution, final IOMetrics ioMetrics) {
        checkState(execution.getState() == ExecutionState.FINISHED);

        final ExecutionVertexID executionVertexId = execution.getVertex().getID();
        // once a task finishes, it will release the assigned allocation/slot and no longer
        // needs it. Therefore, it should stop reserving the slot so that other tasks are
        // possible to use the slot. Ideally, the `stopReserveAllocation` should happen
        // along with the release slot process. However, that process is hidden in the depth
        // of the ExecutionGraph, so we currently do it in DefaultScheduler after that process
        // is done.
        stopReserveAllocation(executionVertexId);

        // 通知调度策略：该顶点已完成，它下游被阻塞的 region 现在可能可以开始调度了（是否真的可调度由策略判断）。
        schedulingStrategy.onExecutionStateChange(executionVertexId, ExecutionState.FINISHED);
    }

    // ★ 失败处理入口：ExecutionGraph 只负责把状态置为 FAILED 并回调这里，
    //   "是否重启、重启哪些顶点"由本类 + FailoverStrategy + RestartBackoffTimeStrategy 共同决定。
    @Override
    protected void onTaskFailed(final Execution execution) {
        checkState(execution.getState() == ExecutionState.FAILED);
        checkState(execution.getFailureInfo().isPresent());

        final Throwable error =
                execution.getFailureInfo().get().getException().deserializeError(userCodeLoader);
        handleTaskFailure(
                execution,
                maybeTranslateToClusterDatasetException(error, execution.getVertex().getID()));
    }

    protected void handleTaskFailure(
            final Execution failedExecution, @Nullable final Throwable error) {
        maybeRestartTasks(recordTaskFailure(failedExecution, error));
    }

    protected FailureHandlingResult recordTaskFailure(
            final Execution failedExecution, @Nullable final Throwable error) {
        final long timestamp = System.currentTimeMillis();
        setGlobalFailureCause(error, timestamp);
        notifyCoordinatorsAboutTaskFailure(failedExecution, error);

        return executionFailureHandler.getFailureHandlingResult(failedExecution, error, timestamp);
    }

    private Throwable maybeTranslateToClusterDatasetException(
            @Nullable Throwable cause, ExecutionVertexID failedVertex) {
        if (!(cause instanceof PartitionException)) {
            return cause;
        }

        final List<IntermediateDataSetID> intermediateDataSetIdsToConsume =
                getExecutionJobVertex(failedVertex.getJobVertexId())
                        .getJobVertex()
                        .getIntermediateDataSetIdsToConsume();
        final IntermediateResultPartitionID failedPartitionId =
                ((PartitionException) cause).getPartitionId().getPartitionId();

        if (!intermediateDataSetIdsToConsume.contains(
                failedPartitionId.getIntermediateDataSetID())) {
            return cause;
        }

        return new ClusterDatasetCorruptedException(
                cause, Collections.singletonList(failedPartitionId.getIntermediateDataSetID()));
    }

    protected void notifyCoordinatorsAboutTaskFailure(
            final Execution execution, @Nullable final Throwable error) {
        final ExecutionJobVertex jobVertex = execution.getVertex().getJobVertex();
        final int subtaskIndex = execution.getParallelSubtaskIndex();
        final int attemptNumber = execution.getAttemptNumber();

        jobVertex
                .getOperatorCoordinators()
                .forEach(c -> c.executionAttemptFailed(subtaskIndex, attemptNumber, error));
    }

    @Override
    public void handleGlobalFailure(final Throwable error) {
        final long timestamp = System.currentTimeMillis();
        setGlobalFailureCause(error, timestamp);

        log.info("Trying to recover from a global failure.", error);
        final FailureHandlingResult failureHandlingResult =
                executionFailureHandler.getGlobalFailureHandlingResult(error, timestamp);
        maybeRestartTasks(failureHandlingResult);
    }

    protected void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
        if (failureHandlingResult.canRestart()) {
            restartTasksWithDelay(failureHandlingResult);
        } else {
            failJob(
                    failureHandlingResult.getError(),
                    failureHandlingResult.getTimestamp(),
                    failureHandlingResult.getFailureLabels());
        }
    }

    // ★ failover 的核心路径，注意它是【异步 + 延迟】的：把失败顶点先标成"等待重启"（作业状态 RUNNING→RESTARTING），
    //   再异步取消它们的当前执行，最后等 restartDelayMS 之后才真正重启 —— 退避是为了避免立刻重试把集群打爆。
    private void restartTasksWithDelay(final FailureHandlingResult failureHandlingResult) {
        final Set<ExecutionVertexID> verticesToRestart =
                failureHandlingResult.getVerticesToRestart();

        final Set<ExecutionVertexVersion> executionVertexVersions =
                new HashSet<>(
                        executionVertexVersioner
                                .recordVertexModifications(verticesToRestart)
                                .values());
        final boolean globalRecovery = failureHandlingResult.isGlobalFailure();

        if (globalRecovery) {
            log.info(
                    "{} tasks will be restarted to recover from a global failure.",
                    verticesToRestart.size());
        } else {
            checkArgument(failureHandlingResult.getFailedExecution().isPresent());
            log.info(
                    "{} tasks will be restarted to recover the failed task {}.",
                    verticesToRestart.size(),
                    failureHandlingResult.getFailedExecution().get().getAttemptId());
        }

        addVerticesToRestartPending(verticesToRestart);

        final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);

        archiveFromFailureHandlingResult(
                createFailureHandlingResultSnapshot(failureHandlingResult));
        // 用 delayExecutor 而不是主线程 executor：退避等待期间绝不能阻塞 JobMaster 主线程。
        delayExecutor.schedule(
                () ->
                        FutureUtils.assertNoException(
                                cancelFuture.thenRunAsync(
                                        () -> restartTasks(executionVertexVersions, globalRecovery),
                                        getMainThreadExecutor())),
                failureHandlingResult.getRestartDelayMS(),
                TimeUnit.MILLISECONDS);
    }

    protected FailureHandlingResultSnapshot createFailureHandlingResultSnapshot(
            final FailureHandlingResult failureHandlingResult) {
        return FailureHandlingResultSnapshot.create(
                failureHandlingResult, id -> getExecutionVertex(id).getCurrentExecutions());
    }

    private void addVerticesToRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
        verticesWaitingForRestart.addAll(verticesToRestart);
        transitionExecutionGraphState(JobStatus.RUNNING, JobStatus.RESTARTING);
    }

    private void removeVerticesFromRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
        verticesWaitingForRestart.removeAll(verticesToRestart);
        if (verticesWaitingForRestart.isEmpty()) {
            transitionExecutionGraphState(JobStatus.RESTARTING, JobStatus.RUNNING);
        }
    }

    private void restartTasks(
            final Set<ExecutionVertexVersion> executionVertexVersions,
            final boolean isGlobalRecovery) {
        final Set<ExecutionVertexID> verticesToRestart =
                executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

        if (verticesToRestart.isEmpty()) {
            return;
        }

        removeVerticesFromRestartPending(verticesToRestart);

        resetForNewExecutions(verticesToRestart);

        try {
            restoreState(verticesToRestart, isGlobalRecovery);
        } catch (Throwable t) {
            handleGlobalFailure(t);
            return;
        }

        // 重启同样交回调度策略：状态恢复完成后，由策略决定这些顶点接下来怎么重新调度。
        schedulingStrategy.restartTasks(verticesToRestart);
    }

    // ★ 取消之前必须先撤掉这些顶点的 pending slot 请求，否则刚被释放的 slot 会被这些"即将作废"的请求抢走。
    private CompletableFuture<?> cancelTasksAsync(final Set<ExecutionVertexID> verticesToRestart) {
        // clean up all the related pending requests to avoid that immediately returned slot
        // is used to fulfill the pending requests of these tasks
        cancelAllPendingSlotRequestsForVertices(verticesToRestart);

        final List<CompletableFuture<?>> cancelFutures =
                verticesToRestart.stream()
                        .map(this::cancelExecutionVertex)
                        .collect(Collectors.toList());

        return FutureUtils.combineAll(cancelFutures);
    }

    private CompletableFuture<?> cancelExecutionVertex(final ExecutionVertexID executionVertexId) {
        return FutureUtils.combineAll(
                getExecutionVertex(executionVertexId).getCurrentExecutions().stream()
                        .map(this::cancelExecution)
                        .collect(Collectors.toList()));
    }

    protected CompletableFuture<?> cancelExecution(final Execution execution) {
        notifyCoordinatorOfCancellation(execution);
        return executionOperations.cancel(execution);
    }

    private void cancelAllPendingSlotRequestsForVertices(
            final Set<ExecutionVertexID> executionVertices) {
        executionVertices.forEach(this::cancelAllPendingSlotRequestsForVertex);
    }

    protected void cancelAllPendingSlotRequestsForVertex(
            final ExecutionVertexID executionVertexId) {
        getExecutionVertex(executionVertexId)
                .getCurrentExecutions()
                .forEach(e -> executionSlotAllocator.cancel(e.getAttemptId()));
    }

    private Execution getCurrentExecutionOfVertex(ExecutionVertexID executionVertexId) {
        return getExecutionVertex(executionVertexId).getCurrentExecutionAttempt();
    }

    // ------------------------------------------------------------------------
    // SchedulerOperations
    // ------------------------------------------------------------------------

    // ★★ 【调度决策 → 真正下发】的分界点。调用方是 SchedulingStrategy.scheduleRegion()，
    //    参数是一整批要部署的顶点（同一个流水线 region 内的全部顶点）。
    //    第 1 步：先给这批顶点记录版本号 —— 之后若发生 failover / cancel，版本变化会让这次部署
    //           被判为"过期"而丢弃，防止迟到的部署打到新的 Execution 上。
    //    第 2 步：把 ExecutionVertexID 换成各自的"当前 Execution"（只是取当前尝试，不新建对象）。
    //    第 3 步：交给 ExecutionDeployer.allocateSlotsAndDeploy()，那里才是真正的三段式：
    //            a) ExecutionSlotAllocator 申请逻辑 slot（→ PhysicalSlotProvider → SlotPool）；
    //            b) 等所有 slot 到位 —— 这一步是【异步】的，可能要等 ResourceManager 拉起新 TM；
    //            c) 逐个调用 Execution.deploy() → TaskManagerGateway.submitTask() 真正下发到 TM。
    //    所以本方法只有四行，却是"决策"与"执行"两大阶段的接缝。
    @Override
    public void allocateSlotsAndDeploy(final List<ExecutionVertexID> verticesToDeploy) {
        final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
                executionVertexVersioner.recordVertexModifications(verticesToDeploy);

        final List<Execution> executionsToDeploy =
                verticesToDeploy.stream()
                        .map(this::getCurrentExecutionOfVertex)
                        .collect(Collectors.toList());

        executionDeployer.allocateSlotsAndDeploy(executionsToDeploy, requiredVersionByVertex);
    }

    private void startReserveAllocation(
            ExecutionVertexID executionVertexId, AllocationID newAllocation) {

        // stop the previous allocation reservation if there is one
        stopReserveAllocation(executionVertexId);

        reservedAllocationByExecutionVertex.put(executionVertexId, newAllocation);
        reservedAllocationRefCounters.compute(
                newAllocation, (ignored, oldCount) -> oldCount == null ? 1 : oldCount + 1);
    }

    private void stopReserveAllocation(ExecutionVertexID executionVertexId) {
        final AllocationID priorAllocation =
                reservedAllocationByExecutionVertex.remove(executionVertexId);
        if (priorAllocation != null) {
            reservedAllocationRefCounters.compute(
                    priorAllocation, (ignored, oldCount) -> oldCount > 1 ? oldCount - 1 : null);
        }
    }

    private void notifyCoordinatorOfCancellation(Execution execution) {
        // this method makes a best effort to filter out duplicate notifications, meaning cases
        // where the coordinator was already notified for that specific task
        // we don't notify if the task is already FAILED, CANCELLING, or CANCELED
        final ExecutionState currentState = execution.getState();
        if (currentState == ExecutionState.FAILED
                || currentState == ExecutionState.CANCELING
                || currentState == ExecutionState.CANCELED) {
            return;
        }

        notifyCoordinatorsAboutTaskFailure(execution, null);
    }

    // 调度器暴露给 slot 分配层的查询上下文：顶点资源需求、上一次 allocation、输入位置、可共享的
    // SlotSharingGroup / CoLocationGroup 等，全部实时从 ExecutionGraph / JobGraph 查，不做缓存。
    private class DefaultExecutionSlotAllocationContext implements ExecutionSlotAllocationContext {

        @Override
        public ResourceProfile getResourceProfile(final ExecutionVertexID executionVertexId) {
            return getExecutionVertex(executionVertexId).getResourceProfile();
        }

        @Override
        public Optional<AllocationID> findPriorAllocationId(
                final ExecutionVertexID executionVertexId) {
            return getExecutionVertex(executionVertexId).findLastAllocation();
        }

        @Override
        public SchedulingTopology getSchedulingTopology() {
            return DefaultScheduler.this.getSchedulingTopology();
        }

        @Override
        public Set<SlotSharingGroup> getLogicalSlotSharingGroups() {
            return getJobGraph().getSlotSharingGroups();
        }

        @Override
        public Set<CoLocationGroup> getCoLocationGroups() {
            return getJobGraph().getCoLocationGroups();
        }

        @Override
        public Collection<ConsumedPartitionGroup> getConsumedPartitionGroups(
                ExecutionVertexID executionVertexId) {
            return inputsLocationsRetriever.getConsumedPartitionGroups(executionVertexId);
        }

        @Override
        public Collection<ExecutionVertexID> getProducersOfConsumedPartitionGroup(
                ConsumedPartitionGroup consumedPartitionGroup) {
            return inputsLocationsRetriever.getProducersOfConsumedPartitionGroup(
                    consumedPartitionGroup);
        }

        @Override
        public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(
                ExecutionVertexID executionVertexId) {
            return inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);
        }

        @Override
        public Optional<TaskManagerLocation> getStateLocation(ExecutionVertexID executionVertexId) {
            return stateLocationRetriever.getStateLocation(executionVertexId);
        }

        @Override
        public Set<AllocationID> getReservedAllocations() {
            return reservedAllocationRefCounters.keySet();
        }
    }
}
