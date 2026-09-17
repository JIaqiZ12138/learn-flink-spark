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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.failover.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.scheduler.InternalFailuresListener;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.util.OptionalFailure;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

// 【定位】1.20 的 ExecutionGraph 已经“瘦身”，它只负责三件事：
//   ①图结构（顶点 / 中间结果 / 连接关系）②状态（JobStatus、重启次数、失败原因）
//   ③供 Web UI 与归档用的快照（jsonPlan、Accumulator、terminationFuture）。
// 调度决策（槽位分配、failover 策略）与检查点触发（CheckpointCoordinator）都已拆到
//   org.apache.flink.runtime.scheduler / checkpoint 包里，本接口不再“什么都管”。
// 分工：JobMaster 持有它并暴露给 REST，SchedulerBase/DefaultScheduler 驱动它，唯一实现类 DefaultExecutionGraph。
/**
 * The execution graph is the central data structure that coordinates the distributed execution of a
 * data flow. It keeps representations of each parallel task, each intermediate stream, and the
 * communication between them.
 *
 * <p>The execution graph consists of the following constructs:
 *
 * <ul>
 *   <li>The {@link ExecutionJobVertex} represents one vertex from the JobGraph (usually one
 *       operation like "map" or "join") during execution. It holds the aggregated state of all
 *       parallel subtasks. The ExecutionJobVertex is identified inside the graph by the {@link
 *       JobVertexID}, which it takes from the JobGraph's corresponding JobVertex.
 *   <li>The {@link ExecutionVertex} represents one parallel subtask. For each ExecutionJobVertex,
 *       there are as many ExecutionVertices as the parallelism. The ExecutionVertex is identified
 *       by the ExecutionJobVertex and the index of the parallel subtask
 *   <li>The {@link Execution} is one attempt to execute a ExecutionVertex. There may be multiple
 *       Executions for the ExecutionVertex, in case of a failure, or in the case where some data
 *       needs to be recomputed because it is no longer available when requested by later
 *       operations. An Execution is always identified by an {@link ExecutionAttemptID}. All
 *       messages between the JobManager and the TaskManager about deployment of tasks and updates
 *       in the task status always use the ExecutionAttemptID to address the message receiver.
 * </ul>
 */
// 注：本文件是**接口**，没有构造函数；真正的构造参数在 DefaultExecutionGraphBuilder.buildGraph()
//   与 DefaultExecutionGraph 构造函数里。分辨“图数据 vs 运行期依赖”的经验：
//   图数据 = jobInformation、vertexParallelismStore、vertexAttemptNumberStore、部署 / 状态监听器；
//   运行期依赖 = futureExecutor、ioExecutor、rpcTimeout、blobWriter、shuffleMaster、partitionTracker 等。
public interface ExecutionGraph extends AccessExecutionGraph {

    // ★ 调用 start() 之前图就已经建好了（JobMaster 构造期 buildGraph 完成）；start() 只是注入
    //   JobMaster 主线程执行器，让图上的状态变更都在主线程串行执行，避免加锁。
    void start(@Nonnull ComponentMainThreadExecutor jobMasterMainThreadExecutor);

    SchedulingTopology getSchedulingTopology();

    // 由 DefaultExecutionGraphBuilder 在 buildGraph 第 6 步调用，且只有 JobGraph 带
    //   checkpointingSettings 时才会走到；它内部创建 CheckpointCoordinator 并交给 scheduler 使用。
    void enableCheckpointing(
            CheckpointCoordinatorConfiguration chkConfig,
            List<MasterTriggerRestoreHook<?>> masterHooks,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore checkpointStore,
            StateBackend checkpointStateBackend,
            CheckpointStorage checkpointStorage,
            CheckpointStatsTracker statsTracker,
            CheckpointsCleaner checkpointsCleaner,
            String changelogStorage);

    @Nullable
    CheckpointCoordinator getCheckpointCoordinator();

    @Nullable
    CheckpointStatsTracker getCheckpointStatsTracker();

    KvStateLocationRegistry getKvStateLocationRegistry();

    // ★ 易错点：这里存进去的 JSON 是 JsonPlanGenerator 针对 **JobGraph** 生成的（只有 JobVertex /
    //   并行度 / 边），不是 ExecutionGraph 的运行期结构；读取端 getJsonPlan() 定义在父接口
    //   AccessExecutionGraph 上，最终由 ArchivedExecutionGraph 在作业进入终态时随快照留存，供 Web UI 与诊断。
    void setJsonPlan(String jsonPlan);

    Configuration getJobConfiguration();

    Throwable getFailureCause();

    @Override
    Iterable<ExecutionJobVertex> getVerticesTopologically();

    @Override
    Iterable<ExecutionVertex> getAllExecutionVertices();

    @Override
    ExecutionJobVertex getJobVertex(JobVertexID id);

    @Override
    Map<JobVertexID, ExecutionJobVertex> getAllVertices();

    /**
     * Gets the number of restarts, including full restarts and fine grained restarts. If a recovery
     * is currently pending, this recovery is included in the count.
     *
     * @return The number of restarts so far
     */
    long getNumberOfRestarts();

    Map<IntermediateDataSetID, IntermediateResult> getAllIntermediateResults();

    /**
     * Gets the intermediate result partition by the given partition ID, or throw an exception if
     * the partition is not found.
     *
     * @param id of the intermediate result partition
     * @return intermediate result partition
     */
    IntermediateResultPartition getResultPartitionOrThrow(final IntermediateResultPartitionID id);

    /**
     * Merges all accumulator results from the tasks previously executed in the Executions.
     *
     * @return The accumulator map
     */
    Map<String, OptionalFailure<Accumulator<?, ?>>> aggregateUserAccumulators();

    /**
     * Updates the accumulators during the runtime of a job. Final accumulator results are
     * transferred through the UpdateTaskExecutionState message.
     *
     * @param accumulatorSnapshot The serialized flink and user-defined accumulators
     */
    void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot);

    void setInternalTaskFailuresListener(InternalFailuresListener internalTaskFailuresListener);

    // ---- 图结构装配入口（buildGraph 第 5 步调用）----
    // ★ JobVertex → ExecutionJobVertex（1:1），后者再按 parallelism 展开出 N 个 ExecutionVertex，
    //   每个 ExecutionVertex 持有一个 Execution（一次执行尝试，带 attemptNumber，失败后重建）；
    //   JobVertex 的 IntermediateDataSet → IntermediateResult，消费边 → IntermediateResultPartition。
    void attachJobGraph(
            List<JobVertex> topologicallySorted, JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobException;

    void transitionToRunning();

    void cancel();

    /**
     * Suspends the current ExecutionGraph.
     *
     * <p>The JobStatus will be directly set to {@link JobStatus#SUSPENDED} iff the current state is
     * not a terminal state. All ExecutionJobVertices will be canceled and the onTerminalState() is
     * executed.
     *
     * <p>The {@link JobStatus#SUSPENDED} state is a local terminal state which stops the execution
     * of the job but does not remove the job from the HA job store so that it can be recovered by
     * another JobManager.
     *
     * @param suspensionCause Cause of the suspension
     */
    void suspend(Throwable suspensionCause);

    void failJob(Throwable cause, long timestamp);

    /**
     * Returns the termination future of this {@link ExecutionGraph}. The termination future is
     * completed with the terminal {@link JobStatus} once the ExecutionGraph reaches this terminal
     * state and all {@link Execution} have been terminated.
     *
     * @return Termination future of this {@link ExecutionGraph}.
     */
    CompletableFuture<JobStatus> getTerminationFuture();

    @VisibleForTesting
    JobStatus waitUntilTerminal() throws InterruptedException;

    // JobStatus 流转的入口之一：CAS 语义（current → newState），不满足前置状态就返回 false。
    boolean transitionState(JobStatus current, JobStatus newState);

    void incrementRestarts();

    void initFailureCause(Throwable t, long timestamp);

    /**
     * Updates the state of one of the ExecutionVertex's Execution attempts. If the new status if
     * "FINISHED", this also updates the accumulators.
     *
     * @param state The state update.
     * @return True, if the task update was properly applied, false, if the execution attempt was
     *     not found.
     */
    boolean updateState(TaskExecutionStateTransition state);

    // attemptId → Execution 的全局索引：JobManager 收到 TaskManager 的执行状态消息时靠它反查执行尝试。
    Map<ExecutionAttemptID, Execution> getRegisteredExecutions();

    void registerJobStatusListener(JobStatusListener listener);

    ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker();

    int getNumFinishedVertices();

    @Nonnull
    ComponentMainThreadExecutor getJobMasterMainThreadExecutor();

    // 顶点在 1.20 被拆成“先 attach 后 initialize”两步，这是为动态图（adaptive scheduler）留的口子：
    //   普通图在 attachJobGraph 里一次性初始化完，动态图则等上游结果确定后再逐个调用本方法。
    default void initializeJobVertex(ExecutionJobVertex ejv, long createTimestamp)
            throws JobException {
        initializeJobVertex(
                ejv,
                createTimestamp,
                VertexInputInfoComputationUtils.computeVertexInputInfos(
                        ejv, getAllIntermediateResults()::get));
    }

    /**
     * Initialize the given execution job vertex, mainly includes creating execution vertices
     * according to the parallelism, and connecting to the predecessors.
     *
     * @param ejv The execution job vertex that needs to be initialized.
     * @param createTimestamp The timestamp for creating execution vertices, used to initialize the
     *     first Execution with.
     * @param jobVertexInputInfos The input infos of this job vertex.
     */
    void initializeJobVertex(
            ExecutionJobVertex ejv,
            long createTimestamp,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos)
            throws JobException;

    /**
     * Notify that some job vertices have been newly initialized, execution graph will try to update
     * scheduling topology.
     *
     * @param vertices The execution job vertices that are newly initialized.
     */
    void notifyNewlyInitializedJobVertices(List<ExecutionJobVertex> vertices);

    Optional<String> findVertexWithAttempt(final ExecutionAttemptID attemptId);

    Optional<AccessExecution> findExecution(final ExecutionAttemptID attemptId);
}
