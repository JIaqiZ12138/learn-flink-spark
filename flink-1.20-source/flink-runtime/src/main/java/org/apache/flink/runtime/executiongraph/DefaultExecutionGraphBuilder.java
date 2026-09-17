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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.failover.partitionrelease.PartitionGroupReleaseStrategy;
import org.apache.flink.runtime.executiongraph.failover.partitionrelease.PartitionGroupReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.apache.flink.configuration.StateChangelogOptions.STATE_CHANGE_LOG_STORAGE;
import static org.apache.flink.util.Preconditions.checkNotNull;

// 【核心入口】JobGraph → ExecutionGraph 的唯一构建入口；本类只做“组装”，不做调度。
// ★ 整条链路都在 JobMaster 的构造函数里同步走完（不是 start() 时）：
//   JobMaster.<init> → createScheduler() → DefaultSchedulerFactory.createInstance()
//     → new DefaultScheduler → SchedulerBase.<init> → ExecutionGraphFactory → buildGraph()（本方法）
// 所以 JobMaster 构造结束时执行图已全量展开；submitJob 之后只是把它调度起来。
// buildGraph() 内按顺序做 6 件事：①基础信息与运行期依赖 ②new 空壳执行图 ③setJsonPlan
//   ④master 端初始化 hook ⑤拓扑排序 + attachJobGraph（顶点在这里才展开）⑥检查点/状态后端装配
/**
 * Utility class to encapsulate the logic of building an {@link DefaultExecutionGraph} from a {@link
 * JobGraph}.
 */
public class DefaultExecutionGraphBuilder {

    public static DefaultExecutionGraph buildGraph(
            JobGraph jobGraph,
            Configuration jobManagerConfig,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            ClassLoader classLoader,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            Time rpcTimeout,
            BlobWriter blobWriter,
            Logger log,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
            ExecutionDeploymentListener executionDeploymentListener,
            ExecutionStateUpdateListener executionStateUpdateListener,
            long initializationTimestamp,
            VertexAttemptNumberStore vertexAttemptNumberStore,
            VertexParallelismStore vertexParallelismStore,
            Supplier<CheckpointStatsTracker> checkpointStatsTrackerFactory,
            boolean isDynamicGraph,
            ExecutionJobVertex.Factory executionJobVertexFactory,
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            boolean nonFinishedHybridPartitionShouldBeUnknown,
            JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobExecutionException, JobException {

        checkNotNull(jobGraph, "job graph cannot be null");

        final String jobName = jobGraph.getName();
        final JobID jobId = jobGraph.getJobID();
        final JobType jobType = jobGraph.getJobType();

        // ---- 第 1 步：准备图外的基础信息与运行期依赖（全在 JobManager 主线程里同步做完）----
        // JobInformation 随后会被序列化并随 TDD 发往 TaskManager，见下面的 TaskDeploymentDescriptorFactory。
        final JobInformation jobInformation =
                new JobInformation(
                        jobId,
                        jobType,
                        jobName,
                        jobGraph.getSerializedExecutionConfig(),
                        jobGraph.getJobConfiguration(),
                        jobGraph.getUserJarBlobKeys(),
                        jobGraph.getClasspaths());

        final int executionHistorySizeLimit =
                jobManagerConfig.get(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);

        final PartitionGroupReleaseStrategy.Factory partitionGroupReleaseStrategyFactory =
                PartitionGroupReleaseStrategyFactoryLoader.loadPartitionGroupReleaseStrategyFactory(
                        jobManagerConfig);

        final int offloadShuffleDescriptorsThreshold =
                jobManagerConfig.get(
                        TaskDeploymentDescriptorFactory.OFFLOAD_SHUFFLE_DESCRIPTORS_THRESHOLD);

        final TaskDeploymentDescriptorFactory taskDeploymentDescriptorFactory;
        try {
            taskDeploymentDescriptorFactory =
                    new TaskDeploymentDescriptorFactory(
                            BlobWriter.serializeAndTryOffload(jobInformation, jobId, blobWriter),
                            jobId,
                            partitionLocationConstraint,
                            blobWriter,
                            nonFinishedHybridPartitionShouldBeUnknown,
                            offloadShuffleDescriptorsThreshold);
        } catch (IOException e) {
            throw new JobException("Could not create the TaskDeploymentDescriptorFactory.", e);
        }

        // ---- 第 2 步：先 new 出一个“空壳”执行图 ----
        // ★ 此刻图里还没有任何顶点：顶点是第 5 步 attachJobGraph 才挂上去的。
        // ★ vertexParallelismStore / vertexAttemptNumberStore 由参数传入，并行度与 maxParallelism
        //   的解析在调用方（SchedulerBase.computeVertexParallelismStore()）就已经做完了。
        // create a new execution graph, if none exists so far
        final DefaultExecutionGraph executionGraph =
                new DefaultExecutionGraph(
                        jobGraph.getJobType(),
                        jobInformation,
                        futureExecutor,
                        ioExecutor,
                        rpcTimeout,
                        executionHistorySizeLimit,
                        classLoader,
                        blobWriter,
                        partitionGroupReleaseStrategyFactory,
                        shuffleMaster,
                        partitionTracker,
                        executionDeploymentListener,
                        executionStateUpdateListener,
                        initializationTimestamp,
                        vertexAttemptNumberStore,
                        vertexParallelismStore,
                        isDynamicGraph,
                        executionJobVertexFactory,
                        jobGraph.getJobStatusHooks(),
                        markPartitionFinishedStrategy,
                        taskDeploymentDescriptorFactory);

        // ---- 第 3 步：设置 jsonPlan（Web UI 的 Plan 页、REST /jobs/:id/plan 用它）----
        // ★ 易错点：JsonPlanGenerator.generatePlan(jobGraph) 生成的是 **JobGraph** 的 JSON，
        //   只有 JobVertex / 并行度 / 边，没有 ExecutionVertex、Execution 这些运行期结构；
        //   ArchivedExecutionGraph.getJsonPlan() 返回的就是这里塞进去的字符串。
        // 生成失败不抛出，降级成空 JSON "{}"，保证作业仍能继续提交。
        // set the basic properties

        try {
            executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
        } catch (Throwable t) {
            log.warn("Cannot create JSON plan for job", t);
            // give the graph an empty plan
            executionGraph.setJsonPlan("{}");
        }

        // ---- 第 4 步：master 端初始化 hook（file sink 在这里建目录、InputFormat 在这里切 split）----
        // ★ 这是必须在 JobManager 侧先执行的副作用，失败即 JobExecutionException，任务无法部署。
        // initialize the vertices that have a master initialization hook
        // file output formats create directories here, input formats create splits

        final long initMasterStart = System.nanoTime();
        log.info("Running initialization on master for job {} ({}).", jobName, jobId);

        for (JobVertex vertex : jobGraph.getVertices()) {
            String executableClass = vertex.getInvokableClassName();
            if (executableClass == null || executableClass.isEmpty()) {
                throw new JobSubmissionException(
                        jobId,
                        "The vertex "
                                + vertex.getID()
                                + " ("
                                + vertex.getName()
                                + ") has no invokable class.");
            }

            // ★ 这里只取“已解析好”的并行度：JobVertex 里 maxParallelism = -1
            //   （JobVertex.MAX_PARALLELISM_DEFAULT）表示“用户未设置”，本文件并不解析它；
            //   解析发生在上游 SchedulerBase.computeVertexParallelismStore()：
            //   默认取下界 DEFAULT_LOWER_BOUND_MAX_PARALLELISM = 1 << 7 = 128。
            try {
                vertex.initializeOnMaster(
                        new SimpleInitializeOnMasterContext(
                                classLoader,
                                vertexParallelismStore
                                        .getParallelismInfo(vertex.getID())
                                        .getParallelism()));
            } catch (Throwable t) {
                throw new JobExecutionException(
                        jobId,
                        "Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(),
                        t);
            }
        }

        log.info(
                "Successfully ran initialization on master in {} ms.",
                (System.nanoTime() - initMasterStart) / 1_000_000);

        // ---- 第 5 步：拓扑排序后 attachJobGraph —— 顶点在这里才真正被展开 ----
        // ★ 展开关系发生在 DefaultExecutionGraph.attachJobGraph() 内部：
        //   JobVertex → ExecutionJobVertex（1:1）→ 按 parallelism 展开成 N 个 ExecutionVertex
        //   → 每个 ExecutionVertex 持有 currentExecution（一次执行尝试，带 attemptNumber，失败后重建）；
        //   JobVertex 的 IntermediateDataSet → IntermediateResult，消费边 → IntermediateResultPartition。
        // ★ 必须先拓扑排序：下游 ExecutionVertex 初始化时要 connectToPredecessors() 连到已建好的上游。
        // topologically sort the job vertices and attach the graph to the existing one
        List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
        if (log.isDebugEnabled()) {
            log.debug(
                    "Adding {} vertices from job graph {} ({}).",
                    sortedTopology.size(),
                    jobName,
                    jobId);
        }
        executionGraph.attachJobGraph(sortedTopology, jobManagerJobMetricGroup);

        if (log.isDebugEnabled()) {
            log.debug(
                    "Successfully created execution graph from job graph {} ({}).", jobName, jobId);
        }

        // ---- 第 6 步：检查点 / 状态后端装配 ----
        // 是否开启只看 JobGraph.getCheckpointingSettings() 是否为 null，它由客户端（StreamGraph → JobGraph）
        //   阶段决定，JobManager 端没有独立开关；动态图（adaptive scheduler 会改并行度）不支持检查点，跳过。
        // configure the state checkpointing
        if (isDynamicGraph) {
            // dynamic graph does not support checkpointing so we skip it
            log.warn("Skip setting up checkpointing for a job with dynamic graph.");
        } else if (isCheckpointingEnabled(jobGraph)) {
            JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();

            // 优先级链：应用内序列化进来的 state backend（checkpointingSettings）→ JobGraph/集群配置 → 默认值；
            //   ★ “代码里 setStateBackend 覆盖集群配置”就发生在这里，checkpoint storage 用的是同一套优先级。
            // load the state backend from the application settings
            final StateBackend applicationConfiguredBackend;
            final SerializedValue<StateBackend> serializedAppConfigured =
                    snapshotSettings.getDefaultStateBackend();

            if (serializedAppConfigured == null) {
                applicationConfiguredBackend = null;
            } else {
                try {
                    applicationConfiguredBackend =
                            serializedAppConfigured.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not deserialize application-defined state backend.", e);
                }
            }

            final StateBackend rootBackend;
            try {
                rootBackend =
                        StateBackendLoader.fromApplicationOrConfigOrDefault(
                                applicationConfiguredBackend,
                                jobGraph.getJobConfiguration(),
                                jobManagerConfig,
                                classLoader,
                                log);
            } catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(
                        jobId, "Could not instantiate configured state backend", e);
            }

            // load the checkpoint storage from the application settings
            final CheckpointStorage applicationConfiguredStorage;
            final SerializedValue<CheckpointStorage> serializedAppConfiguredStorage =
                    snapshotSettings.getDefaultCheckpointStorage();

            if (serializedAppConfiguredStorage == null) {
                applicationConfiguredStorage = null;
            } else {
                try {
                    applicationConfiguredStorage =
                            serializedAppConfiguredStorage.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId,
                            "Could not deserialize application-defined checkpoint storage.",
                            e);
                }
            }

            final CheckpointStorage rootStorage;
            try {
                rootStorage =
                        CheckpointStorageLoader.load(
                                applicationConfiguredStorage,
                                rootBackend,
                                jobGraph.getJobConfiguration(),
                                jobManagerConfig,
                                classLoader,
                                log);
            } catch (IllegalConfigurationException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(
                        jobId, "Could not instantiate configured checkpoint storage", e);
            }

            // instantiate the user-defined checkpoint hooks

            final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks =
                    snapshotSettings.getMasterHooks();
            final List<MasterTriggerRestoreHook<?>> hooks;

            if (serializedHooks == null) {
                hooks = Collections.emptyList();
            } else {
                final MasterTriggerRestoreHook.Factory[] hookFactories;
                try {
                    hookFactories = serializedHooks.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not instantiate user-defined checkpoint hooks", e);
                }

                final Thread thread = Thread.currentThread();
                final ClassLoader originalClassLoader = thread.getContextClassLoader();
                thread.setContextClassLoader(classLoader);

                try {
                    hooks = new ArrayList<>(hookFactories.length);
                    for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
                        hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
                    }
                } finally {
                    thread.setContextClassLoader(originalClassLoader);
                }
            }

            final CheckpointCoordinatorConfiguration chkConfig =
                    snapshotSettings.getCheckpointCoordinatorConfiguration();

            executionGraph.enableCheckpointing(
                    chkConfig,
                    hooks,
                    checkpointIdCounter,
                    completedCheckpointStore,
                    rootBackend,
                    rootStorage,
                    checkpointStatsTrackerFactory.get(),
                    checkpointsCleaner,
                    jobManagerConfig.get(STATE_CHANGE_LOG_STORAGE));
        }

        return executionGraph;
    }

    public static boolean isCheckpointingEnabled(JobGraph jobGraph) {
        return jobGraph.getCheckpointingSettings() != null;
    }

    // ------------------------------------------------------------------------

    /** This class is not supposed to be instantiated. */
    private DefaultExecutionGraphBuilder() {}
}
