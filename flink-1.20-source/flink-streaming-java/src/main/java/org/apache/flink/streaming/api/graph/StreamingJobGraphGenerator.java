/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphUtils;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroup;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupImpl;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}. */
@Internal
public class StreamingJobGraphGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public static JobGraph createJobGraph(StreamGraph streamGraph) {
        return new StreamingJobGraphGenerator(
                        Thread.currentThread().getContextClassLoader(),
                        streamGraph,
                        null,
                        Runnable::run)
                .createJobGraph();
    }

    public static JobGraph createJobGraph(
            ClassLoader userClassLoader, StreamGraph streamGraph, @Nullable JobID jobID) {
        // TODO Currently, we construct a new thread pool for the compilation of each job. In the
        // future, we may refactor the job submission framework and make it reusable across jobs.

        // serializationExecutor：
        final ExecutorService serializationExecutor =
                Executors.newFixedThreadPool(
                        Math.max(
                                1,
                                Math.min(
                                        Hardware.getNumberCPUCores(),
                                        streamGraph.getExecutionConfig().getParallelism())),
                        new ExecutorThreadFactory("flink-operator-serialization-io"));
        try {
            return new StreamingJobGraphGenerator(
                            userClassLoader, streamGraph, jobID, serializationExecutor)
                    .createJobGraph();
        } finally {
            serializationExecutor.shutdown();
        }
    }

    // ------------------------------------------------------------------------

    private final ClassLoader userClassloader;
    private final StreamGraph streamGraph;

    private final Map<Integer, JobVertex> jobVertices;
    private final JobGraph jobGraph;
    private final Collection<Integer> builtVertices;

    private final List<StreamEdge> physicalEdgesInOrder;

    private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

    private final Map<Integer, StreamConfig> vertexConfigs;
    private final Map<Integer, String> chainedNames;

    private final Map<Integer, ResourceSpec> chainedMinResources;
    private final Map<Integer, ResourceSpec> chainedPreferredResources;

    private final Map<Integer, InputOutputFormatContainer> chainedInputOutputFormats;

    // the ids of nodes whose output result partition type should be set to BLOCKING
    private final Set<Integer> outputBlockingNodesID;

    private final StreamGraphHasher defaultStreamGraphHasher;
    private final List<StreamGraphHasher> legacyStreamGraphHashers;

    private boolean hasHybridResultPartition = false;

    private final Executor serializationExecutor;

    // Futures for the serialization of operator coordinators
    private final Map<
                    JobVertexID,
                    List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
            coordinatorSerializationFuturesPerJobVertex = new HashMap<>();

    /** The {@link OperatorChainInfo}s, key is the start node id of the chain. */
    private final Map<Integer, OperatorChainInfo> chainInfos;

    /**
     * This is used to cache the non-chainable outputs, to set the non-chainable outputs config
     * after all job vertices are created.
     */
    private final Map<Integer, List<StreamEdge>> opNonChainableOutputsCache;

    private StreamingJobGraphGenerator(
            ClassLoader userClassloader,
            StreamGraph streamGraph,
            @Nullable JobID jobID,
            Executor serializationExecutor) {
        this.userClassloader = userClassloader;
        this.streamGraph = streamGraph;
        this.defaultStreamGraphHasher = new StreamGraphHasherV2();
        this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

        this.jobVertices = new LinkedHashMap<>();
        this.builtVertices = new HashSet<>();
        this.chainedConfigs = new HashMap<>();
        this.vertexConfigs = new HashMap<>();
        this.chainedNames = new HashMap<>();
        this.chainedMinResources = new HashMap<>();
        this.chainedPreferredResources = new HashMap<>();
        this.chainedInputOutputFormats = new HashMap<>();
        this.outputBlockingNodesID = new HashSet<>();
        this.physicalEdgesInOrder = new ArrayList<>();
        this.serializationExecutor = Preconditions.checkNotNull(serializationExecutor);
        this.chainInfos = new HashMap<>();
        this.opNonChainableOutputsCache = new LinkedHashMap<>();

        jobGraph = new JobGraph(jobID, streamGraph.getJobName());
    }

    //生成jobGraph的核心方法 生成了Operator Chain 并行度相同的map算子进行合并
    private JobGraph createJobGraph() {
        // ① 提交前的合法性校验（只在开启 checkpointing 时才做实际检查）：
        //      - 迭代作业默认不支持 checkpoint（无法保证 exactly-once）；
        //      - 迭代作业 + 非对齐 checkpoint 不支持；
        //      - 自定义 partitioner + 非对齐 checkpoint 不支持（rescale 无法保证正确）；
        //      - 实现 InputSelectable 的算子不支持 checkpoint；
        //      - 非对齐 checkpoint 只支持 EXACTLY_ONCE，否则**静默降级并告警**
        //        （注意这里会就地改写 checkpointConfig，不只是抛异常）。
        preValidate();

        // ② 作业级属性：JobType(STREAMING/BATCH) 和是否 dynamic（batch 自适应调度）。
        //    这两个值来自 StreamGraph，而 StreamGraph 的值在 StreamGraphGenerator
        //    的 configureStreamGraph() 里已按 runtime-mode 定好。
        jobGraph.setJobType(streamGraph.getJobType());
        jobGraph.setDynamic(streamGraph.isDynamic());

        // 近似本地恢复（approximate local recovery）：失败时优先在原 TM 上恢复，
        // 省掉从远端拉状态的开销。
        jobGraph.enableApproximateLocalRecovery(
                streamGraph.getCheckpointConfig().isApproximateLocalRecoveryEnabled());

        // ③ ★ 生成"确定性哈希"，这是算子链和 savepoint 的共同前置条件。
        //    defaultStreamGraphHasher 会遍历整个 StreamGraph，为每个 StreamNode 算一个 hash。
        //    两个用途：
        //      (a) 作为 JobVertexID 的来源（见 createJobVertex()）；
        //      (b) 建立 savepoint 里状态 与 算子 的对应关系。
        //    "确定性"是关键：只要算子没变，重新提交时算出的 hash 就一样，
        //    从而能从 savepoint 恢复。反过来，改了算子结构（或调了影响 hash 的配置）
        //    → hash 变 → 恢复时报"找不到对应状态"，除非显式允许 skip。
        // Generate deterministic hashes for the nodes in order to identify them across
        // submission iff they didn't change.
        Map<Integer, byte[]> hashes =
                defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

        // 同时生成历史版本的 hash，用于兼容老版本 savepoint
        // （新版 hasher 一旦上线就不能再改算法，只能新增，否则老 savepoint 全部失效）。
        // Generate legacy version hashes for backwards compatibility
        List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
        for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
            legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
        }

        // ④ ★★ 本方法的核心：算子链（operator chaining）在这里产生。
        //    setChaining() 会消费 ③ 生成的 hash，把 StreamGraph 的 StreamNode/StreamEdge
        //    合并成 JobGraph 的 JobVertex/JobEdge —— 这是"逻辑图 → 物理图"的关键一步，
        //    也是 JobVertex 数量少于 StreamNode 数量的原因。
        //
        //    详细算法与判定规则见下面 setChaining() / createChain() 的注释。
        setChaining(hashes, legacyHashes);

        // ⑤ dynamic graph（batch 自适应调度/自动并行度）的并行度回填。
        //    必须在 ④ 之后：它需要先知道链的划分，才能把同一个 forward group 内
        //    的 JobVertex 并行度对齐。
        if (jobGraph.isDynamic()) {
            setVertexParallelismsForDynamicGraphIfNecessary();
        }

        // ⑥ 设置所有"非链式输出"（需要过网络的那些输出）的配置。
        //    源码注释点明了顺序约束：必须在 ⑤ 之后，
        //    因为 setVertexParallelismsForDynamicGraphIfNecessary() 会影响
        //    JobVertex 的并行度，进而影响 partition reuse 的判定。
        // Note that we set all the non-chainable outputs configuration here because the
        // "setVertexParallelismsForDynamicGraphIfNecessary" may affect the parallelism of job
        // vertices and partition-reuse
        final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs =
                new HashMap<>();
        setAllOperatorNonChainedOutputsConfigs(opIntermediateOutputs);
        setAllVertexNonChainedOutputsConfigs(opIntermediateOutputs);

        // ⑦ 建立 JobGraph 层面的边：把断链处的 StreamEdge 转成
        //    IntermediateDataSet（上游产物）+ JobEdge（下游消费），即真正的 shuffle 边界。
        setPhysicalEdges();

        // ⑧ 标记哪些 JobVertex 支持"并发执行尝试"（speculative execution 相关）。
        markSupportingConcurrentExecutionAttempts();

        // ⑨ hybrid shuffle 只支持 batch，这里做断言校验。
        validateHybridShuffleExecuteInBatchMode();

        // ⑩ 落到 JobVertex 上的 SlotSharingGroup 与 CoLocationGroup。
        //    这就是前面聊过的：StreamGraph 层只存 SSG 名字，
        //    到这一步才合成运行时的 SlotSharingGroup 对象并挂到 vertex 上。
        setSlotSharingAndCoLocation();

        // ⑪ 计算每个算子/槽位的托管内存占比。
        setManagedMemoryFraction(
                Collections.unmodifiableMap(jobVertices),
                Collections.unmodifiableMap(vertexConfigs),
                Collections.unmodifiableMap(chainedConfigs),
                id -> streamGraph.getStreamNode(id).getManagedMemoryOperatorScopeUseCaseWeights(),
                id -> streamGraph.getStreamNode(id).getManagedMemorySlotScopeUseCases());

        // ⑫ checkpoint 相关设置（间隔、模式、超时、状态后端、重启策略等）。
        configureCheckpointing();

        // ⑬ savepoint 恢复设置 —— 就是 CLI 的 -s / -n / -cm 最终落到 JobGraph 的地方。
        jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());

        // ⑭ 用户 artifact（分布式缓存）：把注册的 cached file 转成 JobGraph 的 userArtifacts。
        //    注意前面讲过：application 模式下这些文件必须 JobManager 也能访问到。
        final Map<String, DistributedCache.DistributedCacheEntry> distributedCacheEntries =
                JobGraphUtils.prepareUserArtifactEntries(
                        streamGraph.getUserArtifacts().stream()
                                .collect(Collectors.toMap(e -> e.f0, e -> e.f1)),
                        jobGraph.getJobID());

        for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                distributedCacheEntries.entrySet()) {
            jobGraph.addUserArtifact(entry.getKey(), entry.getValue());
        }

        // ⑮ ExecutionConfig 必须**最后**设置，因为前面的步骤（尤其是 chaining 和
        //    资源/并行度计算）会往 ExecutionConfig 里回写内容，只有到这里它才"定型"。
        //    失败时抛 IllegalConfigurationException 并给出提示：
        //    通常是注册了不可序列化的自定义序列化器或类型。
        // set the ExecutionConfig last when it has been finalized
        try {
            jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
        } catch (IOException e) {
            throw new IllegalConfigurationException(
                    "Could not serialize the ExecutionConfig."
                            + "This indicates that non-serializable types (like custom serializers) were registered");
        }

        // ⑯ 作业级配置。就是 env.configure() / -D 那批配置的落点，
        //    最终成为 JobGraph 的 jobConfiguration，在集群侧作为作业级覆盖生效。
        jobGraph.setJobConfiguration(streamGraph.getJobConfiguration());

        // ⑰ 顶点命名：可选的索引前缀 + 描述信息（调试/Web UI 可读性）。
        addVertexIndexPrefixInVertexName();

        setVertexDescription();

        // ⑱ ★ 等待异步序列化完成。
        //    这里体现了 Flink 的一个性能优化：StreamConfig（算子配置）和
        //    OperatorCoordinator.Provider 的序列化是**提前异步提交**到
        //    serializationExecutor 的（见 createJobVertex 里的 triggerSerializationAndReturnFuture），
        //    在这里统一 get() 等待，从而把序列化和后面的构建步骤重叠起来。
        //    coordinator 是 FLIP-27 Source 的协调器（如 Kafka 的 split 分配），
        //    必须等 JobVertex 建好之后再 add 进去。
        // Wait for the serialization of operator coordinators and stream config.
        try {
            FutureUtils.combineAll(
                            vertexConfigs.values().stream()
                                    .map(
                                            config ->
                                                    config.triggerSerializationAndReturnFuture(
                                                            serializationExecutor))
                                    .collect(Collectors.toList()))
                    .get();

            waitForSerializationFuturesAndUpdateJobVertices();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error in serialization.", e);
        }

        // ⑲ 作业状态钩子（JobStatusHook），用于在作业状态变化时回调。
        if (!streamGraph.getJobStatusHooks().isEmpty()) {
            jobGraph.setJobStatusHooks(streamGraph.getJobStatusHooks());
        }

        // ⑳ 返回最终的 JobGraph —— 客户端侧的工作到此结束，
        //    接下来它会被序列化并通过 REST（或 application 模式下的进程内 RPC）送到 JobManager。
        return jobGraph;
    }

    private void waitForSerializationFuturesAndUpdateJobVertices()
            throws ExecutionException, InterruptedException {
        for (Map.Entry<
                        JobVertexID,
                        List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
                futuresPerJobVertex : coordinatorSerializationFuturesPerJobVertex.entrySet()) {
            final JobVertexID jobVertexId = futuresPerJobVertex.getKey();
            final JobVertex jobVertex = jobGraph.findVertexByID(jobVertexId);

            Preconditions.checkState(
                    jobVertex != null,
                    "OperatorCoordinator providers were registered for JobVertexID '%s' but no corresponding JobVertex can be found.",
                    jobVertexId);
            FutureUtils.combineAll(futuresPerJobVertex.getValue())
                    .get()
                    .forEach(jobVertex::addOperatorCoordinator);
        }
    }

    private void addVertexIndexPrefixInVertexName() {
        if (!streamGraph.isVertexNameIncludeIndexPrefix()) {
            return;
        }
        final AtomicInteger vertexIndexId = new AtomicInteger(0);
        jobGraph.getVerticesSortedTopologicallyFromSources()
                .forEach(
                        vertex ->
                                vertex.setName(
                                        String.format(
                                                "[vertex-%d]%s",
                                                vertexIndexId.getAndIncrement(),
                                                vertex.getName())));
    }

    private void setVertexDescription() {
        for (Map.Entry<Integer, JobVertex> headOpAndJobVertex : jobVertices.entrySet()) {
            Integer headOpId = headOpAndJobVertex.getKey();
            JobVertex vertex = headOpAndJobVertex.getValue();
            StringBuilder builder = new StringBuilder();
            switch (streamGraph.getVertexDescriptionMode()) {
                case CASCADING:
                    buildCascadingDescription(builder, headOpId, headOpId);
                    break;
                case TREE:
                    buildTreeDescription(builder, headOpId, headOpId, "", true);
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Description mode %s not supported",
                                    streamGraph.getVertexDescriptionMode()));
            }
            vertex.setOperatorPrettyName(builder.toString());
        }
    }

    private void buildCascadingDescription(StringBuilder builder, int headOpId, int currentOpId) {
        StreamNode node = streamGraph.getStreamNode(currentOpId);
        builder.append(getDescriptionWithChainedSourcesInfo(node));

        LinkedList<Integer> chainedOutput = getChainedOutputNodes(headOpId, node);
        if (chainedOutput.isEmpty()) {
            return;
        }
        builder.append(" -> ");

        boolean multiOutput = chainedOutput.size() > 1;
        if (multiOutput) {
            builder.append("(");
        }
        while (true) {
            Integer outputId = chainedOutput.pollFirst();
            buildCascadingDescription(builder, headOpId, outputId);
            if (chainedOutput.isEmpty()) {
                break;
            }
            builder.append(" , ");
        }
        if (multiOutput) {
            builder.append(")");
        }
    }

    private LinkedList<Integer> getChainedOutputNodes(int headOpId, StreamNode node) {
        LinkedList<Integer> chainedOutput = new LinkedList<>();
        if (chainedConfigs.containsKey(headOpId)) {
            for (StreamEdge edge : node.getOutEdges()) {
                int targetId = edge.getTargetId();
                if (chainedConfigs.get(headOpId).containsKey(targetId)) {
                    chainedOutput.add(targetId);
                }
            }
        }
        return chainedOutput;
    }

    private void buildTreeDescription(
            StringBuilder builder, int headOpId, int currentOpId, String prefix, boolean isLast) {
        // Replace the '-' in prefix of current node with ' ', keep ':'
        // HeadNode
        // :- Node1
        // :  :- Child1
        // :  +- Child2
        // +- Node2
        //    :- Child3
        //    +- Child4
        String currentNodePrefix = "";
        String childPrefix = "";
        if (currentOpId != headOpId) {
            if (isLast) {
                currentNodePrefix = prefix + "+- ";
                childPrefix = prefix + "   ";
            } else {
                currentNodePrefix = prefix + ":- ";
                childPrefix = prefix + ":  ";
            }
        }

        StreamNode node = streamGraph.getStreamNode(currentOpId);
        builder.append(currentNodePrefix);
        builder.append(getDescriptionWithChainedSourcesInfo(node));
        builder.append("\n");

        LinkedList<Integer> chainedOutput = getChainedOutputNodes(headOpId, node);
        while (!chainedOutput.isEmpty()) {
            Integer outputId = chainedOutput.pollFirst();
            buildTreeDescription(builder, headOpId, outputId, childPrefix, chainedOutput.isEmpty());
        }
    }

    private String getDescriptionWithChainedSourcesInfo(StreamNode node) {

        List<StreamNode> chainedSources;
        if (!chainedConfigs.containsKey(node.getId())) {
            // node is not head operator of a vertex
            chainedSources = Collections.emptyList();
        } else {
            chainedSources =
                    node.getInEdges().stream()
                            .map(StreamEdge::getSourceId)
                            .filter(
                                    id ->
                                            streamGraph.getSourceIDs().contains(id)
                                                    && chainedConfigs
                                                            .get(node.getId())
                                                            .containsKey(id))
                            .map(streamGraph::getStreamNode)
                            .collect(Collectors.toList());
        }
        return chainedSources.isEmpty()
                ? node.getOperatorDescription()
                : String.format(
                        "%s [%s]",
                        node.getOperatorDescription(),
                        chainedSources.stream()
                                .map(StreamNode::getOperatorDescription)
                                .collect(Collectors.joining(", ")));
    }

    @SuppressWarnings("deprecation")
    private void preValidate() {
        CheckpointConfig checkpointConfig = streamGraph.getCheckpointConfig();

        if (checkpointConfig.isCheckpointingEnabled()) {
            // temporarily forbid checkpointing for iterative jobs
            if (streamGraph.isIterative() && !checkpointConfig.isForceCheckpointing()) {
                throw new UnsupportedOperationException(
                        "Checkpointing is currently not supported by default for iterative jobs, as we cannot guarantee exactly once semantics. "
                                + "State checkpoints happen normally, but records in-transit during the snapshot will be lost upon failure. "
                                + "\nThe user can force enable state checkpoints with the reduced guarantees by calling: env.enableCheckpointing(interval,true)");
            }
            if (streamGraph.isIterative()
                    && checkpointConfig.isUnalignedCheckpointsEnabled()
                    && !checkpointConfig.isForceUnalignedCheckpoints()) {
                throw new UnsupportedOperationException(
                        "Unaligned Checkpoints are currently not supported for iterative jobs, "
                                + "as rescaling would require alignment (in addition to the reduced checkpointing guarantees)."
                                + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }
            if (checkpointConfig.isUnalignedCheckpointsEnabled()
                    && !checkpointConfig.isForceUnalignedCheckpoints()
                    && streamGraph.getStreamNodes().stream().anyMatch(this::hasCustomPartitioner)) {
                throw new UnsupportedOperationException(
                        "Unaligned checkpoints are currently not supported for custom partitioners, "
                                + "as rescaling is not guaranteed to work correctly."
                                + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }

            for (StreamNode node : streamGraph.getStreamNodes()) {
                StreamOperatorFactory operatorFactory = node.getOperatorFactory();
                if (operatorFactory != null) {
                    Class<?> operatorClass =
                            operatorFactory.getStreamOperatorClass(userClassloader);
                    if (InputSelectable.class.isAssignableFrom(operatorClass)) {

                        throw new UnsupportedOperationException(
                                "Checkpointing is currently not supported for operators that implement InputSelectable:"
                                        + operatorClass.getName());
                    }
                }
            }
        }

        if (checkpointConfig.isUnalignedCheckpointsEnabled()
                && getCheckpointingMode(checkpointConfig) != CheckpointingMode.EXACTLY_ONCE) {
            LOG.warn("Unaligned checkpoints can only be used with checkpointing mode EXACTLY_ONCE");
            checkpointConfig.enableUnalignedCheckpoints(false);
        }
    }

    private boolean hasCustomPartitioner(StreamNode node) {
        return node.getOutEdges().stream()
                .anyMatch(edge -> edge.getPartitioner() instanceof CustomPartitionerWrapper);
    }

    private void setPhysicalEdges() {
        Map<Integer, List<StreamEdge>> physicalInEdgesInOrder =
                new HashMap<Integer, List<StreamEdge>>();

        for (StreamEdge edge : physicalEdgesInOrder) {
            int target = edge.getTargetId();

            List<StreamEdge> inEdges =
                    physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());

            inEdges.add(edge);
        }

        for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
            int vertex = inEdges.getKey();
            List<StreamEdge> edgeList = inEdges.getValue();

            vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
        }
    }

    private Map<Integer, OperatorChainInfo> buildChainedInputsAndGetHeadInputs(
            final Map<Integer, byte[]> hashes, final List<Map<Integer, byte[]>> legacyHashes) {

        final Map<Integer, ChainedSourceInfo> chainedSources = new HashMap<>();
        final Map<Integer, OperatorChainInfo> chainEntryPoints = new HashMap<>();

        for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
            final StreamNode sourceNode = streamGraph.getStreamNode(sourceNodeId);

            if (sourceNode.getOperatorFactory() != null
                    && sourceNode.getOperatorFactory() instanceof SourceOperatorFactory
                    && sourceNode.getOutEdges().size() == 1) {
                // as long as only NAry ops support this chaining, we need to skip the other parts
                final StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);
                final StreamNode target = streamGraph.getStreamNode(sourceOutEdge.getTargetId());
                final ChainingStrategy targetChainingStrategy =
                        Preconditions.checkNotNull(target.getOperatorFactory())
                                .getChainingStrategy();

                if (targetChainingStrategy == ChainingStrategy.HEAD_WITH_SOURCES
                        && isChainableInput(sourceOutEdge, streamGraph)) {
                    final OperatorID opId = new OperatorID(hashes.get(sourceNodeId));
                    final StreamConfig.SourceInputConfig inputConfig =
                            new StreamConfig.SourceInputConfig(sourceOutEdge);
                    final StreamConfig operatorConfig = new StreamConfig(new Configuration());
                    setOperatorConfig(sourceNodeId, operatorConfig, Collections.emptyMap());
                    setOperatorChainedOutputsConfig(operatorConfig, Collections.emptyList());
                    // we cache the non-chainable outputs here, and set the non-chained config later
                    opNonChainableOutputsCache.put(sourceNodeId, Collections.emptyList());

                    operatorConfig.setChainIndex(0); // sources are always first
                    operatorConfig.setOperatorID(opId);
                    operatorConfig.setOperatorName(sourceNode.getOperatorName());
                    chainedSources.put(
                            sourceNodeId, new ChainedSourceInfo(operatorConfig, inputConfig));

                    final SourceOperatorFactory<?> sourceOpFact =
                            (SourceOperatorFactory<?>) sourceNode.getOperatorFactory();
                    final OperatorCoordinator.Provider coord =
                            sourceOpFact.getCoordinatorProvider(sourceNode.getOperatorName(), opId);

                    final OperatorChainInfo chainInfo =
                            chainEntryPoints.computeIfAbsent(
                                    sourceOutEdge.getTargetId(),
                                    (k) ->
                                            new OperatorChainInfo(
                                                    sourceOutEdge.getTargetId(),
                                                    hashes,
                                                    legacyHashes,
                                                    chainedSources,
                                                    streamGraph));
                    chainInfo.addCoordinatorProvider(coord);
                    chainInfo.recordChainedNode(sourceNodeId);
                    continue;
                }
            }

            chainEntryPoints.put(
                    sourceNodeId,
                    new OperatorChainInfo(
                            sourceNodeId, hashes, legacyHashes, chainedSources, streamGraph));
        }

        return chainEntryPoints;
    }

    /**
     * Sets up task chains from the source {@link StreamNode} instances.
     *
     * <p>This will recursively create all {@link JobVertex} instances.
     */
    private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes) {
        // ★ 第一步：区分两类 source。
        //   同一个 source 有两种命运：
        //     a) 作为「链头」(head operator) —— 自己开一条链，是 JobVertex 的起点；
        //     b) 作为「chained input」被塞进下游算子的链内 ——
        //        此时它在链里的位置是 0（chainIndex=0），
        //        所以下面 createChain 的 chainIndex 从 1 开始，把 0 留给它。
        //   只有下游的 ChainingStrategy == HEAD_WITH_SOURCES 时才会走 b)（目前仅 N-ary 算子支持），
        //   具体逻辑见 buildChainedInputsAndGetHeadInputs()。
        //   返回 Map<链头节点id, OperatorChainInfo>，OperatorChainInfo 是一条链的"账本"
        //   （记录链内节点、各自的 hash、资源规格、算子 ID 等）。
        // we separate out the sources that run as inputs to another operator (chained inputs)
        // from the sources that needs to run as the main (head) operator.
        final Map<Integer, OperatorChainInfo> chainEntryPoints =
                buildChainedInputsAndGetHeadInputs(hashes, legacyHashes);

        // ★ 第二步：按节点 id 排序后再遍历，保证生成顺序**确定**。
        //   如果不排序，HashMap 的遍历顺序不稳定，同一份程序每次生成的
        //   JobVertex 添加顺序可能不同，影响可复现性（也难以对比两次构建的差异）。
        final Collection<OperatorChainInfo> initialEntryPoints =
                chainEntryPoints.entrySet().stream()
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

        // ★ 第三步：对每个链头做一次 DFS。
        //   注意这里遍历的是 initialEntryPoints 这个**副本**，而不是 chainEntryPoints 本身：
        //   因为 createChain 在遇到断链时会往 chainEntryPoints 里塞新链
        //   （见 createChain 里的 chainEntryPoints.computeIfAbsent(...)），
        //   直接遍历原 map 会触发 ConcurrentModificationException。
        // iterate over a copy of the values, because this map gets concurrently modified
        for (OperatorChainInfo info : initialEntryPoints) {
            createChain(
                    info.getStartNodeId(),
                    1, // operators start at position 1 because 0 is for chained source inputs
                    info,
                    chainEntryPoints);
        }
    }

    private List<StreamEdge> createChain(
            final Integer currentNodeId,
            final int chainIndex,
            final OperatorChainInfo chainInfo,
            final Map<Integer, OperatorChainInfo> chainEntryPoints) {

        Integer startNodeId = chainInfo.getStartNodeId();

        // ★ 链头已经建过 JobVertex 就直接返回（返回空表示"没有新的传递性出边要上报"）。
        //   这是 DFS 的终止条件之一：同一条链的链头只会被创建一次。
        if (!builtVertices.contains(startNodeId)) {

            // ★★ 传递性出边（transitiveOutEdges）：本次 DFS 收集到的所有"断链处"的边。
            //    它最终会被设到链头所属的 OperatorChainInfo 上
            //    （见下面 chainInfo.setTransitiveOutEdges(...)），
            //    再由 setPhysicalEdges() 转成 JobGraph 里 JobVertex 之间的 JobEdge。
            //    为什么叫"传递性"：链内中间断链的边要一路冒泡到链头，
            //    因为 JobEdge 的起点必须是最外层 JobVertex，而不是链内某个算子。
            List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

            // ★★ 核心：把当前节点的出边分成两组。
            //   chainable    —— 可以并入**同一条链**（同一个 JobVertex、同一个线程）
            //   nonChainable —— 必须断链，开启**新的链**（新的 JobVertex），数据过网络
            //   判定规则全在 isChainable(edge, streamGraph) 里。
            List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
            List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

            StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

            // "只允许在流结束后才输出"的算子（需要先缓存全部数据、最后统一输出的算子）：
            // 它的输出必须走 BLOCKING 边且禁用 buffer timeout。
            // 这里把整条链的节点都登记进 outputBlockingNodesID（下面还有两处 add），
            // 该集合会在 determineUndefinedResultPartitionType() 里被消费，
            // 决定边的 ResultPartitionType。
            boolean isOutputOnlyAfterEndOfStream = currentNode.isOutputOnlyAfterEndOfStream();
            if (isOutputOnlyAfterEndOfStream) {
                outputBlockingNodesID.add(currentNode.getId());
            }

            for (StreamEdge outEdge : currentNode.getOutEdges()) {
                if (isChainable(outEdge, streamGraph)) {
                    chainableOutputs.add(outEdge);
                } else {
                    nonChainableOutputs.add(outEdge);
                }
            }

            // ★ 分支一：可链出边 —— 递归下去，但**沿用同一个 chainInfo**
            //   （仍属同一条链、最终同一个 JobVertex），且 chainIndex + 1
            //   （在链里的位置往后挪一位）。
            //   递归返回的出边会被合并进本层的 transitiveOutEdges，
            //   这样断链边就能一路冒泡到链头。
            for (StreamEdge chainable : chainableOutputs) {
                // Mark downstream nodes in the same chain as outputBlocking
                if (isOutputOnlyAfterEndOfStream) {
                    outputBlockingNodesID.add(chainable.getTargetId());
                }
                transitiveOutEdges.addAll(
                        createChain(
                                chainable.getTargetId(),
                                chainIndex + 1,
                                chainInfo,
                                chainEntryPoints));
                // Mark upstream nodes in the same chain as outputBlocking
                if (outputBlockingNodesID.contains(chainable.getTargetId())) {
                    outputBlockingNodesID.add(currentNodeId);
                }
            }

            // ★ 分支二：不可链出边 —— 递归下去，但**开一条新链**：
            //   chainInfo.newChain(targetId) 新建一个 OperatorChainInfo，
            //   chainIndex 重置回 1（新链 = 链头 + 后续算子）。
            //   同时这条边本身被加入 transitiveOutEdges —— 它就是 JobVertex 之间的边界，
            //   将来变成 IntermediateDataSet + JobEdge。
            //   computeIfAbsent 的用意：若该下游节点已作为别的链的链头登记过，
            //   直接复用那份 chainInfo，避免同一节点被建成两个 JobVertex。
            for (StreamEdge nonChainable : nonChainableOutputs) {
                transitiveOutEdges.add(nonChainable);
                createChain(
                        nonChainable.getTargetId(),
                        1, // operators start at position 1 because 0 is for chained source inputs
                        chainEntryPoints.computeIfAbsent(
                                nonChainable.getTargetId(),
                                (k) -> chainInfo.newChain(nonChainable.getTargetId())),
                        chainEntryPoints);
            }

            // ★ 链的"聚合属性"：整条链对外只暴露一个名字和一套资源规格，
            //   所以要沿着链把所有节点的名字/资源合并起来
            //   （例如 "Source -> Map -> Sink"）。
            chainedNames.put(
                    currentNodeId,
                    createChainedName(
                            currentNodeId,
                            chainableOutputs,
                            Optional.ofNullable(chainEntryPoints.get(currentNodeId))));
            chainedMinResources.put(
                    currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
            chainedPreferredResources.put(
                    currentNodeId,
                    createChainedPreferredResources(currentNodeId, chainableOutputs));

            // ★ 把本节点登记进链，并拿到它的 OperatorID。
            //   注意 OperatorID 是**每个算子一个**（不是每个 JobVertex 一个），
            //   默认由算子 hash 生成，用户可以用 uid(...) 显式指定。
            //   savepoint 里的状态是按 OperatorID 索引的 —— 这就是"改了算子就要重新指定 uid"
            //   这条运维规则的根源。
            OperatorID currentOperatorId =
                    chainInfo.addNodeToChain(
                            currentNodeId,
                            streamGraph.getStreamNode(currentNodeId).getOperatorName());

            if (currentNode.getInputFormat() != null) {
                getOrCreateFormatContainer(startNodeId)
                        .addInputFormat(currentOperatorId, currentNode.getInputFormat());
            }

            if (currentNode.getOutputFormat() != null) {
                getOrCreateFormatContainer(startNodeId)
                        .addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
            }

            // ★ 链头 vs 链内节点，构造 StreamConfig 的方式不同：
            //     链头 → createJobVertex() 真正创建 JobVertex
            //            （JobVertexID 来自 createJobGraph ③ 生成的确定性 hash）
            //     链内 → 只 new 一个空 StreamConfig，稍后塞进 chainedConfigs
            //   这是理解"一个 JobVertex 内部怎么装下多个算子"的关键：
            //   链头的 StreamConfig 通过 setTransitiveChainedTaskConfigs(...)
            //   携带了整条链上所有算子的配置。
            StreamConfig config =
                    currentNodeId.equals(startNodeId)
                            ? createJobVertex(startNodeId, chainInfo)
                            : new StreamConfig(new Configuration());

            tryConvertPartitionerForDynamicGraph(chainableOutputs, nonChainableOutputs);

            setOperatorConfig(currentNodeId, config, chainInfo.getChainedSources());

            // 可链出边：写进本算子的 chain 输出配置（运行时表现为链内直接方法调用，不走网络）
            setOperatorChainedOutputsConfig(config, chainableOutputs);

            // 不可链出边：**先缓存**，等并行度全部确定后再统一设置
            // （见 createJobGraph ⑥ 的 setAllOperatorNonChainedOutputsConfigs）。
            // 之所以延后，是因为 dynamic graph 会回填并行度，进而影响 partition reuse 的判定。
            // we cache the non-chainable outputs here, and set the non-chained config later
            opNonChainableOutputsCache.put(currentNodeId, nonChainableOutputs);

            if (currentNodeId.equals(startNodeId)) {
                // ★ 链头分支：把收集好的传递性出边挂到链上，登记 chainInfos，
                //   并把整条链的配置一并塞给链头的 StreamConfig。
                chainInfo.setTransitiveOutEdges(transitiveOutEdges);
                chainInfos.put(startNodeId, chainInfo);

                config.setChainStart();
                config.setChainIndex(chainIndex);
                config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
                config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

            } else {
                // ★ 链内节点分支：只把配置登记到"以链头为 key"的 map 里，
                //   它不会单独产生 JobVertex。
                chainedConfigs.computeIfAbsent(
                        startNodeId, k -> new HashMap<Integer, StreamConfig>());

                config.setChainIndex(chainIndex);
                StreamNode node = streamGraph.getStreamNode(currentNodeId);
                config.setOperatorName(node.getOperatorName());
                chainedConfigs.get(startNodeId).put(currentNodeId, config);
            }

            config.setOperatorID(currentOperatorId);

            // 没有可链的出边 ⇒ 本节点就是链尾（运行时 OperatorChain 的最后一个算子）。
            if (chainableOutputs.isEmpty()) {
                config.setChainEnd();
            }
            return transitiveOutEdges;

        } else {
            // 链头已建过，这条分支不需要再贡献出边
            return new ArrayList<>();
        }
    }

    /**
     * This method is used to reset or set job vertices' parallelism for dynamic graph:
     *
     * <p>1. Reset parallelism for job vertices whose parallelism is not configured.
     *
     * <p>2. Set parallelism and maxParallelism for job vertices in forward group, to ensure the
     * parallelism and maxParallelism of vertices in the same forward group to be the same; set the
     * parallelism at early stage if possible, to avoid invalid partition reuse.
     */
    private void setVertexParallelismsForDynamicGraphIfNecessary() {
        // Note that the jobVertices are reverse topological order
        final List<JobVertex> topologicalOrderVertices =
                IterableUtils.toStream(jobVertices.values()).collect(Collectors.toList());
        Collections.reverse(topologicalOrderVertices);

        // reset parallelism for job vertices whose parallelism is not configured
        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    final OperatorChainInfo chainInfo = chainInfos.get(startNodeId);
                    if (!jobVertex.isParallelismConfigured()
                            && streamGraph.isAutoParallelismEnabled()) {
                        jobVertex.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);
                        chainInfo
                                .getAllChainedNodes()
                                .forEach(
                                        n ->
                                                n.setParallelism(
                                                        ExecutionConfig.PARALLELISM_DEFAULT,
                                                        false));
                    }
                });

        final Map<JobVertex, Set<JobVertex>> forwardProducersByJobVertex = new HashMap<>();
        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    Set<JobVertex> forwardConsumers =
                            chainInfos.get(startNodeId).getTransitiveOutEdges().stream()
                                    .filter(
                                            edge ->
                                                    edge.getPartitioner()
                                                            instanceof ForwardPartitioner)
                                    .map(StreamEdge::getTargetId)
                                    .map(jobVertices::get)
                                    .collect(Collectors.toSet());

                    for (JobVertex forwardConsumer : forwardConsumers) {
                        forwardProducersByJobVertex.compute(
                                forwardConsumer,
                                (ignored, producers) -> {
                                    if (producers == null) {
                                        producers = new HashSet<>();
                                    }
                                    producers.add(jobVertex);
                                    return producers;
                                });
                    }
                });

        // compute forward groups
        final Map<JobVertexID, ForwardGroup> forwardGroupsByJobVertexId =
                ForwardGroupComputeUtil.computeForwardGroups(
                        topologicalOrderVertices,
                        jobVertex ->
                                forwardProducersByJobVertex.getOrDefault(
                                        jobVertex, Collections.emptySet()));

        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    ForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertex.getID());
                    // set parallelism for vertices in forward group
                    if (forwardGroup != null && forwardGroup.isParallelismDecided()) {
                        jobVertex.setParallelism(forwardGroup.getParallelism());
                        jobVertex.setParallelismConfigured(true);
                        chainInfos
                                .get(startNodeId)
                                .getAllChainedNodes()
                                .forEach(
                                        streamNode ->
                                                streamNode.setParallelism(
                                                        forwardGroup.getParallelism(), true));
                    }

                    // set max parallelism for vertices in forward group
                    if (forwardGroup != null && forwardGroup.isMaxParallelismDecided()) {
                        jobVertex.setMaxParallelism(forwardGroup.getMaxParallelism());
                        chainInfos
                                .get(startNodeId)
                                .getAllChainedNodes()
                                .forEach(
                                        streamNode ->
                                                streamNode.setMaxParallelism(
                                                        forwardGroup.getMaxParallelism()));
                    }
                });
    }

    private void checkAndReplaceReusableHybridPartitionType(NonChainedOutput reusableOutput) {
        if (reusableOutput.getPartitionType() == ResultPartitionType.HYBRID_SELECTIVE) {
            // for can be reused hybrid output, it can be optimized to always use full
            // spilling strategy to significantly reduce shuffle data writing cost.
            reusableOutput.setPartitionType(ResultPartitionType.HYBRID_FULL);
            LOG.info(
                    "{} result partition has been replaced by {} result partition to support partition reuse,"
                            + " which will reduce shuffle data writing cost.",
                    reusableOutput.getPartitionType().name(),
                    ResultPartitionType.HYBRID_FULL.name());
        }
    }

    private InputOutputFormatContainer getOrCreateFormatContainer(Integer startNodeId) {
        return chainedInputOutputFormats.computeIfAbsent(
                startNodeId,
                k ->
                        new InputOutputFormatContainer(
                                Thread.currentThread().getContextClassLoader()));
    }

    private String createChainedName(
            Integer vertexID,
            List<StreamEdge> chainedOutputs,
            Optional<OperatorChainInfo> operatorChainInfo) {
        List<ChainedSourceInfo> chainedSourceInfos =
                operatorChainInfo
                        .map(chainInfo -> getChainedSourcesByVertexId(vertexID, chainInfo))
                        .orElse(Collections.emptyList());
        final String operatorName =
                nameWithChainedSourcesInfo(
                        streamGraph.getStreamNode(vertexID).getOperatorName(), chainedSourceInfos);
        if (chainedOutputs.size() > 1) {
            List<String> outputChainedNames = new ArrayList<>();
            for (StreamEdge chainable : chainedOutputs) {
                outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
            }
            return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
        } else if (chainedOutputs.size() == 1) {
            return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
        } else {
            return operatorName;
        }
    }

    private List<ChainedSourceInfo> getChainedSourcesByVertexId(
            Integer vertexId, OperatorChainInfo chainInfo) {
        return streamGraph.getStreamNode(vertexId).getInEdges().stream()
                .map(inEdge -> chainInfo.getChainedSources().get(inEdge.getSourceId()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private ResourceSpec createChainedMinResources(
            Integer vertexID, List<StreamEdge> chainedOutputs) {
        ResourceSpec minResources = streamGraph.getStreamNode(vertexID).getMinResources();
        for (StreamEdge chainable : chainedOutputs) {
            minResources = minResources.merge(chainedMinResources.get(chainable.getTargetId()));
        }
        return minResources;
    }

    private ResourceSpec createChainedPreferredResources(
            Integer vertexID, List<StreamEdge> chainedOutputs) {
        ResourceSpec preferredResources =
                streamGraph.getStreamNode(vertexID).getPreferredResources();
        for (StreamEdge chainable : chainedOutputs) {
            preferredResources =
                    preferredResources.merge(
                            chainedPreferredResources.get(chainable.getTargetId()));
        }
        return preferredResources;
    }

    private StreamConfig createJobVertex(Integer streamNodeId, OperatorChainInfo chainInfo) {

        JobVertex jobVertex;
        StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

        byte[] hash = chainInfo.getHash(streamNodeId);

        if (hash == null) {
            throw new IllegalStateException(
                    "Cannot find node hash. "
                            + "Did you generate them before calling this method?");
        }

        JobVertexID jobVertexId = new JobVertexID(hash);

        List<Tuple2<byte[], byte[]>> chainedOperators =
                chainInfo.getChainedOperatorHashes(streamNodeId);
        List<OperatorIDPair> operatorIDPairs = new ArrayList<>();
        if (chainedOperators != null) {
            for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
                OperatorID userDefinedOperatorID =
                        chainedOperator.f1 == null ? null : new OperatorID(chainedOperator.f1);
                operatorIDPairs.add(
                        OperatorIDPair.of(
                                new OperatorID(chainedOperator.f0), userDefinedOperatorID));
            }
        }

        if (chainedInputOutputFormats.containsKey(streamNodeId)) {
            jobVertex =
                    new InputOutputFormatVertex(
                            chainedNames.get(streamNodeId), jobVertexId, operatorIDPairs);

            chainedInputOutputFormats
                    .get(streamNodeId)
                    .write(new TaskConfig(jobVertex.getConfiguration()));
        } else {
            jobVertex = new JobVertex(chainedNames.get(streamNodeId), jobVertexId, operatorIDPairs);
        }

        if (streamNode.getConsumeClusterDatasetId() != null) {
            jobVertex.addIntermediateDataSetIdToConsume(streamNode.getConsumeClusterDatasetId());
        }

        final List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>
                serializationFutures = new ArrayList<>();
        for (OperatorCoordinator.Provider coordinatorProvider :
                chainInfo.getCoordinatorProviders()) {
            serializationFutures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return new SerializedValue<>(coordinatorProvider);
                                } catch (IOException e) {
                                    throw new FlinkRuntimeException(
                                            String.format(
                                                    "Coordinator Provider for node %s is not serializable.",
                                                    chainedNames.get(streamNodeId)),
                                            e);
                                }
                            },
                            serializationExecutor));
        }
        if (!serializationFutures.isEmpty()) {
            coordinatorSerializationFuturesPerJobVertex.put(jobVertexId, serializationFutures);
        }

        jobVertex.setResources(
                chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

        jobVertex.setInvokableClass(streamNode.getJobVertexClass());

        int parallelism = streamNode.getParallelism();

        if (parallelism > 0) {
            jobVertex.setParallelism(parallelism);
        } else {
            parallelism = jobVertex.getParallelism();
        }

        jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
        }

        jobVertices.put(streamNodeId, jobVertex);
        builtVertices.add(streamNodeId);
        jobGraph.addVertex(jobVertex);

        jobVertex.setParallelismConfigured(
                chainInfo.getAllChainedNodes().stream()
                        .anyMatch(StreamNode::isParallelismConfigured));

        return new StreamConfig(jobVertex.getConfiguration());
    }

    private void setOperatorConfig(
            Integer vertexId, StreamConfig config, Map<Integer, ChainedSourceInfo> chainedSources) {

        StreamNode vertex = streamGraph.getStreamNode(vertexId);

        config.setVertexID(vertexId);

        // build the inputs as a combination of source and network inputs
        final List<StreamEdge> inEdges = vertex.getInEdges();
        final TypeSerializer<?>[] inputSerializers = vertex.getTypeSerializersIn();

        final StreamConfig.InputConfig[] inputConfigs =
                new StreamConfig.InputConfig[inputSerializers.length];

        int inputGateCount = 0;
        for (final StreamEdge inEdge : inEdges) {
            final ChainedSourceInfo chainedSource = chainedSources.get(inEdge.getSourceId());

            final int inputIndex =
                    inEdge.getTypeNumber() == 0
                            ? 0 // single input operator
                            : inEdge.getTypeNumber() - 1; // in case of 2 or more inputs

            if (chainedSource != null) {
                // chained source is the input
                if (inputConfigs[inputIndex] != null) {
                    throw new IllegalStateException(
                            "Trying to union a chained source with another input.");
                }
                inputConfigs[inputIndex] = chainedSource.getInputConfig();
                chainedConfigs
                        .computeIfAbsent(vertexId, (key) -> new HashMap<>())
                        .put(inEdge.getSourceId(), chainedSource.getOperatorConfig());
            } else {
                // network input. null if we move to a new input, non-null if this is a further edge
                // that is union-ed into the same input
                if (inputConfigs[inputIndex] == null) {
                    // PASS_THROUGH is a sensible default for streaming jobs. Only for BATCH
                    // execution can we have sorted inputs
                    StreamConfig.InputRequirement inputRequirement =
                            vertex.getInputRequirements()
                                    .getOrDefault(
                                            inputIndex, StreamConfig.InputRequirement.PASS_THROUGH);
                    inputConfigs[inputIndex] =
                            new StreamConfig.NetworkInputConfig(
                                    inputSerializers[inputIndex],
                                    inputGateCount++,
                                    inputRequirement);
                }
            }
        }

        // set the input config of the vertex if it consumes from cached intermediate dataset.
        if (vertex.getConsumeClusterDatasetId() != null) {
            config.setNumberOfNetworkInputs(1);
            inputConfigs[0] = new StreamConfig.NetworkInputConfig(inputSerializers[0], 0);
        }

        config.setInputs(inputConfigs);

        config.setTypeSerializerOut(vertex.getTypeSerializerOut());

        config.setStreamOperatorFactory(vertex.getOperatorFactory());

        config.setTimeCharacteristic(streamGraph.getTimeCharacteristic());

        final CheckpointConfig checkpointCfg = streamGraph.getCheckpointConfig();

        config.setStateBackend(streamGraph.getStateBackend());
        config.setCheckpointStorage(streamGraph.getCheckpointStorage());
        config.setGraphContainingLoops(streamGraph.isIterative());
        config.setTimerServiceProvider(streamGraph.getTimerServiceProvider());
        config.setCheckpointingEnabled(checkpointCfg.isCheckpointingEnabled());
        config.getConfiguration()
                .set(
                        CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH,
                        streamGraph.isEnableCheckpointsAfterTasksFinish());
        config.setCheckpointMode(getCheckpointingMode(checkpointCfg));
        config.setUnalignedCheckpointsEnabled(checkpointCfg.isUnalignedCheckpointsEnabled());
        config.setUnalignedCheckpointsSplittableTimersEnabled(
                checkpointCfg.isUnalignedCheckpointsInterruptibleTimersEnabled());
        config.setAlignedCheckpointTimeout(checkpointCfg.getAlignedCheckpointTimeout());
        config.setMaxSubtasksPerChannelStateFile(checkpointCfg.getMaxSubtasksPerChannelStateFile());
        config.setMaxConcurrentCheckpoints(checkpointCfg.getMaxConcurrentCheckpoints());

        for (int i = 0; i < vertex.getStatePartitioners().length; i++) {
            config.setStatePartitioner(i, vertex.getStatePartitioners()[i]);
        }
        config.setStateKeySerializer(vertex.getStateKeySerializer());

        Class<? extends TaskInvokable> vertexClass = vertex.getJobVertexClass();

        if (vertexClass.equals(StreamIterationHead.class)
                || vertexClass.equals(StreamIterationTail.class)) {
            config.setIterationId(streamGraph.getBrokerID(vertexId));
            config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexId));
        }

        vertexConfigs.put(vertexId, config);
    }

    private void setOperatorChainedOutputsConfig(
            StreamConfig config, List<StreamEdge> chainableOutputs) {
        // iterate edges, find sideOutput edges create and save serializers for each outputTag type
        for (StreamEdge edge : chainableOutputs) {
            if (edge.getOutputTag() != null) {
                config.setTypeSerializerSideOut(
                        edge.getOutputTag(),
                        edge.getOutputTag()
                                .getTypeInfo()
                                .createSerializer(
                                        streamGraph.getExecutionConfig().getSerializerConfig()));
            }
        }
        config.setChainedOutputs(chainableOutputs);
    }

    private void setOperatorNonChainedOutputsConfig(
            Integer vertexId,
            StreamConfig config,
            List<StreamEdge> nonChainableOutputs,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge) {
        // iterate edges, find sideOutput edges create and save serializers for each outputTag type
        for (StreamEdge edge : nonChainableOutputs) {
            if (edge.getOutputTag() != null) {
                config.setTypeSerializerSideOut(
                        edge.getOutputTag(),
                        edge.getOutputTag()
                                .getTypeInfo()
                                .createSerializer(
                                        streamGraph.getExecutionConfig().getSerializerConfig()));
            }
        }

        List<NonChainedOutput> deduplicatedOutputs =
                mayReuseNonChainedOutputs(vertexId, nonChainableOutputs, outputsConsumedByEdge);
        config.setNumberOfOutputs(deduplicatedOutputs.size());
        config.setOperatorNonChainedOutputs(deduplicatedOutputs);
    }

    private void setVertexNonChainedOutputsConfig(
            Integer startNodeId,
            StreamConfig config,
            List<StreamEdge> transitiveOutEdges,
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs) {

        LinkedHashSet<NonChainedOutput> transitiveOutputs = new LinkedHashSet<>();
        for (StreamEdge edge : transitiveOutEdges) {
            NonChainedOutput output = opIntermediateOutputs.get(edge.getSourceId()).get(edge);
            transitiveOutputs.add(output);
            connect(startNodeId, edge, output);
        }

        config.setVertexNonChainedOutputs(new ArrayList<>(transitiveOutputs));
    }

    private void setAllOperatorNonChainedOutputsConfigs(
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs) {
        // set non chainable output config
        opNonChainableOutputsCache.forEach(
                (vertexId, nonChainableOutputs) -> {
                    Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge =
                            opIntermediateOutputs.computeIfAbsent(
                                    vertexId, ignored -> new HashMap<>());
                    setOperatorNonChainedOutputsConfig(
                            vertexId,
                            vertexConfigs.get(vertexId),
                            nonChainableOutputs,
                            outputsConsumedByEdge);
                });
    }

    private void setAllVertexNonChainedOutputsConfigs(
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs) {
        jobVertices
                .keySet()
                .forEach(
                        startNodeId ->
                                setVertexNonChainedOutputsConfig(
                                        startNodeId,
                                        vertexConfigs.get(startNodeId),
                                        chainInfos.get(startNodeId).getTransitiveOutEdges(),
                                        opIntermediateOutputs));
    }

    private List<NonChainedOutput> mayReuseNonChainedOutputs(
            int vertexId,
            List<StreamEdge> consumerEdges,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge) {
        if (consumerEdges.isEmpty()) {
            return new ArrayList<>();
        }
        List<NonChainedOutput> outputs = new ArrayList<>(consumerEdges.size());
        for (StreamEdge consumerEdge : consumerEdges) {
            checkState(vertexId == consumerEdge.getSourceId(), "Vertex id must be the same.");
            ResultPartitionType partitionType = getResultPartitionType(consumerEdge);
            IntermediateDataSetID dataSetId = new IntermediateDataSetID();

            boolean isPersistentDataSet =
                    isPersistentIntermediateDataset(partitionType, consumerEdge);
            if (isPersistentDataSet) {
                partitionType = ResultPartitionType.BLOCKING_PERSISTENT;
                dataSetId = consumerEdge.getIntermediateDatasetIdToProduce();
            }

            if (partitionType.isHybridResultPartition()) {
                hasHybridResultPartition = true;
                if (consumerEdge.getPartitioner().isBroadcast()
                        && partitionType == ResultPartitionType.HYBRID_SELECTIVE) {
                    // for broadcast result partition, it can be optimized to always use full
                    // spilling strategy to significantly reduce shuffle data writing cost.
                    LOG.info(
                            "{} result partition has been replaced by {} result partition to support "
                                    + "broadcast optimization, which will reduce shuffle data writing cost.",
                            partitionType.name(),
                            ResultPartitionType.HYBRID_FULL.name());
                    partitionType = ResultPartitionType.HYBRID_FULL;
                }
            }

            createOrReuseOutput(
                    outputs,
                    outputsConsumedByEdge,
                    consumerEdge,
                    isPersistentDataSet,
                    dataSetId,
                    partitionType);
        }
        return outputs;
    }

    private void createOrReuseOutput(
            List<NonChainedOutput> outputs,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge,
            StreamEdge consumerEdge,
            boolean isPersistentDataSet,
            IntermediateDataSetID dataSetId,
            ResultPartitionType partitionType) {
        int consumerParallelism =
                streamGraph.getStreamNode(consumerEdge.getTargetId()).getParallelism();
        int consumerMaxParallelism =
                streamGraph.getStreamNode(consumerEdge.getTargetId()).getMaxParallelism();
        NonChainedOutput reusableOutput = null;
        if (isPartitionTypeCanBeReuse(partitionType)) {
            for (NonChainedOutput outputCandidate : outputsConsumedByEdge.values()) {
                // Reusing the same output can improve performance. The target output can be reused
                // if meeting the following conditions:
                // 1. all is hybrid partition or are same re-consumable partition.
                // 2. have the same partitioner, consumer parallelism, persistentDataSetId,
                // outputTag.
                if (allHybridOrSameReconsumablePartitionType(
                                outputCandidate.getPartitionType(), partitionType)
                        && consumerParallelism == outputCandidate.getConsumerParallelism()
                        && consumerMaxParallelism == outputCandidate.getConsumerMaxParallelism()
                        && Objects.equals(
                                outputCandidate.getPersistentDataSetId(),
                                consumerEdge.getIntermediateDatasetIdToProduce())
                        && Objects.equals(
                                outputCandidate.getOutputTag(), consumerEdge.getOutputTag())
                        && Objects.equals(
                                consumerEdge.getPartitioner(), outputCandidate.getPartitioner())) {
                    reusableOutput = outputCandidate;
                    outputsConsumedByEdge.put(consumerEdge, reusableOutput);
                    checkAndReplaceReusableHybridPartitionType(reusableOutput);
                    break;
                }
            }
        }
        if (reusableOutput == null) {
            NonChainedOutput output =
                    new NonChainedOutput(
                            consumerEdge.supportsUnalignedCheckpoints(),
                            consumerEdge.getSourceId(),
                            consumerParallelism,
                            consumerMaxParallelism,
                            consumerEdge.getBufferTimeout(),
                            isPersistentDataSet,
                            dataSetId,
                            consumerEdge.getOutputTag(),
                            consumerEdge.getPartitioner(),
                            partitionType);
            outputs.add(output);
            outputsConsumedByEdge.put(consumerEdge, output);
        }
    }

    private boolean isPartitionTypeCanBeReuse(ResultPartitionType partitionType) {
        // for non-hybrid partition, partition reuse only works when its re-consumable.
        // for hybrid selective partition, it still has the opportunity to be converted to
        // hybrid full partition to support partition reuse.
        return partitionType.isReconsumable() || partitionType.isHybridResultPartition();
    }

    private boolean allHybridOrSameReconsumablePartitionType(
            ResultPartitionType partitionType1, ResultPartitionType partitionType2) {
        return (partitionType1.isReconsumable() && partitionType1 == partitionType2)
                || (partitionType1.isHybridResultPartition()
                        && partitionType2.isHybridResultPartition());
    }

    private void tryConvertPartitionerForDynamicGraph(
            List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

        for (StreamEdge edge : chainableOutputs) {
            StreamPartitioner<?> partitioner = edge.getPartitioner();
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner
                    || partitioner instanceof ForwardForUnspecifiedPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        String.format(
                                "%s should only be used in dynamic graph.",
                                partitioner.getClass().getSimpleName()));
                edge.setPartitioner(new ForwardPartitioner<>());
            }
        }
        for (StreamEdge edge : nonChainableOutputs) {
            StreamPartitioner<?> partitioner = edge.getPartitioner();
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        "ForwardForConsecutiveHashPartitioner should only be used in dynamic graph.");
                edge.setPartitioner(
                        ((ForwardForConsecutiveHashPartitioner<?>) partitioner)
                                .getHashPartitioner());
            } else if (partitioner instanceof ForwardForUnspecifiedPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        "ForwardForUnspecifiedPartitioner should only be used in dynamic graph.");
                edge.setPartitioner(new RescalePartitioner<>());
            }
        }
    }

    private CheckpointingMode getCheckpointingMode(CheckpointConfig checkpointConfig) {
        CheckpointingMode checkpointingMode = checkpointConfig.getCheckpointingConsistencyMode();

        checkArgument(
                checkpointingMode == CheckpointingMode.EXACTLY_ONCE
                        || checkpointingMode == CheckpointingMode.AT_LEAST_ONCE,
                "Unexpected checkpointing mode.");

        if (checkpointConfig.isCheckpointingEnabled()) {
            return checkpointingMode;
        } else {
            // the "at-least-once" input handler is slightly cheaper (in the absence of
            // checkpoints),
            // so we use that one if checkpointing is not enabled
            return CheckpointingMode.AT_LEAST_ONCE;
        }
    }

    private void connect(Integer headOfChain, StreamEdge edge, NonChainedOutput output) {

        physicalEdgesInOrder.add(edge);

        Integer downStreamVertexID = edge.getTargetId();

        JobVertex headVertex = jobVertices.get(headOfChain);
        JobVertex downStreamVertex = jobVertices.get(downStreamVertexID);

        StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

        downStreamConfig.setNumberOfNetworkInputs(downStreamConfig.getNumberOfNetworkInputs() + 1);

        StreamPartitioner<?> partitioner = output.getPartitioner();
        ResultPartitionType resultPartitionType = output.getPartitionType();

        checkBufferTimeout(resultPartitionType, edge);

        JobEdge jobEdge;
        if (partitioner.isPointwise()) {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.POINTWISE,
                            resultPartitionType,
                            output.getDataSetId(),
                            partitioner.isBroadcast());
        } else {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.ALL_TO_ALL,
                            resultPartitionType,
                            output.getDataSetId(),
                            partitioner.isBroadcast());
        }

        // set strategy name so that web interface can show it.
        jobEdge.setShipStrategyName(partitioner.toString());
        jobEdge.setForward(partitioner instanceof ForwardPartitioner);
        jobEdge.setDownstreamSubtaskStateMapper(partitioner.getDownstreamSubtaskStateMapper());
        jobEdge.setUpstreamSubtaskStateMapper(partitioner.getUpstreamSubtaskStateMapper());

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "CONNECTED: {} - {} -> {}",
                    partitioner.getClass().getSimpleName(),
                    headOfChain,
                    downStreamVertexID);
        }
    }

    private boolean isPersistentIntermediateDataset(
            ResultPartitionType resultPartitionType, StreamEdge edge) {
        return resultPartitionType.isBlockingOrBlockingPersistentResultPartition()
                && edge.getIntermediateDatasetIdToProduce() != null;
    }

    private void checkBufferTimeout(ResultPartitionType type, StreamEdge edge) {
        long bufferTimeout = edge.getBufferTimeout();
        if (!type.canBePipelinedConsumed()
                && bufferTimeout != ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT) {
            throw new UnsupportedOperationException(
                    "only canBePipelinedConsumed partition support buffer timeout "
                            + bufferTimeout
                            + " for src operator in edge "
                            + edge
                            + ". \nPlease either disable buffer timeout (via -1) or use the canBePipelinedConsumed partition.");
        }
    }

    private ResultPartitionType getResultPartitionType(StreamEdge edge) {
        switch (edge.getExchangeMode()) {
            case PIPELINED:
                return ResultPartitionType.PIPELINED_BOUNDED;
            case BATCH:
                return ResultPartitionType.BLOCKING;
            case HYBRID_FULL:
                return ResultPartitionType.HYBRID_FULL;
            case HYBRID_SELECTIVE:
                return ResultPartitionType.HYBRID_SELECTIVE;
            case UNDEFINED:
                return determineUndefinedResultPartitionType(edge);
            default:
                throw new UnsupportedOperationException(
                        "Data exchange mode " + edge.getExchangeMode() + " is not supported yet.");
        }
    }

    private ResultPartitionType determineUndefinedResultPartitionType(StreamEdge edge) {
        if (outputBlockingNodesID.contains(edge.getSourceId())) {
            edge.setBufferTimeout(ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT);
            return ResultPartitionType.BLOCKING;
        }

        StreamPartitioner<?> partitioner = edge.getPartitioner();
        switch (streamGraph.getGlobalStreamExchangeMode()) {
            case ALL_EDGES_BLOCKING:
                return ResultPartitionType.BLOCKING;
            case FORWARD_EDGES_PIPELINED:
                if (partitioner instanceof ForwardPartitioner) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case POINTWISE_EDGES_PIPELINED:
                if (partitioner.isPointwise()) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case ALL_EDGES_PIPELINED:
                return ResultPartitionType.PIPELINED_BOUNDED;
            case ALL_EDGES_PIPELINED_APPROXIMATE:
                return ResultPartitionType.PIPELINED_APPROXIMATE;
            case ALL_EDGES_HYBRID_FULL:
                return ResultPartitionType.HYBRID_FULL;
            case ALL_EDGES_HYBRID_SELECTIVE:
                return ResultPartitionType.HYBRID_SELECTIVE;
            default:
                throw new RuntimeException(
                        "Unrecognized global data exchange mode "
                                + streamGraph.getGlobalStreamExchangeMode());
        }
    }

    /**
     * ★★★ 算子链的判定入口：判断一条 StreamEdge 两端能否并入同一条链。
     *
     * <p>一条边要"可链"，必须**同时**满足下面所有条件（按代码顺序）：
     *
     * <ol>
     *   <li><b>下游只有这一条入边</b>（{@code downStreamVertex.getInEdges().size() == 1}）。
     *       多输入的算子（join、co-process、union 等）无法链，因为它需要多个输入通道。
     *   <li>下面 {@link #isChainableInput} 里的全套条件，其中：
     *       <ul>
     *         <li>全局开关 {@code streamGraph.isChainingEnabled()}
     *             （对应 {@code pipeline.operator-chaining.enabled}，可整体关闭）；
     *         <li>上下游在<b>同一个 SlotSharingGroup</b> 内
     *             （{@code isSameSlotSharingGroup}）—— 不同组的算子会落到不同 slot，
     *             自然不可能同线程；
     *         <li>{@link #areOperatorsChainable}：两个算子的 {@code ChainingStrategy}
     *             组合允许，且<b>并行度必须相同</b>
     *             （未开启新特性时 maxParallelism 也必须相同）；
     *         <li>{@link #arePartitionerAndExchangeModeChainable}：只允许
     *             {@code ForwardPartitioner}（且 exchangeMode 不是 BATCH）。
     *             任何需要重分区的边（hash / rebalance / broadcast / custom）都会断链，
     *             因为重分区意味着数据必须经过网络栈。
     *       </ul>
     *   <li><b>排除 union</b>：遍历下游所有入边，若存在另一条入边与当前边
     *       {@code typeNumber}（即输入位置）相同，则不可链。源码注释解释了原因：
     *       "unions currently only work through the network/byte-channel stack"
     *       —— union 的语义要求走 byte-channel，链内直传实现不了。
     * </ol>
     *
     * <p>注意 {@code ChainingStrategy} 的四个取值及其含义：
     * {@code ALWAYS}（默认，总是可链）、{@code NEVER}（禁止链）、
     * {@code HEAD}（必须作为链头，不能被链到上游）、
     * {@code HEAD_WITH_SOURCES}（作为链头，且允许把 source 作为 chained input 收进来）。
     */
    public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

        // 条件 1：下游只能有这一个输入
        return downStreamVertex.getInEdges().size() == 1 && isChainableInput(edge, streamGraph);
    }

    private static boolean isChainableInput(StreamEdge edge, StreamGraph streamGraph) {
        StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

        if (!(streamGraph.isChainingEnabled()
                && upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
                && areOperatorsChainable(upStreamVertex, downStreamVertex, streamGraph)
                && arePartitionerAndExchangeModeChainable(
                        edge.getPartitioner(), edge.getExchangeMode(), streamGraph.isDynamic()))) {

            return false;
        }

        // check that we do not have a union operation, because unions currently only work
        // through the network/byte-channel stack.
        // we check that by testing that each "type" (which means input position) is used only once
        for (StreamEdge inEdge : downStreamVertex.getInEdges()) {
            if (inEdge != edge && inEdge.getTypeNumber() == edge.getTypeNumber()) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    static boolean arePartitionerAndExchangeModeChainable(
            StreamPartitioner<?> partitioner,
            StreamExchangeMode exchangeMode,
            boolean isDynamicGraph) {
        // 条件 3（partitioner 层面）：只有 ForwardPartitioner 能链。
        // 其余 partitioner —— hash / rebalance / rescale / broadcast / custom ——
        // 都意味着"按某种规则把数据分发到下游不同 subtask"，
        // 这必须经由 ResultPartition/InputChannel 的网络栈，链内直传无法表达。
        if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
            checkState(isDynamicGraph);
            return true;
        } else if ((partitioner instanceof ForwardPartitioner)
                && exchangeMode != StreamExchangeMode.BATCH) {
            return true;
        } else {
            return false;
        }
    }

    @VisibleForTesting
    static boolean areOperatorsChainable(
            StreamNode upStreamVertex, StreamNode downStreamVertex, StreamGraph streamGraph) {
        StreamOperatorFactory<?> upStreamOperator = upStreamVertex.getOperatorFactory();
        StreamOperatorFactory<?> downStreamOperator = downStreamVertex.getOperatorFactory();
        if (downStreamOperator == null || upStreamOperator == null) {
            return false;
        }

        // yielding operators cannot be chained to legacy sources
        // unfortunately the information that vertices have been chained is not preserved at this
        // point
        if (downStreamOperator instanceof YieldingOperatorFactory
                && getHeadOperator(upStreamVertex, streamGraph).isLegacySource()) {
            return false;
        }

        // 条件 4（算子层面）：两个算子的 ChainingStrategy 组合必须允许。
        // 上游策略先定基调：NEVER 直接否决；ALWAYS / HEAD / HEAD_WITH_SOURCES 都允许往下传。
        // we use switch/case here to make sure this is exhaustive if ever values are added to the
        // ChainingStrategy enum
        boolean isChainable;

        switch (upStreamOperator.getChainingStrategy()) {
            case NEVER:
                isChainable = false;
                break;
            case ALWAYS:
            case HEAD:
            case HEAD_WITH_SOURCES:
                isChainable = true;
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + upStreamOperator.getChainingStrategy());
        }

        // 下游策略再做修正：
        //   NEVER        → 否决，它要独占一个 JobVertex（如某些 sink）
        //   HEAD         → 否决，它必须当链头，不能被链到上游
        //   ALWAYS       → 保留上游的判定结果
        //   HEAD_WITH_SOURCES → 仅当上游本身是 SourceOperatorFactory 时才允许（N-ary 算子场景）
        switch (downStreamOperator.getChainingStrategy()) {
            case NEVER:
            case HEAD:
                isChainable = false;
                break;
            case ALWAYS:
                // keep the value from upstream
                break;
            case HEAD_WITH_SOURCES:
                // only if upstream is a source
                isChainable &= (upStreamOperator instanceof SourceOperatorFactory);
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + downStreamOperator.getChainingStrategy());
        }

        // 条件 5：只有**并行度相同**的算子才能链。
        // 这是链式合并最本质的约束：链内所有算子跑在同一个线程里、
        // 一一对应地传递记录，所以个数必须相等。
        // （这也是为什么 source(1) -> map(4) 会断链，而不是被强行合并。）
        // Only vertices with the same parallelism can be chained.
        isChainable &= upStreamVertex.getParallelism() == downStreamVertex.getParallelism();

        // 条件 6：默认还要求 maxParallelism 也相同。
        // 可以通过开关放宽（用于允许 maxParallelism 不同的算子链在一起，
        // 代价是链内 key group 划分需要额外处理）。
        if (!streamGraph.isChainingOfOperatorsWithDifferentMaxParallelismEnabled()) {
            isChainable &=
                    upStreamVertex.getMaxParallelism() == downStreamVertex.getMaxParallelism();
        }

        return isChainable;
    }

    /** Backtraces the head of an operator chain. */
    private static StreamOperatorFactory<?> getHeadOperator(
            StreamNode upStreamVertex, StreamGraph streamGraph) {
        if (streamGraph.getHeadOperatorForNodeFromCache(upStreamVertex) == null) {
            if (upStreamVertex.getInEdges().size() == 1
                    && isChainable(upStreamVertex.getInEdges().get(0), streamGraph)) {
                StreamOperatorFactory<?> headOperator =
                        getHeadOperator(
                                streamGraph.getSourceVertex(upStreamVertex.getInEdges().get(0)),
                                streamGraph);
                streamGraph.cacheHeadOperatorForNode(upStreamVertex, headOperator);
            } else {
                Preconditions.checkNotNull(upStreamVertex.getOperatorFactory());
                streamGraph.cacheHeadOperatorForNode(
                        upStreamVertex, upStreamVertex.getOperatorFactory());
            }
        }

        return streamGraph.getHeadOperatorForNodeFromCache(upStreamVertex);
    }

    private void markSupportingConcurrentExecutionAttempts() {
        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
            final JobVertex jobVertex = entry.getValue();
            final Set<Integer> vertexOperators = new HashSet<>();
            vertexOperators.add(entry.getKey());
            final Map<Integer, StreamConfig> vertexChainedConfigs =
                    chainedConfigs.get(entry.getKey());
            if (vertexChainedConfigs != null) {
                vertexOperators.addAll(vertexChainedConfigs.keySet());
            }

            // disable supportConcurrentExecutionAttempts of job vertex if there is any stream node
            // does not support it
            boolean supportConcurrentExecutionAttempts = true;
            for (int nodeId : vertexOperators) {
                final StreamNode streamNode = streamGraph.getStreamNode(nodeId);
                if (!streamNode.isSupportsConcurrentExecutionAttempts()) {
                    supportConcurrentExecutionAttempts = false;
                    break;
                }
            }
            jobVertex.setSupportsConcurrentExecutionAttempts(supportConcurrentExecutionAttempts);
        }
    }

    private void setSlotSharingAndCoLocation() {
        setSlotSharing();
        setCoLocation();
    }

    private void setSlotSharing() {
        final Map<String, SlotSharingGroup> specifiedSlotSharingGroups = new HashMap<>();
        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups =
                buildVertexRegionSlotSharingGroups();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final JobVertex vertex = entry.getValue();
            final String slotSharingGroupKey =
                    streamGraph.getStreamNode(entry.getKey()).getSlotSharingGroup();

            checkNotNull(slotSharingGroupKey, "StreamNode slot sharing group must not be null");

            final SlotSharingGroup effectiveSlotSharingGroup;
            if (slotSharingGroupKey.equals(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)) {
                // fallback to the region slot sharing group by default
                effectiveSlotSharingGroup =
                        checkNotNull(vertexRegionSlotSharingGroups.get(vertex.getID()));
            } else {
                checkState(
                        !hasHybridResultPartition,
                        "hybrid shuffle mode currently does not support setting non-default slot sharing group.");

                effectiveSlotSharingGroup =
                        specifiedSlotSharingGroups.computeIfAbsent(
                                slotSharingGroupKey,
                                k -> {
                                    SlotSharingGroup ssg = new SlotSharingGroup();
                                    streamGraph
                                            .getSlotSharingGroupResource(k)
                                            .ifPresent(ssg::setResourceProfile);
                                    return ssg;
                                });
            }

            vertex.setSlotSharingGroup(effectiveSlotSharingGroup);
        }
    }

    private void validateHybridShuffleExecuteInBatchMode() {
        if (hasHybridResultPartition) {
            checkState(
                    jobGraph.getJobType() == JobType.BATCH,
                    "hybrid shuffle mode only supports batch job, please set %s to %s",
                    ExecutionOptions.RUNTIME_MODE.key(),
                    RuntimeExecutionMode.BATCH.name());
        }
    }

    /**
     * Maps a vertex to its region slot sharing group. If {@link
     * StreamGraph#isAllVerticesInSameSlotSharingGroupByDefault()} returns true, all regions will be
     * in the same slot sharing group.
     */
    private Map<JobVertexID, SlotSharingGroup> buildVertexRegionSlotSharingGroups() {
        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = new HashMap<>();
        final SlotSharingGroup defaultSlotSharingGroup = new SlotSharingGroup();
        streamGraph
                .getSlotSharingGroupResource(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                .ifPresent(defaultSlotSharingGroup::setResourceProfile);

        final boolean allRegionsInSameSlotSharingGroup =
                streamGraph.isAllVerticesInSameSlotSharingGroupByDefault();

        final Iterable<DefaultLogicalPipelinedRegion> regions =
                DefaultLogicalTopology.fromJobGraph(jobGraph).getAllPipelinedRegions();
        for (DefaultLogicalPipelinedRegion region : regions) {
            final SlotSharingGroup regionSlotSharingGroup;
            if (allRegionsInSameSlotSharingGroup) {
                regionSlotSharingGroup = defaultSlotSharingGroup;
            } else {
                regionSlotSharingGroup = new SlotSharingGroup();
                streamGraph
                        .getSlotSharingGroupResource(
                                StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                        .ifPresent(regionSlotSharingGroup::setResourceProfile);
            }

            for (LogicalVertex vertex : region.getVertices()) {
                vertexRegionSlotSharingGroups.put(vertex.getId(), regionSlotSharingGroup);
            }
        }

        return vertexRegionSlotSharingGroups;
    }

    private void setCoLocation() {
        final Map<String, Tuple2<SlotSharingGroup, CoLocationGroupImpl>> coLocationGroups =
                new HashMap<>();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final StreamNode node = streamGraph.getStreamNode(entry.getKey());
            final JobVertex vertex = entry.getValue();
            final SlotSharingGroup sharingGroup = vertex.getSlotSharingGroup();

            // configure co-location constraint
            final String coLocationGroupKey = node.getCoLocationGroup();
            if (coLocationGroupKey != null) {
                if (sharingGroup == null) {
                    throw new IllegalStateException(
                            "Cannot use a co-location constraint without a slot sharing group");
                }

                Tuple2<SlotSharingGroup, CoLocationGroupImpl> constraint =
                        coLocationGroups.computeIfAbsent(
                                coLocationGroupKey,
                                k -> new Tuple2<>(sharingGroup, new CoLocationGroupImpl()));

                if (constraint.f0 != sharingGroup) {
                    throw new IllegalStateException(
                            "Cannot co-locate operators from different slot sharing groups");
                }

                vertex.updateCoLocationGroup(constraint.f1);
                constraint.f1.addVertex(vertex);
            }
        }
    }

    private static void setManagedMemoryFraction(
            final Map<Integer, JobVertex> jobVertices,
            final Map<Integer, StreamConfig> operatorConfigs,
            final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
            final java.util.function.Function<Integer, Map<ManagedMemoryUseCase, Integer>>
                    operatorScopeManagedMemoryUseCaseWeightsRetriever,
            final java.util.function.Function<Integer, Set<ManagedMemoryUseCase>>
                    slotScopeManagedMemoryUseCasesRetriever) {

        // all slot sharing groups in this job
        final Set<SlotSharingGroup> slotSharingGroups =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // maps a job vertex ID to its head operator ID
        final Map<JobVertexID, Integer> vertexHeadOperators = new HashMap<>();

        // maps a job vertex ID to IDs of all operators in the vertex
        final Map<JobVertexID, Set<Integer>> vertexOperators = new HashMap<>();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
            final int headOperatorId = entry.getKey();
            final JobVertex jobVertex = entry.getValue();

            final SlotSharingGroup jobVertexSlotSharingGroup = jobVertex.getSlotSharingGroup();

            checkState(
                    jobVertexSlotSharingGroup != null,
                    "JobVertex slot sharing group must not be null");
            slotSharingGroups.add(jobVertexSlotSharingGroup);

            vertexHeadOperators.put(jobVertex.getID(), headOperatorId);

            final Set<Integer> operatorIds = new HashSet<>();
            operatorIds.add(headOperatorId);
            operatorIds.addAll(
                    vertexChainedConfigs
                            .getOrDefault(headOperatorId, Collections.emptyMap())
                            .keySet());
            vertexOperators.put(jobVertex.getID(), operatorIds);
        }

        for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
            setManagedMemoryFractionForSlotSharingGroup(
                    slotSharingGroup,
                    vertexHeadOperators,
                    vertexOperators,
                    operatorConfigs,
                    vertexChainedConfigs,
                    operatorScopeManagedMemoryUseCaseWeightsRetriever,
                    slotScopeManagedMemoryUseCasesRetriever);
        }
    }

    private static void setManagedMemoryFractionForSlotSharingGroup(
            final SlotSharingGroup slotSharingGroup,
            final Map<JobVertexID, Integer> vertexHeadOperators,
            final Map<JobVertexID, Set<Integer>> vertexOperators,
            final Map<Integer, StreamConfig> operatorConfigs,
            final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs,
            final java.util.function.Function<Integer, Map<ManagedMemoryUseCase, Integer>>
                    operatorScopeManagedMemoryUseCaseWeightsRetriever,
            final java.util.function.Function<Integer, Set<ManagedMemoryUseCase>>
                    slotScopeManagedMemoryUseCasesRetriever) {

        final Set<Integer> groupOperatorIds =
                slotSharingGroup.getJobVertexIds().stream()
                        .flatMap((vid) -> vertexOperators.get(vid).stream())
                        .collect(Collectors.toSet());

        final Map<ManagedMemoryUseCase, Integer> groupOperatorScopeUseCaseWeights =
                groupOperatorIds.stream()
                        .flatMap(
                                (oid) ->
                                        operatorScopeManagedMemoryUseCaseWeightsRetriever.apply(oid)
                                                .entrySet().stream())
                        .collect(
                                Collectors.groupingBy(
                                        Map.Entry::getKey,
                                        Collectors.summingInt(Map.Entry::getValue)));

        final Set<ManagedMemoryUseCase> groupSlotScopeUseCases =
                groupOperatorIds.stream()
                        .flatMap(
                                (oid) ->
                                        slotScopeManagedMemoryUseCasesRetriever.apply(oid).stream())
                        .collect(Collectors.toSet());

        for (JobVertexID jobVertexID : slotSharingGroup.getJobVertexIds()) {
            for (int operatorNodeId : vertexOperators.get(jobVertexID)) {
                final StreamConfig operatorConfig = operatorConfigs.get(operatorNodeId);
                final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights =
                        operatorScopeManagedMemoryUseCaseWeightsRetriever.apply(operatorNodeId);
                final Set<ManagedMemoryUseCase> slotScopeUseCases =
                        slotScopeManagedMemoryUseCasesRetriever.apply(operatorNodeId);
                setManagedMemoryFractionForOperator(
                        operatorScopeUseCaseWeights,
                        slotScopeUseCases,
                        groupOperatorScopeUseCaseWeights,
                        groupSlotScopeUseCases,
                        operatorConfig);
            }

            // need to refresh the chained task configs because they are serialized
            final int headOperatorNodeId = vertexHeadOperators.get(jobVertexID);
            final StreamConfig vertexConfig = operatorConfigs.get(headOperatorNodeId);
            vertexConfig.setTransitiveChainedTaskConfigs(
                    vertexChainedConfigs.get(headOperatorNodeId));
        }
    }

    private static void setManagedMemoryFractionForOperator(
            final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights,
            final Set<ManagedMemoryUseCase> slotScopeUseCases,
            final Map<ManagedMemoryUseCase, Integer> groupManagedMemoryWeights,
            final Set<ManagedMemoryUseCase> groupSlotScopeUseCases,
            final StreamConfig operatorConfig) {

        // For each operator, make sure fractions are set for all use cases in the group, even if
        // the operator does not have the use case (set the fraction to 0.0). This allows us to
        // learn which use cases exist in the group from either one of the stream configs.
        for (Map.Entry<ManagedMemoryUseCase, Integer> entry :
                groupManagedMemoryWeights.entrySet()) {
            final ManagedMemoryUseCase useCase = entry.getKey();
            final int groupWeight = entry.getValue();
            final int operatorWeight = operatorScopeUseCaseWeights.getOrDefault(useCase, 0);
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(
                    useCase,
                    operatorWeight > 0
                            ? ManagedMemoryUtils.getFractionRoundedDown(operatorWeight, groupWeight)
                            : 0.0);
        }
        for (ManagedMemoryUseCase useCase : groupSlotScopeUseCases) {
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(
                    useCase, slotScopeUseCases.contains(useCase) ? 1.0 : 0.0);
        }
    }

    private void configureCheckpointing() {
        CheckpointConfig cfg = streamGraph.getCheckpointConfig();

        long interval = cfg.getCheckpointInterval();
        if (interval < MINIMAL_CHECKPOINT_TIME) {
            interval = CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL;
        }

        //  --- configure options ---

        CheckpointRetentionPolicy retentionAfterTermination;
        if (cfg.isExternalizedCheckpointsEnabled()) {
            ExternalizedCheckpointRetention cleanup = cfg.getExternalizedCheckpointRetention();
            // Sanity check
            if (cleanup == null) {
                throw new IllegalStateException(
                        "Externalized checkpoints enabled, but no cleanup mode configured.");
            }
            retentionAfterTermination =
                    cleanup.deleteOnCancellation()
                            ? CheckpointRetentionPolicy.RETAIN_ON_FAILURE
                            : CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
        } else {
            retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
        }

        //  --- configure the master-side checkpoint hooks ---

        final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

        for (StreamNode node : streamGraph.getStreamNodes()) {
            if (node.getOperatorFactory() != null
                    && node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
                Function f =
                        ((UdfStreamOperatorFactory) node.getOperatorFactory()).getUserFunction();

                if (f instanceof WithMasterCheckpointHook) {
                    hooks.add(
                            new FunctionMasterCheckpointHookFactory(
                                    (WithMasterCheckpointHook<?>) f));
                }
            }
        }

        // because the hooks can have user-defined code, they need to be stored as
        // eagerly serialized values
        final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
        if (hooks.isEmpty()) {
            serializedHooks = null;
        } else {
            try {
                MasterTriggerRestoreHook.Factory[] asArray =
                        hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
                serializedHooks = new SerializedValue<>(asArray);
            } catch (IOException e) {
                throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
            }
        }

        // because the state backend can have user-defined code, it needs to be stored as
        // eagerly serialized value
        final SerializedValue<StateBackend> serializedStateBackend;
        if (streamGraph.getStateBackend() == null) {
            serializedStateBackend = null;
        } else {
            try {
                serializedStateBackend =
                        new SerializedValue<StateBackend>(streamGraph.getStateBackend());
            } catch (IOException e) {
                throw new FlinkRuntimeException("State backend is not serializable", e);
            }
        }

        // because the checkpoint storage can have user-defined code, it needs to be stored as
        // eagerly serialized value
        final SerializedValue<CheckpointStorage> serializedCheckpointStorage;
        if (streamGraph.getCheckpointStorage() == null) {
            serializedCheckpointStorage = null;
        } else {
            try {
                serializedCheckpointStorage =
                        new SerializedValue<>(streamGraph.getCheckpointStorage());
            } catch (IOException e) {
                throw new FlinkRuntimeException("Checkpoint storage is not serializable", e);
            }
        }

        //  --- done, put it all together ---

        JobCheckpointingSettings settings =
                new JobCheckpointingSettings(
                        CheckpointCoordinatorConfiguration.builder()
                                .setCheckpointInterval(interval)
                                .setCheckpointIntervalDuringBacklog(
                                        cfg.getCheckpointIntervalDuringBacklog())
                                .setCheckpointTimeout(cfg.getCheckpointTimeout())
                                .setMinPauseBetweenCheckpoints(cfg.getMinPauseBetweenCheckpoints())
                                .setMaxConcurrentCheckpoints(cfg.getMaxConcurrentCheckpoints())
                                .setCheckpointRetentionPolicy(retentionAfterTermination)
                                .setExactlyOnce(
                                        getCheckpointingMode(cfg) == CheckpointingMode.EXACTLY_ONCE)
                                .setTolerableCheckpointFailureNumber(
                                        cfg.getTolerableCheckpointFailureNumber())
                                .setUnalignedCheckpointsEnabled(cfg.isUnalignedCheckpointsEnabled())
                                .setCheckpointIdOfIgnoredInFlightData(
                                        cfg.getCheckpointIdOfIgnoredInFlightData())
                                .setAlignedCheckpointTimeout(
                                        cfg.getAlignedCheckpointTimeout().toMillis())
                                .setEnableCheckpointsAfterTasksFinish(
                                        streamGraph.isEnableCheckpointsAfterTasksFinish())
                                .build(),
                        serializedStateBackend,
                        streamGraph
                                .getJobConfiguration()
                                .getOptional(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)
                                .map(TernaryBoolean::fromBoolean)
                                .orElse(TernaryBoolean.UNDEFINED),
                        serializedCheckpointStorage,
                        serializedHooks);

        jobGraph.setSnapshotSettings(settings);
    }

    private static String nameWithChainedSourcesInfo(
            String operatorName, Collection<ChainedSourceInfo> chainedSourceInfos) {
        return chainedSourceInfos.isEmpty()
                ? operatorName
                : String.format(
                        "%s [%s]",
                        operatorName,
                        chainedSourceInfos.stream()
                                .map(
                                        chainedSourceInfo ->
                                                chainedSourceInfo
                                                        .getOperatorConfig()
                                                        .getOperatorName())
                                .collect(Collectors.joining(", ")));
    }

    /**
     * A private class to help maintain the information of an operator chain during the recursive
     * call in {@link #createChain(Integer, int, OperatorChainInfo, Map)}.
     */
    private static class OperatorChainInfo {
        private final Integer startNodeId;
        private final Map<Integer, byte[]> hashes;
        private final List<Map<Integer, byte[]>> legacyHashes;
        private final Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes;
        private final Map<Integer, ChainedSourceInfo> chainedSources;
        private final List<OperatorCoordinator.Provider> coordinatorProviders;
        private final StreamGraph streamGraph;
        private final List<StreamNode> chainedNodes;
        private final List<StreamEdge> transitiveOutEdges;

        private OperatorChainInfo(
                int startNodeId,
                Map<Integer, byte[]> hashes,
                List<Map<Integer, byte[]>> legacyHashes,
                Map<Integer, ChainedSourceInfo> chainedSources,
                StreamGraph streamGraph) {
            this.startNodeId = startNodeId;
            this.hashes = hashes;
            this.legacyHashes = legacyHashes;
            this.chainedOperatorHashes = new HashMap<>();
            this.coordinatorProviders = new ArrayList<>();
            this.chainedSources = chainedSources;
            this.streamGraph = streamGraph;
            this.chainedNodes = new ArrayList<>();
            this.transitiveOutEdges = new ArrayList<>();
        }

        byte[] getHash(Integer streamNodeId) {
            return hashes.get(streamNodeId);
        }

        private Integer getStartNodeId() {
            return startNodeId;
        }

        private List<Tuple2<byte[], byte[]>> getChainedOperatorHashes(int startNodeId) {
            return chainedOperatorHashes.get(startNodeId);
        }

        void addCoordinatorProvider(OperatorCoordinator.Provider coordinator) {
            coordinatorProviders.add(coordinator);
        }

        private List<OperatorCoordinator.Provider> getCoordinatorProviders() {
            return coordinatorProviders;
        }

        Map<Integer, ChainedSourceInfo> getChainedSources() {
            return chainedSources;
        }

        private OperatorID addNodeToChain(int currentNodeId, String operatorName) {
            recordChainedNode(currentNodeId);
            StreamNode streamNode = streamGraph.getStreamNode(currentNodeId);

            List<Tuple2<byte[], byte[]>> operatorHashes =
                    chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

            byte[] primaryHashBytes = hashes.get(currentNodeId);

            for (Map<Integer, byte[]> legacyHash : legacyHashes) {
                operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
            }

            streamNode
                    .getCoordinatorProvider(operatorName, new OperatorID(getHash(currentNodeId)))
                    .map(coordinatorProviders::add);

            return new OperatorID(primaryHashBytes);
        }

        private void setTransitiveOutEdges(final List<StreamEdge> transitiveOutEdges) {
            this.transitiveOutEdges.addAll(transitiveOutEdges);
        }

        private List<StreamEdge> getTransitiveOutEdges() {
            return transitiveOutEdges;
        }

        private void recordChainedNode(int currentNodeId) {
            StreamNode streamNode = streamGraph.getStreamNode(currentNodeId);
            chainedNodes.add(streamNode);
        }

        private OperatorChainInfo newChain(Integer startNodeId) {
            return new OperatorChainInfo(
                    startNodeId, hashes, legacyHashes, chainedSources, streamGraph);
        }

        private List<StreamNode> getAllChainedNodes() {
            return chainedNodes;
        }
    }

    private static final class ChainedSourceInfo {
        private final StreamConfig operatorConfig;
        private final StreamConfig.SourceInputConfig inputConfig;

        ChainedSourceInfo(StreamConfig operatorConfig, StreamConfig.SourceInputConfig inputConfig) {
            this.operatorConfig = operatorConfig;
            this.inputConfig = inputConfig;
        }

        public StreamConfig getOperatorConfig() {
            return operatorConfig;
        }

        public StreamConfig.SourceInputConfig getInputConfig() {
            return inputConfig;
        }
    }
}
