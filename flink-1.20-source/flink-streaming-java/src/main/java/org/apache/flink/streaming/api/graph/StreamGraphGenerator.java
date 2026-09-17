/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.util.SlotSharingGroupUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionCheckpointStorage;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionInternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionStateBackend;
import org.apache.flink.streaming.api.transformations.BroadcastStateTransformation;
import org.apache.flink.streaming.api.transformations.CacheTransformation;
import org.apache.flink.streaming.api.transformations.CoFeedbackTransformation;
import org.apache.flink.streaming.api.transformations.FeedbackTransformation;
import org.apache.flink.streaming.api.transformations.GlobalCommitterTransform;
import org.apache.flink.streaming.api.transformations.KeyedBroadcastStateTransformation;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.api.transformations.ReduceTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformationWrapper;
import org.apache.flink.streaming.api.transformations.TimestampsAndWatermarksTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.streaming.api.transformations.WithBoundedness;
import org.apache.flink.streaming.runtime.translators.BroadcastStateTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.CacheTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.GlobalCommitterTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.KeyedBroadcastStateTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.LegacySinkTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.LegacySourceTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.MultiInputTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.OneInputTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.PartitionTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.ReduceTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.SideOutputTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.SinkTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.SourceTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.TimestampsAndWatermarksTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.TwoInputTransformationTranslator;
import org.apache.flink.streaming.runtime.translators.UnionTransformationTranslator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A generator that generates a {@link StreamGraph} from a graph of {@link Transformation}s.
 *
 * <p>This traverses the tree of {@code Transformations} starting from the sinks. At each
 * transformation we recursively transform the inputs, then create a node in the {@code StreamGraph}
 * and add edges from the input Nodes to our newly created node. The transformation methods return
 * the IDs of the nodes in the StreamGraph that represent the input transformation. Several IDs can
 * be returned to be able to deal with feedback transformations and unions.
 *
 * <p>Partitioning, split/select and union don't create actual nodes in the {@code StreamGraph}. For
 * these, we create a virtual node in the {@code StreamGraph} that holds the specific property, i.e.
 * partitioning, selector and so on. When an edge is created from a virtual node to a downstream
 * node the {@code StreamGraph} resolved the id of the original node and creates an edge in the
 * graph with the desired property. For example, if you have this graph:
 *
 * <pre>
 *     Map-1 -&gt; HashPartition-2 -&gt; Map-3
 * </pre>
 *
 * <p>where the numbers represent transformation IDs. We first recurse all the way down. {@code
 * Map-1} is transformed, i.e. we create a {@code StreamNode} with ID 1. Then we transform the
 * {@code HashPartition}, for this, we create virtual node of ID 4 that holds the property {@code
 * HashPartition}. This transformation returns the ID 4. Then we transform the {@code Map-3}. We add
 * the edge {@code 4 -> 3}. The {@code StreamGraph} resolved the actual node with ID 1 and creates
 * and edge {@code 1 -> 3} with the property HashPartition.
 */
@Internal
public class StreamGraphGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamGraphGenerator.class);

    public static final int DEFAULT_LOWER_BOUND_MAX_PARALLELISM =
            KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

    public static final TimeCharacteristic DEFAULT_TIME_CHARACTERISTIC =
            TimeCharacteristic.ProcessingTime;

    public static final String DEFAULT_STREAMING_JOB_NAME = "Flink Streaming Job";

    public static final String DEFAULT_BATCH_JOB_NAME = "Flink Batch Job";

    public static final String DEFAULT_SLOT_SHARING_GROUP = "default";

    private final List<Transformation<?>> transformations;

    private final ExecutionConfig executionConfig;

    private final CheckpointConfig checkpointConfig;

    private final Configuration configuration;

    // Records the slot sharing groups and their corresponding fine-grained ResourceProfile
    private final Map<String, ResourceProfile> slotSharingGroupResources = new HashMap<>();

    private StateBackend stateBackend;

    private CheckpointStorage checkpointStorage;

    private TimeCharacteristic timeCharacteristic = DEFAULT_TIME_CHARACTERISTIC;

    private SavepointRestoreSettings savepointRestoreSettings;

    private boolean shouldExecuteInBatchMode;

    @SuppressWarnings("rawtypes")
    private static final Map<
                    Class<? extends Transformation>,
                    TransformationTranslator<?, ? extends Transformation>>
            translatorMap;

    static {
        @SuppressWarnings("rawtypes")
        Map<Class<? extends Transformation>, TransformationTranslator<?, ? extends Transformation>>
                tmp = new HashMap<>();
        tmp.put(OneInputTransformation.class, new OneInputTransformationTranslator<>());
        tmp.put(TwoInputTransformation.class, new TwoInputTransformationTranslator<>());
        tmp.put(MultipleInputTransformation.class, new MultiInputTransformationTranslator<>());
        tmp.put(KeyedMultipleInputTransformation.class, new MultiInputTransformationTranslator<>());
        tmp.put(SourceTransformation.class, new SourceTransformationTranslator<>());
        tmp.put(SinkTransformation.class, new SinkTransformationTranslator<>());
        tmp.put(GlobalCommitterTransform.class, new GlobalCommitterTransformationTranslator<>());
        tmp.put(LegacySinkTransformation.class, new LegacySinkTransformationTranslator<>());
        tmp.put(LegacySourceTransformation.class, new LegacySourceTransformationTranslator<>());
        tmp.put(UnionTransformation.class, new UnionTransformationTranslator<>());
        tmp.put(PartitionTransformation.class, new PartitionTransformationTranslator<>());
        tmp.put(SideOutputTransformation.class, new SideOutputTransformationTranslator<>());
        tmp.put(ReduceTransformation.class, new ReduceTransformationTranslator<>());
        tmp.put(
                TimestampsAndWatermarksTransformation.class,
                new TimestampsAndWatermarksTransformationTranslator<>());
        tmp.put(BroadcastStateTransformation.class, new BroadcastStateTransformationTranslator<>());
        tmp.put(
                KeyedBroadcastStateTransformation.class,
                new KeyedBroadcastStateTransformationTranslator<>());
        tmp.put(CacheTransformation.class, new CacheTransformationTranslator<>());
        translatorMap = Collections.unmodifiableMap(tmp);
    }

    // This is used to assign a unique ID to iteration source/sink
    protected static Integer iterationIdCounter = 0;

    public static int getNewIterationNodeId() {
        iterationIdCounter--;
        return iterationIdCounter;
    }

    private StreamGraph streamGraph;

    // Keep track of which Transforms we have already transformed, this is necessary because
    // we have loops, i.e. feedback edges.
    private Map<Transformation<?>, Collection<Integer>> alreadyTransformed;

    public StreamGraphGenerator(
            final List<Transformation<?>> transformations,
            final ExecutionConfig executionConfig,
            final CheckpointConfig checkpointConfig) {
        this(transformations, executionConfig, checkpointConfig, new Configuration());
    }

    public StreamGraphGenerator(
            List<Transformation<?>> transformations,
            ExecutionConfig executionConfig,
            CheckpointConfig checkpointConfig,
            Configuration configuration) {
        this.transformations = checkNotNull(transformations);
        this.executionConfig = checkNotNull(executionConfig);
        this.checkpointConfig = new CheckpointConfig(checkpointConfig);
        this.configuration = checkNotNull(configuration);
        this.checkpointStorage = this.checkpointConfig.getCheckpointStorage();
        this.savepointRestoreSettings = SavepointRestoreSettings.fromConfiguration(configuration);
    }

    public StreamGraphGenerator setStateBackend(StateBackend stateBackend) {
        this.stateBackend = stateBackend;
        return this;
    }

    public StreamGraphGenerator setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
        this.timeCharacteristic = timeCharacteristic;
        return this;
    }

    /**
     * Specify fine-grained resource requirements for slot sharing groups.
     *
     * <p>Note that a slot sharing group hints the scheduler that the grouped operators CAN be
     * deployed into a shared slot. There's no guarantee that the scheduler always deploy the
     * grouped operators together. In cases grouped operators are deployed into separate slots, the
     * slot resources will be derived from the specified group requirements.
     */
    public StreamGraphGenerator setSlotSharingGroupResource(
            Map<String, ResourceProfile> slotSharingGroupResources) {
        slotSharingGroupResources.forEach(
                (name, profile) -> {
                    if (!profile.equals(ResourceProfile.UNKNOWN)) {
                        this.slotSharingGroupResources.put(name, profile);
                    }
                });
        return this;
    }

    public void setSavepointRestoreSettings(SavepointRestoreSettings savepointRestoreSettings) {
        this.savepointRestoreSettings = savepointRestoreSettings;
    }

    //生成StreamGraph
//    public StreamGraph generate() {
//        streamGraph =
//                new StreamGraph(
//                        configuration, executionConfig, checkpointConfig, savepointRestoreSettings);
//        shouldExecuteInBatchMode = shouldExecuteInBatchMode();
//        configureStreamGraph(streamGraph);
//
//        alreadyTransformed = new IdentityHashMap<>();
//
//        for (Transformation<?> transformation : transformations) {
//            transform(transformation);
//        }
//
//        streamGraph.setSlotSharingGroupResource(slotSharingGroupResources);
//
//        setFineGrainedGlobalStreamExchangeMode(streamGraph);
//
//        for (StreamNode node : streamGraph.getStreamNodes()) {
//            if (node.getInEdges().stream()
//                    .anyMatch(e -> !e.getPartitioner().isSupportsUnalignedCheckpoint())) {
//                for (StreamEdge edge : node.getInEdges()) {
//                    edge.setSupportsUnalignedCheckpoints(false);
//                }
//            }
//        }
//
//        final StreamGraph builtStreamGraph = streamGraph;
//
//        alreadyTransformed.clear();
//        alreadyTransformed = null;
//        streamGraph = null;
//
//        return builtStreamGraph;
//    }
    //todo 生成StreamGraph
    public StreamGraph generate() {
        // ① 先造一个"空壳" StreamGraph：此刻图里没有任何节点，
        //    只是把 ExecutionEnvironment 的环境级配置接手过来：
        //      configuration            —— 作业级配置，最终会变成 JobGraph 的 jobConfiguration
        //      executionConfig          —— 并行度 / 序列化器 / 重启策略 / maxParallelism 等
        //      checkpointConfig         —— CheckpointConfig（间隔、模式、超时…）
        //      savepointRestoreSettings —— CLI 的 -s / -n / -cm 所对应的恢复设置
        streamGraph =
                new StreamGraph(
                        configuration, executionConfig, checkpointConfig, savepointRestoreSettings);

        // ② 先给这次作业定性：STREAMING 还是 BATCH。判定逻辑见 shouldExecuteInBatchMode()：
        //      - execution.runtime-mode 显式配了 STREAMING/BATCH → 听配置的；
        //      - 配成 AUTOMATIC → 存在无界源就 STREAMING，全是有界源就 BATCH。
        //    结果要存成字段，因为后面的 configureStreamGraph、setDynamic、
        //    setFineGrainedGlobalStreamExchangeMode 都要反复读它。
        //    这里还会顺带 checkState 掉一个非法组合：BATCH 模式 + 存在无界源，直接报错拒绝执行。
        shouldExecuteInBatchMode = shouldExecuteInBatchMode();

        // ③ 依据 ② 的结论，把作业级属性刷进空壳。内部是 batch / streaming 二选一：
        //      streaming: jobName("Flink Streaming Job")、JobType.STREAMING、
        //                 stateBackend、checkpointStorage、全局 StreamExchangeMode
        //      batch    : jobName("Flink Batch Job")、JobType.BATCH、关闭 checkpointing、
        //                 批量 state backend、batch shuffle mode、且默认不在同一 slot sharing group
        //    注意 batch 分支还有个副作用：configuration.set(BUFFER_TIMEOUT_ENABLED, false)。
        configureStreamGraph(streamGraph);

        // ④ 递归遍历的"记忆化"表，用来去重。
        //    用 IdentityHashMap 而非 HashMap 是刻意的：Transformation 没有重写 equals/hashCode，
        //    这里要的正是"同一个对象实例"的语义。
        //    为什么必须去重：图里可能存在环（feedback edge / 迭代流），
        //    没有这张表就会无限递归；同时它保证被多个下游共享的节点只翻译一次。
        alreadyTransformed = new IdentityHashMap<>();

        // ⑤ 真正干活的一步。从用户登记过的每个 Transformation（"根"）出发，
        //    向 input 方向深度优先递归：transform() 内部先递归处理完所有上游，
        //    再为当前节点创建 StreamNode，并把上游节点连成本节点的入边。
        //    注意：只保证"根"能被走到就够了。中间节点（keyBy 产生的 PartitionTransformation、
        //    union 产生的 UnionTransformation 等）并不登记，靠递归被发现。
        for (Transformation<?> transformation : transformations) {
            transform(transformation);
        }

        // ⑥ 回填细粒度资源。⑤ 的递归过程中，每遇到一个携带 ResourceSpec 的 SlotSharingGroup，
        //    就会往 slotSharingGroupResources 里累积 (组名 -> ResourceProfile)，
        //    必须等整张图走完才能一次性设置。
        streamGraph.setSlotSharingGroupResource(slotSharingGroupResources);

        // ⑦ 细粒度资源 + BATCH 的死锁防护（FLINK-20865）：
        //    BATCH 模式下若用了细粒度资源又存在 PIPELINE 边，会有资源死锁风险，
        //    所以要么把全局边强制转成 BLOCKING（fine-grained.shuffle-mode.all-blocking=true），
        //    要么直接抛 IllegalConfigurationException 拒绝启动。
        setFineGrainedGlobalStreamExchangeMode(streamGraph);

        // ⑧ 非对齐 checkpoint（UC）可用性的修正——必须在图结构完全定型之后做。
        //    规则是"节点级一票否决"：只要某个 StreamNode 的**任意**一条入边的 partitioner
        //    不支持 UC，就把该节点的**所有**入边都标成不支持。
        //
        //    为什么是"连坐"而不是只关掉有问题的那一条边：
        //      下游 task 的 barrier 处理器是个状态机，收到本次 checkpoint 的第一个 barrier 时
        //      就决定了走对齐还是非对齐路径，并且用断言把这条规则写死了——
        //      AbstractAlignedBarrierHandlerState.barrierReceived() 开头就是
        //      checkState(!checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint())。
        //      如果同一节点的入边里既有支持 UC 的、又有不支持的，就可能先收到非对齐 barrier、
        //      再从另一条通道收到被 withUnalignedUnsupported() 降级过的强制对齐 barrier，
        //      直接踩断言。所以只能整节点统一。
        //
        //    哪些 partitioner 不支持（见 StreamPartitioner.isSupportsUnalignedCheckpoint()）：
        //      - pointwise（forward）边：本来就不承载通道状态；
        //      - broadcast 边；
        //      - 显式 disableUnalignedCheckpoints() 关掉的，例如 FLINK-36287 之后
        //        sink writer → committer 之间的内部通道（那条链只传 committable，
        //        barrier 一旦超车就会导致提交时机错乱）。
        //
        //    生效链路：StreamEdge.supportsUnalignedCheckpoints
        //      → StreamingJobGraphGenerator → NonChainedOutput
        //      → RecordWriterOu    tput.broadcastEvent()：若为 false，发送 barrier 时
        //        用 withUnalignedUnsupported() 降级为强制对齐，并取消 priority 事件语义。
        for (StreamNode node : streamGraph.getStreamNodes()) {
            if (node.getInEdges().stream()
                    .anyMatch(e -> !e.getPartitioner().isSupportsUnalignedCheckpoint())) {
                for (StreamEdge edge : node.getInEdges()) {
                    edge.setSupportsUnalignedCheckpoints(false);
                }
            }
        }

        // ⑨ 先接住返回值，再把内部可变状态清空并置 null——刻意的"一次性使用"设计：
        //    generate() 一旦返回，这个 generator 实例就作废了。这样做有两个目的：
        //      1) 及时切断对 builtStreamGraph 的额外引用（图并没有被拷贝，返回的就是同一个对象）；
        //      2) 防止内部类 TranslatorContext 在 generate() 返回后仍被 translator 误用：
        //         它的 transform() / getTransformedIds() 会直接访问这两个字段，
        //         置 null 之后能立刻 NPE 快速失败，而不是悄悄改坏一个已经返回的图。
        final StreamGraph builtStreamGraph = streamGraph;

        alreadyTransformed.clear();
        alreadyTransformed = null;
        streamGraph = null;

        // ⑩ 返回的是本地副本，不受上面置 null 的影响。
        return builtStreamGraph;
    }

    private void setDynamic(final StreamGraph graph) {
        Optional<JobManagerOptions.SchedulerType> schedulerTypeOptional =
                executionConfig.getSchedulerType();
        boolean dynamic =
                shouldExecuteInBatchMode
                        && schedulerTypeOptional.orElse(
                                        JobManagerOptions.SchedulerType.AdaptiveBatch)
                                == JobManagerOptions.SchedulerType.AdaptiveBatch;
        graph.setDynamic(dynamic);
    }

    private void configureStreamGraph(final StreamGraph graph) {
        checkNotNull(graph);

        graph.setTimeCharacteristic(timeCharacteristic);
        graph.setVertexDescriptionMode(configuration.get(PipelineOptions.VERTEX_DESCRIPTION_MODE));
        graph.setVertexNameIncludeIndexPrefix(
                configuration.get(PipelineOptions.VERTEX_NAME_INCLUDE_INDEX_PREFIX));
        graph.setAutoParallelismEnabled(
                configuration.get(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED));
        graph.setEnableCheckpointsAfterTasksFinish(
                configuration.get(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH));
        setDynamic(graph);

        if (shouldExecuteInBatchMode) {
            configureStreamGraphBatch(graph);
            configuration.set(ExecutionOptions.BUFFER_TIMEOUT_ENABLED, false);
        } else {
            configureStreamGraphStreaming(graph);
        }
    }

    private void configureStreamGraphBatch(final StreamGraph graph) {
        graph.setJobType(JobType.BATCH);
        graph.setJobName(deriveJobName(DEFAULT_BATCH_JOB_NAME));

        if (checkpointConfig.isCheckpointingEnabled()) {
            LOG.info(
                    "Disabled Checkpointing. Checkpointing is not supported and not needed when executing jobs in BATCH mode.");
            checkpointConfig.disableCheckpointing();
        }
        setBatchStateBackendAndTimerService(graph);

        graph.setGlobalStreamExchangeMode(deriveGlobalStreamExchangeModeBatch());
        graph.setAllVerticesInSameSlotSharingGroupByDefault(false);
    }

    private void configureStreamGraphStreaming(final StreamGraph graph) {
        graph.setJobType(JobType.STREAMING);
        graph.setJobName(deriveJobName(DEFAULT_STREAMING_JOB_NAME));

        graph.setStateBackend(stateBackend);
        graph.setCheckpointStorage(checkpointStorage);
        graph.setGlobalStreamExchangeMode(deriveGlobalStreamExchangeModeStreaming());
    }

    private String deriveJobName(String defaultJobName) {
        return configuration.getOptional(PipelineOptions.NAME).orElse(defaultJobName);
    }

    private GlobalStreamExchangeMode deriveGlobalStreamExchangeModeBatch() {
        final BatchShuffleMode shuffleMode = configuration.get(ExecutionOptions.BATCH_SHUFFLE_MODE);
        switch (shuffleMode) {
            case ALL_EXCHANGES_PIPELINED:
                return GlobalStreamExchangeMode.ALL_EDGES_PIPELINED;
            case ALL_EXCHANGES_BLOCKING:
                return GlobalStreamExchangeMode.ALL_EDGES_BLOCKING;
            case ALL_EXCHANGES_HYBRID_FULL:
                return GlobalStreamExchangeMode.ALL_EDGES_HYBRID_FULL;
            case ALL_EXCHANGES_HYBRID_SELECTIVE:
                return GlobalStreamExchangeMode.ALL_EDGES_HYBRID_SELECTIVE;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported shuffle mode '%s' in BATCH runtime mode.",
                                shuffleMode.toString()));
        }
    }

    private GlobalStreamExchangeMode deriveGlobalStreamExchangeModeStreaming() {
        if (checkpointConfig.isApproximateLocalRecoveryEnabled()) {
            checkApproximateLocalRecoveryCompatibility();
            return GlobalStreamExchangeMode.ALL_EDGES_PIPELINED_APPROXIMATE;
        }
        return GlobalStreamExchangeMode.ALL_EDGES_PIPELINED;
    }

    private void checkApproximateLocalRecoveryCompatibility() {
        checkState(
                !checkpointConfig.isUnalignedCheckpointsEnabled(),
                "Approximate Local Recovery and Unaligned Checkpoint can not be used together yet");
    }

    private void setBatchStateBackendAndTimerService(StreamGraph graph) {
        boolean useStateBackend = configuration.get(ExecutionOptions.USE_BATCH_STATE_BACKEND);
        boolean sortInputs = configuration.get(ExecutionOptions.SORT_INPUTS);
        checkState(
                !useStateBackend || sortInputs,
                "Batch state backend requires the sorted inputs to be enabled!");

        if (useStateBackend) {
            LOG.debug("Using BATCH execution state backend and timer service.");
            graph.setStateBackend(new BatchExecutionStateBackend());
            graph.getJobConfiguration().set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, false);
            graph.setCheckpointStorage(new BatchExecutionCheckpointStorage());
            graph.setTimerServiceProvider(BatchExecutionInternalTimeServiceManager::create);
        } else {
            graph.setStateBackend(stateBackend);
        }
    }

    private void setFineGrainedGlobalStreamExchangeMode(StreamGraph graph) {
        // There might be a resource deadlock when applying fine-grained resource management in
        // batch jobs with PIPELINE edges. Users need to trigger the
        // fine-grained.shuffle-mode.all-blocking to convert all edges to BLOCKING before we fix
        // that issue.
        if (shouldExecuteInBatchMode && graph.hasFineGrainedResource()) {
            if (configuration.get(ClusterOptions.FINE_GRAINED_SHUFFLE_MODE_ALL_BLOCKING)) {
                graph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);
            } else {
                throw new IllegalConfigurationException(
                        "At the moment, fine-grained resource management requires batch workloads to "
                                + "be executed with types of all edges being BLOCKING. To do that, you need to configure '"
                                + ClusterOptions.FINE_GRAINED_SHUFFLE_MODE_ALL_BLOCKING.key()
                                + "' to 'true'. Notice that this may affect the performance. See FLINK-20865 for more details.");
            }
        }
    }

    private boolean shouldExecuteInBatchMode() {
        final RuntimeExecutionMode configuredMode =
                configuration.get(ExecutionOptions.RUNTIME_MODE);

        final boolean existsUnboundedSource = existsUnboundedSource();

        checkState(
                configuredMode != RuntimeExecutionMode.BATCH || !existsUnboundedSource,
                "Detected an UNBOUNDED source with the '"
                        + ExecutionOptions.RUNTIME_MODE.key()
                        + "' set to 'BATCH'. "
                        + "This combination is not allowed, please set the '"
                        + ExecutionOptions.RUNTIME_MODE.key()
                        + "' to STREAMING or AUTOMATIC");

        if (checkNotNull(configuredMode) != RuntimeExecutionMode.AUTOMATIC) {
            return configuredMode == RuntimeExecutionMode.BATCH;
        }
        return !existsUnboundedSource;
    }

    private boolean existsUnboundedSource() {
        return transformations.stream()
                .anyMatch(
                        transformation ->
                                isUnboundedSource(transformation)
                                        || transformation.getTransitivePredecessors().stream()
                                                .anyMatch(this::isUnboundedSource));
    }

    private boolean isUnboundedSource(final Transformation<?> transformation) {
        checkNotNull(transformation);
        return transformation instanceof WithBoundedness
                && ((WithBoundedness) transformation).getBoundedness() != Boundedness.BOUNDED;
    }

    /**
     * Transforms one {@code Transformation}.
     *
     * <p>This checks whether we already transformed it and exits early in that case. If not it
     * delegates to one of the transformation specific methods.
     */
    private Collection<Integer> transform(Transformation<?> transform) {
        // ① 记忆化（去重）闸门，也是整个递归的第一道防线。
        //    返回值是"这个 Transformation 在 StreamGraph 中对应的 StreamNode id 集合"——
        //    之所以是集合而不是单个 id：union、feedback（迭代）这类结构一个逻辑节点
        //    会对应多个 StreamNode（比如迭代会额外生成一对 iteration source/sink）。
        //    为什么必须去重：图里可能有环（feedback edge / 迭代流），
        //    没有这道闸门递归就停不下来；同时它保证被多个下游共享的节点只翻译一次。
        if (alreadyTransformed.containsKey(transform)) {
            return alreadyTransformed.get(transform);
        }

        // ② 注意这里是字符串拼接而不是 {} 占位符。因为本方法是递归的，
        //    一次作业执行会把"翻译顺序"完整打印出来（从根一路向下到源），
        //    排查"图是怎么长出来的"时把这条日志开 DEBUG 非常有用。
        LOG.debug("Transforming " + transform);

        // ③ maxParallelism 的兜底赋值。
        //    算子自己没设（约定 <= 0 表示未设置）时，退回到 ExecutionConfig 上的作业级设置。
        //    语义上 maxParallelism 决定 key group 的数量和动态扩缩容的上限，
        //    **它还直接影响 savepoint 兼容性**——改了它旧 savepoint 就恢复不了，
        //    所以这里只是"补默认值"，而不是随意改写用户设定。
        //    注意这是就地对 Transformation 对象赋值（有副作用），后面所有读取该字段的地方都会看到新值。

        //todo 并行度相关配置
        if (transform.getMaxParallelism() <= 0) {

            // if the max parallelism hasn't been set, then first use the job wide max parallelism
            // from the ExecutionConfig.
            int globalMaxParallelismFromConfig = executionConfig.getMaxParallelism();
            if (globalMaxParallelismFromConfig > 0) {
                transform.setMaxParallelism(globalMaxParallelismFromConfig);
            }
        }

        // ④ 细粒度资源（fine-grained resource management）的登记。
        //    若该算子所属的 SlotSharingGroup 显式配了 ResourceSpec（不是 UNKNOWN），
        //    就把 (组名 -> ResourceProfile) 累积到 slotSharingGroupResources。
        //    这里用 compute() 做"存在即校验"：同一个 slot sharing group 名
        //    若被两个算子配了不同的 ResourceSpec，直接抛 IllegalArgumentException 拒绝启动。
        //    这些记录不会立刻生效，而是在 generate() 收尾时由
        //    streamGraph.setSlotSharingGroupResource(...) 统一回填。
        transform
                .getSlotSharingGroup()
                .ifPresent(
                        slotSharingGroup -> {
                            final ResourceSpec resourceSpec =
                                    SlotSharingGroupUtils.extractResourceSpec(slotSharingGroup);
                            if (!resourceSpec.equals(ResourceSpec.UNKNOWN)) {
                                slotSharingGroupResources.compute(
                                        slotSharingGroup.getName(),
                                        (name, profile) -> {
                                            if (profile == null) {
                                                return ResourceProfile.fromResourceSpec(
                                                        resourceSpec, MemorySize.ZERO);
                                            } else if (!ResourceProfile.fromResourceSpec(
                                                            resourceSpec, MemorySize.ZERO)
                                                    .equals(profile)) {
                                                throw new IllegalArgumentException(
                                                        "The slot sharing group "
                                                                + slotSharingGroup.getName()
                                                                + " has been configured with two different resource spec.");
                                            } else {
                                                return profile;
                                            }
                                        });
                            }
                        });

        // ⑤ 这行**故意丢弃返回值**，唯一目的是提前触发类型推断的异常：
        //    当输出类型是 MissingTypeInfo（泛型擦除导致 TypeExtractor 推不出来）时，
        //    getOutputType() 会立刻抛 InvalidTypesException 并提示用 returns(...) 加类型提示。
        //    如果不在翻译的入口处主动碰一下，这个错误会拖到很后面才暴露，堆栈难以定位。
        // call at least once to trigger exceptions about MissingTypeInfo
        transform.getOutputType();

        // ⑥ 按**精确类**（getClass()）查翻译器，注意不是 isAssignableFrom 的模糊匹配。
        //    所以新增一种 Transformation 子类时，必须显式注册到上面 translatorMap 的 static 块，
        //    否则会查不到而掉进 ⑦ 的 legacyTransform 分支，报 "Unknown transformation"。
        @SuppressWarnings("unchecked")
        final TransformationTranslator<?, Transformation<?>> translator =
                (TransformationTranslator<?, Transformation<?>>)
                        translatorMap.get(transform.getClass());

        // ⑦ 两条翻译路径的分派：
        //      translate()      —— 新式主路径，交给 TransformationTranslator，
        //                          其内部会先递归处理 inputs（getParentInputIds -> transform），
        //                          再创建自己的 StreamNode 并连边；
        //      legacyTransform()—— 兜底老路径，现在只剩 FeedbackTransformation、
        //                          CoFeedbackTransformation、SourceTransformationWrapper 三种，
        //                          外加 bufferTimeout / uid / userHash / 资源等公共属性的回填。
        //    无论走哪条，返回的都是"本节点在 StreamGraph 中对应的 StreamNode id 集合"。
        Collection<Integer> transformedIds;
        if (translator != null) {
            transformedIds = translate(translator, transform);
        } else {
            transformedIds = legacyTransform(transform);
        }

        // ⑧ 最后这个"再查一次"的守卫，是为迭代（iterate）准备的破环技巧：
        //    transformFeedback() 会在翻译 feedback edges **之前**就把 iterate 自己
        //    预先 put 进 alreadyTransformed（见 StreamGraphGenerator#transformFeedback 中
        //    "at the iterate to the already-seen-set" 那段），这样当递归顺着反馈边
        //    绕回到 iterate 本身时，inner 的那次 transform() 会命中 ① 直接返回，
        //    环就被切断了。
        //    因此走到这里时，值可能已经被内层递归填好了——必须避免用自己的旧值覆盖它。
        // need this check because the iterate transformation adds itself before
        // transforming the feedback edges
        if (!alreadyTransformed.containsKey(transform)) {
            alreadyTransformed.put(transform, transformedIds);
        }

        return transformedIds;
    }

    private Collection<Integer> legacyTransform(Transformation<?> transform) {
        Collection<Integer> transformedIds;
        if (transform instanceof FeedbackTransformation<?>) {
            transformedIds = transformFeedback((FeedbackTransformation<?>) transform);
        } else if (transform instanceof CoFeedbackTransformation<?>) {
            transformedIds = transformCoFeedback((CoFeedbackTransformation<?>) transform);
        } else if (transform instanceof SourceTransformationWrapper<?>) {
            transformedIds = transform(((SourceTransformationWrapper<?>) transform).getInput());
        } else {
            throw new IllegalStateException("Unknown transformation: " + transform);
        }

        if (transform.getBufferTimeout() >= 0) {
            streamGraph.setBufferTimeout(transform.getId(), transform.getBufferTimeout());
        } else {
            streamGraph.setBufferTimeout(transform.getId(), getBufferTimeout());
        }

        if (transform.getUid() != null) {
            streamGraph.setTransformationUID(transform.getId(), transform.getUid());
        }
        if (transform.getUserProvidedNodeHash() != null) {
            streamGraph.setTransformationUserHash(
                    transform.getId(), transform.getUserProvidedNodeHash());
        }

        if (!streamGraph.getExecutionConfig().hasAutoGeneratedUIDsEnabled()) {
            if (transform instanceof PhysicalTransformation
                    && transform.getUserProvidedNodeHash() == null
                    && transform.getUid() == null) {
                throw new IllegalStateException(
                        "Auto generated UIDs have been disabled "
                                + "but no UID or hash has been assigned to operator "
                                + transform.getName());
            }
        }

        if (transform.getMinResources() != null && transform.getPreferredResources() != null) {
            streamGraph.setResources(
                    transform.getId(),
                    transform.getMinResources(),
                    transform.getPreferredResources());
        }

        streamGraph.setManagedMemoryUseCaseWeights(
                transform.getId(),
                transform.getManagedMemoryOperatorScopeUseCaseWeights(),
                transform.getManagedMemorySlotScopeUseCases());

        return transformedIds;
    }

    private long getBufferTimeout() {
        return configuration.get(ExecutionOptions.BUFFER_TIMEOUT_ENABLED)
                ? configuration.get(ExecutionOptions.BUFFER_TIMEOUT).toMillis()
                : ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT;
    }

    /**
     * Transforms a {@code FeedbackTransformation}.
     *
     * <p>This will recursively transform the input and the feedback edges. We return the
     * concatenation of the input IDs and the feedback IDs so that downstream operations can be
     * wired to both.
     *
     * <p>This is responsible for creating the IterationSource and IterationSink which are used to
     * feed back the elements.
     */
    private <T> Collection<Integer> transformFeedback(FeedbackTransformation<T> iterate) {

        if (shouldExecuteInBatchMode) {
            throw new UnsupportedOperationException(
                    "Iterations are not supported in BATCH"
                            + " execution mode. If you want to execute such a pipeline, please set the "
                            + "'"
                            + ExecutionOptions.RUNTIME_MODE.key()
                            + "'="
                            + RuntimeExecutionMode.STREAMING.name());
        }

        if (iterate.getFeedbackEdges().size() <= 0) {
            throw new IllegalStateException(
                    "Iteration " + iterate + " does not have any feedback edges.");
        }

        List<Transformation<?>> inputs = iterate.getInputs();
        checkState(inputs.size() == 1);
        Transformation<?> input = inputs.get(0);

        List<Integer> resultIds = new ArrayList<>();

        // first transform the input stream(s) and store the result IDs
        Collection<Integer> inputIds = transform(input);
        resultIds.addAll(inputIds);

        // the recursive transform might have already transformed this
        if (alreadyTransformed.containsKey(iterate)) {
            return alreadyTransformed.get(iterate);
        }

        // create the fake iteration source/sink pair
        Tuple2<StreamNode, StreamNode> itSourceAndSink =
                streamGraph.createIterationSourceAndSink(
                        iterate.getId(),
                        getNewIterationNodeId(),
                        getNewIterationNodeId(),
                        iterate.getWaitTime(),
                        iterate.getParallelism(),
                        iterate.getMaxParallelism(),
                        iterate.getMinResources(),
                        iterate.getPreferredResources());

        StreamNode itSource = itSourceAndSink.f0;
        StreamNode itSink = itSourceAndSink.f1;

        // We set the proper serializers for the sink/source
        streamGraph.setSerializers(
                itSource.getId(),
                null,
                null,
                iterate.getOutputType().createSerializer(executionConfig.getSerializerConfig()));
        streamGraph.setSerializers(
                itSink.getId(),
                iterate.getOutputType().createSerializer(executionConfig.getSerializerConfig()),
                null,
                null);

        // also add the feedback source ID to the result IDs, so that downstream operators will
        // add both as input
        resultIds.add(itSource.getId());

        // at the iterate to the already-seen-set with the result IDs, so that we can transform
        // the feedback edges and let them stop when encountering the iterate node
        alreadyTransformed.put(iterate, resultIds);

        // so that we can determine the slot sharing group from all feedback edges
        List<Integer> allFeedbackIds = new ArrayList<>();

        for (Transformation<T> feedbackEdge : iterate.getFeedbackEdges()) {
            Collection<Integer> feedbackIds = transform(feedbackEdge);
            allFeedbackIds.addAll(feedbackIds);
            for (Integer feedbackId : feedbackIds) {
                streamGraph.addEdge(feedbackId, itSink.getId(), 0);
            }
        }

        String slotSharingGroup = determineSlotSharingGroup(null, allFeedbackIds);
        // slot sharing group of iteration node must exist
        if (slotSharingGroup == null) {
            slotSharingGroup = "SlotSharingGroup-" + iterate.getId();
        }

        itSink.setSlotSharingGroup(slotSharingGroup);
        itSource.setSlotSharingGroup(slotSharingGroup);

        return resultIds;
    }

    /**
     * Transforms a {@code CoFeedbackTransformation}.
     *
     * <p>This will only transform feedback edges, the result of this transform will be wired to the
     * second input of a Co-Transform. The original input is wired directly to the first input of
     * the downstream Co-Transform.
     *
     * <p>This is responsible for creating the IterationSource and IterationSink which are used to
     * feed back the elements.
     */
    private <F> Collection<Integer> transformCoFeedback(CoFeedbackTransformation<F> coIterate) {

        if (shouldExecuteInBatchMode) {
            throw new UnsupportedOperationException(
                    "Iterations are not supported in BATCH"
                            + " execution mode. If you want to execute such a pipeline, please set the "
                            + "'"
                            + ExecutionOptions.RUNTIME_MODE.key()
                            + "'="
                            + RuntimeExecutionMode.STREAMING.name());
        }

        // For Co-Iteration we don't need to transform the input and wire the input to the
        // head operator by returning the input IDs, the input is directly wired to the left
        // input of the co-operation. This transform only needs to return the ids of the feedback
        // edges, since they need to be wired to the second input of the co-operation.

        // create the fake iteration source/sink pair
        Tuple2<StreamNode, StreamNode> itSourceAndSink =
                streamGraph.createIterationSourceAndSink(
                        coIterate.getId(),
                        getNewIterationNodeId(),
                        getNewIterationNodeId(),
                        coIterate.getWaitTime(),
                        coIterate.getParallelism(),
                        coIterate.getMaxParallelism(),
                        coIterate.getMinResources(),
                        coIterate.getPreferredResources());

        StreamNode itSource = itSourceAndSink.f0;
        StreamNode itSink = itSourceAndSink.f1;

        // We set the proper serializers for the sink/source
        streamGraph.setSerializers(
                itSource.getId(),
                null,
                null,
                coIterate.getOutputType().createSerializer(executionConfig.getSerializerConfig()));
        streamGraph.setSerializers(
                itSink.getId(),
                coIterate.getOutputType().createSerializer(executionConfig.getSerializerConfig()),
                null,
                null);

        Collection<Integer> resultIds = Collections.singleton(itSource.getId());

        // at the iterate to the already-seen-set with the result IDs, so that we can transform
        // the feedback edges and let them stop when encountering the iterate node
        alreadyTransformed.put(coIterate, resultIds);

        // so that we can determine the slot sharing group from all feedback edges
        List<Integer> allFeedbackIds = new ArrayList<>();

        for (Transformation<F> feedbackEdge : coIterate.getFeedbackEdges()) {
            Collection<Integer> feedbackIds = transform(feedbackEdge);
            allFeedbackIds.addAll(feedbackIds);
            for (Integer feedbackId : feedbackIds) {
                streamGraph.addEdge(feedbackId, itSink.getId(), 0);
            }
        }

        String slotSharingGroup = determineSlotSharingGroup(null, allFeedbackIds);

        itSink.setSlotSharingGroup(slotSharingGroup);
        itSource.setSlotSharingGroup(slotSharingGroup);

        return Collections.singleton(itSource.getId());
    }

    private Collection<Integer> translate(
            final TransformationTranslator<?, Transformation<?>> translator,
            final Transformation<?> transform) {
        checkNotNull(translator);
        checkNotNull(transform);

        final List<Collection<Integer>> allInputIds = getParentInputIds(transform.getInputs());

        // the recursive call might have already transformed this
        if (alreadyTransformed.containsKey(transform)) {
            return alreadyTransformed.get(transform);
        }

        final String slotSharingGroup =
                determineSlotSharingGroup(
                        transform.getSlotSharingGroup().isPresent()
                                ? transform.getSlotSharingGroup().get().getName()
                                : null,
                        allInputIds.stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()));

        final TransformationTranslator.Context context =
                new ContextImpl(this, streamGraph, slotSharingGroup, configuration);

        return shouldExecuteInBatchMode
                ? translator.translateForBatch(transform, context)
                : translator.translateForStreaming(transform, context);
    }

    /**
     * Returns a list of lists containing the ids of the nodes in the transformation graph that
     * correspond to the provided transformations. Each transformation may have multiple nodes.
     *
     * <p>Parent transformations will be translated if they are not already translated.
     *
     * @param parentTransformations the transformations whose node ids to return.
     * @return the nodeIds per transformation or an empty list if the {@code parentTransformations}
     *     are empty.
     */
    private List<Collection<Integer>> getParentInputIds(
            @Nullable final Collection<Transformation<?>> parentTransformations) {
        final List<Collection<Integer>> allInputIds = new ArrayList<>();
        if (parentTransformations == null) {
            return allInputIds;
        }

        for (Transformation<?> transformation : parentTransformations) {
            allInputIds.add(transform(transformation));
        }
        return allInputIds;
    }

    /**
     * Determines the slot sharing group for an operation based on the slot sharing group set by the
     * user and the slot sharing groups of the inputs.
     *
     * <p>If the user specifies a group name, this is taken as is. If nothing is specified and the
     * input operations all have the same group name then this name is taken. Otherwise the default
     * group is chosen.
     *
     * @param specifiedGroup The group specified by the user.
     * @param inputIds The IDs of the input operations.
     */
    private String determineSlotSharingGroup(String specifiedGroup, Collection<Integer> inputIds) {
        if (specifiedGroup != null) {
            return specifiedGroup;
        } else {
            String inputGroup = null;
            for (int id : inputIds) {
                String inputGroupCandidate = streamGraph.getSlotSharingGroup(id);
                if (inputGroup == null) {
                    inputGroup = inputGroupCandidate;
                } else if (!inputGroup.equals(inputGroupCandidate)) {
                    return DEFAULT_SLOT_SHARING_GROUP;
                }
            }
            return inputGroup == null ? DEFAULT_SLOT_SHARING_GROUP : inputGroup;
        }
    }

    private static class ContextImpl implements TransformationTranslator.Context {

        private final StreamGraphGenerator streamGraphGenerator;

        private final StreamGraph streamGraph;

        private final String slotSharingGroup;

        private final ReadableConfig config;

        public ContextImpl(
                final StreamGraphGenerator streamGraphGenerator,
                final StreamGraph streamGraph,
                final String slotSharingGroup,
                final ReadableConfig config) {
            this.streamGraphGenerator = checkNotNull(streamGraphGenerator);
            this.streamGraph = checkNotNull(streamGraph);
            this.slotSharingGroup = checkNotNull(slotSharingGroup);
            this.config = checkNotNull(config);
        }

        @Override
        public StreamGraph getStreamGraph() {
            return streamGraph;
        }

        @Override
        public Collection<Integer> getStreamNodeIds(final Transformation<?> transformation) {
            checkNotNull(transformation);
            final Collection<Integer> ids =
                    streamGraphGenerator.alreadyTransformed.get(transformation);
            checkState(
                    ids != null,
                    "Parent transformation \"" + transformation + "\" has not been transformed.");
            return ids;
        }

        @Override
        public String getSlotSharingGroup() {
            return slotSharingGroup;
        }

        @Override
        public long getDefaultBufferTimeout() {
            return streamGraphGenerator.getBufferTimeout();
        }

        @Override
        public ReadableConfig getGraphGeneratorConfig() {
            return config;
        }

        @Override
        public Collection<Integer> transform(Transformation<?> transformation) {
            return streamGraphGenerator.transform(transformation);
        }
    }
}
