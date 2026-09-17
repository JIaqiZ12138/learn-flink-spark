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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

// PipelinedRegion 粒度的调度策略，Flink 1.20 默认使用的调度策略。
// 调用链：SchedulerBase.startScheduling() → 本类 startScheduling() → maybeScheduleRegions()
//   → scheduleRegion() → SchedulerOperations.allocateSlotsAndDeploy()（交给 SlotAllocator 分配槽位）。
// ★ PipelinedRegion（流水线区域）= 执行图中由"流水线边"连成的一组 ExecutionVertex。
//   划分规则：消费端对某个 ResultPartition 的消费方式为 canBePipelinedConsumed()（即 PIPELINED /
//   PIPELINED_BOUNDED）时，这条边不切断区域；只有 blocking 边才是区域边界。区域本身在
//   DefaultExecutionTopology 构造时由 SchedulingPipelinedRegionComputeUtil 算好，本类只消费结果并建索引。
// ★ 为什么区域必须整体调度：流水线传输要求下游消费时上游已经在运行（数据边产生边消费、没有落盘屏障），
//   只调度区域的一部分会让下游永远等不到数据，所以"区域"才是最小调度单位；区域之间则可独立调度。
/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of pipelined regions.
 */
public class PipelinedRegionSchedulingStrategy implements SchedulingStrategy {

    private final SchedulerOperations schedulerOperations;

    private final SchedulingTopology schedulingTopology;

    /** External consumer regions of each ConsumedPartitionGroup. */
    private final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
            partitionGroupConsumerRegions = new IdentityHashMap<>();

    private final Map<SchedulingPipelinedRegion, List<ExecutionVertexID>> regionVerticesSorted =
            new IdentityHashMap<>();

    /** All produced partition groups of one schedulingPipelinedRegion. */
    private final Map<SchedulingPipelinedRegion, Set<ConsumedPartitionGroup>>
            producedPartitionGroupsOfRegion = new IdentityHashMap<>();

    /** The ConsumedPartitionGroups which are produced by multiple regions. */
    private final Set<ConsumedPartitionGroup> crossRegionConsumedPartitionGroups =
            Collections.newSetFromMap(new IdentityHashMap<>());

    private final Set<SchedulingPipelinedRegion> scheduledRegions =
            Collections.newSetFromMap(new IdentityHashMap<>());

    public PipelinedRegionSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);

        init();
    }

    // 构造函数里就调用的"重活"：一次性把后面判断可调度性需要的三张索引表建好。
    private void init() {

        initCrossRegionConsumedPartitionGroups();

        initPartitionGroupConsumerRegions();

        initProducedPartitionGroupsOfRegion();

        // regionVerticesSorted：区域 → 区域内顶点列表；scheduleRegion 部署时按这个列表整体下发。
        for (SchedulingExecutionVertex vertex : schedulingTopology.getVertices()) {
            final SchedulingPipelinedRegion region =
                    schedulingTopology.getPipelinedRegionOfVertex(vertex.getId());
            regionVerticesSorted
                    .computeIfAbsent(region, r -> new ArrayList<>())
                    .add(vertex.getId());
        }
    }

    // 索引 1：区域 → 该区域产出的所有 ConsumedPartitionGroup，用于沿流水线边向后扩散找下游区域。
    private void initProducedPartitionGroupsOfRegion() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            Set<ConsumedPartitionGroup> producedPartitionGroupsSetOfRegion = new HashSet<>();
            for (SchedulingExecutionVertex executionVertex : region.getVertices()) {
                producedPartitionGroupsSetOfRegion.addAll(
                        IterableUtils.toStream(executionVertex.getProducedResults())
                                .flatMap(
                                        partition ->
                                                partition.getConsumedPartitionGroups().stream())
                                .collect(Collectors.toSet()));
            }
            producedPartitionGroupsOfRegion.put(region, producedPartitionGroupsSetOfRegion);
        }
    }

    // 索引 2：找出"跨区域"的消费组 —— 一个 ConsumedPartitionGroup 的产出分区来自多个不同区域。
    // ★ 这类分组是易错点：既不能当纯区域内依赖，也不能简单当外部依赖，后面判断可调度性时要单独走一条分支。
    private void initCrossRegionConsumedPartitionGroups() {
        final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
                producerRegionsByConsumedPartitionGroup = new IdentityHashMap<>();

        for (SchedulingPipelinedRegion pipelinedRegion :
                schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                producerRegionsByConsumedPartitionGroup.computeIfAbsent(
                        consumedPartitionGroup, this::getProducerRegionsForConsumedPartitionGroup);
            }
        }

        for (SchedulingPipelinedRegion pipelinedRegion :
                schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                final Set<SchedulingPipelinedRegion> producerRegions =
                        producerRegionsByConsumedPartitionGroup.get(consumedPartitionGroup);
                if (producerRegions.size() > 1 && producerRegions.contains(pipelinedRegion)) {
                    crossRegionConsumedPartitionGroups.add(consumedPartitionGroup);
                }
            }
        }
    }

    private Set<SchedulingPipelinedRegion> getProducerRegionsForConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup) {
        final Set<SchedulingPipelinedRegion> producerRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            producerRegions.add(getProducerRegion(partitionId));
        }
        return producerRegions;
    }

    private SchedulingPipelinedRegion getProducerRegion(IntermediateResultPartitionID partitionId) {
        return schedulingTopology.getPipelinedRegionOfVertex(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    private void initPartitionGroupConsumerRegions() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    region.getAllNonPipelinedConsumedPartitionGroups()) {
                if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                        || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                    partitionGroupConsumerRegions
                            .computeIfAbsent(consumedPartitionGroup, group -> new HashSet<>())
                            .add(region);
                }
            }
        }
    }

    // 某顶点跑完后，找出因它的 blocking 输出而被解锁的下游区域。
    // ★ 只有 blocking 分区（不能流水线消费）才在这里解锁；流水线下游在调度本区域时已经一起调度过了。
    private Set<SchedulingPipelinedRegion> getBlockingDownstreamRegionsOfVertex(
            SchedulingExecutionVertex executionVertex) {
        return IterableUtils.toStream(executionVertex.getProducedResults())
                .filter(partition -> !partition.getResultType().canBePipelinedConsumed())
                .flatMap(partition -> partition.getConsumedPartitionGroups().stream())
                .filter(
                        group ->
                                crossRegionConsumedPartitionGroups.contains(group)
                                        || group.areAllPartitionsFinished())
                .flatMap(
                        partitionGroup ->
                                partitionGroupConsumerRegions
                                        .getOrDefault(partitionGroup, Collections.emptySet())
                                        .stream())
                .collect(Collectors.toSet());
    }

    @Override
    // 【入口】作业启动时的第一次调度：只挑出"源区域"开始，其余区域等前面的区域跑完再被解锁。
    public void startScheduling() {
        final Set<SchedulingPipelinedRegion> sourceRegions =
                IterableUtils.toStream(schedulingTopology.getAllPipelinedRegions())
                        .filter(this::isSourceRegion)
                        .collect(Collectors.toSet());
        // 从源区域出发，顺着流水线边一层层向后推，直到再也推不出新的可调度区域。
        maybeScheduleRegions(sourceRegions);
    }

    // 源区域 = 没有任何"跨区域"或"外部"的 blocking 输入分组，即它可以直接开跑，不需要等别的区域。
    private boolean isSourceRegion(SchedulingPipelinedRegion region) {
        for (ConsumedPartitionGroup consumedPartitionGroup :
                region.getAllNonPipelinedConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                    || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                return false;
            }
        }
        return true;
    }

    // 【失败重调度】区域失败后 ExecutionGraph 会把要重启的顶点交回来（region 是最小重启单位）。
    // ★ 必须先把区域从 scheduledRegions 里摘掉，否则 maybeScheduleRegions 会认为它已调度过而跳过，永远不重启。
    @Override
    public void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
        final Set<SchedulingPipelinedRegion> regionsToRestart =
                verticesToRestart.stream()
                        .map(schedulingTopology::getPipelinedRegionOfVertex)
                        .collect(Collectors.toSet());
        scheduledRegions.removeAll(regionsToRestart);
        maybeScheduleRegions(regionsToRestart);
    }

    @Override
    public void onExecutionStateChange(
            final ExecutionVertexID executionVertexId, final ExecutionState executionState) {
        // 只有 FINISHED 才可能解锁下游 blocking 区域；失败/取消的去向是 restartTasks（走失败重调度路径）。
        if (executionState == ExecutionState.FINISHED) {
            maybeScheduleRegions(
                    getBlockingDownstreamRegionsOfVertex(
                            schedulingTopology.getVertex(executionVertexId)));
        }
    }

    // 空实现：流水线分区的"可消费"事件对本策略没有意义 —— 区域内的流水线下游在同一批里已经一起调度了。
    @Override
    public void onPartitionConsumable(final IntermediateResultPartitionID resultPartitionId) {}

    // ★ 核心循环：反复从候选集里挑出可调度的区域，并把它们的（流水线）下游区域作为下一批候选，
    //   直到再也推不出新区域 —— 每一轮都是一次"沿流水线边扩散"。最后按拓扑序部署，保证部署顺序与依赖一致。
    private void maybeScheduleRegions(final Set<SchedulingPipelinedRegion> regions) {
        final Set<SchedulingPipelinedRegion> regionsToSchedule = new HashSet<>();
        Set<SchedulingPipelinedRegion> nextRegions = regions;
        while (!nextRegions.isEmpty()) {
            nextRegions = addSchedulableAndGetNextRegions(nextRegions, regionsToSchedule);
        }
        // schedule regions in topological order.
        SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(
                        schedulingTopology, regionsToSchedule)
                .forEach(this::scheduleRegion);
    }

    // 单轮扩散：先判断候选区域是否可调度；可调度的区域再通过"它产出的流水线分组"找到消费方区域，作为下一轮候选。
    private Set<SchedulingPipelinedRegion> addSchedulableAndGetNextRegions(
            Set<SchedulingPipelinedRegion> currentRegions,
            Set<SchedulingPipelinedRegion> regionsToSchedule) {
        Set<SchedulingPipelinedRegion> nextRegions = new HashSet<>();
        // cache consumedPartitionGroup's consumable status to avoid compute repeatedly.
        // 缓存分组的可消费状态，避免同一个分组在多个区域之间被反复计算（大图上这里是热点）。
        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new HashMap<>();
        final Set<ConsumedPartitionGroup> visitedConsumedPartitionGroups = new HashSet<>();

        for (SchedulingPipelinedRegion currentRegion : currentRegions) {
            if (isRegionSchedulable(currentRegion, consumableStatusCache, regionsToSchedule)) {
                regionsToSchedule.add(currentRegion);
                producedPartitionGroupsOfRegion
                        .getOrDefault(currentRegion, Collections.emptySet())
                        .forEach(
                                (producedPartitionGroup) -> {
                                    if (!producedPartitionGroup
                                            .getResultPartitionType()
                                            .canBePipelinedConsumed()) {
                                        return;
                                    }
                                    // If this group has been visited, there is no need
                                    // to repeat the determination.
                                    if (visitedConsumedPartitionGroups.contains(
                                            producedPartitionGroup)) {
                                        return;
                                    }
                                    visitedConsumedPartitionGroups.add(producedPartitionGroup);
                                    nextRegions.addAll(
                                            partitionGroupConsumerRegions.getOrDefault(
                                                    producedPartitionGroup,
                                                    Collections.emptySet()));
                                });
            }
        }
        return nextRegions;
    }

    private boolean isRegionSchedulable(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        return !regionToSchedule.contains(region)
                && !scheduledRegions.contains(region)
                && areRegionInputsAllConsumable(region, consumableStatusCache, regionToSchedule);
    }

    // 【跨界点】真正下发部署：allocateSlotsAndDeploy 会走 ExecutionSlotAllocator 分配槽位，最终部署到 TaskExecutor。
    // ★ 先 checkState 保证区域内顶点都在 CREATED：重复调度同一区域是 bug，这里用断言兜住。
    private void scheduleRegion(final SchedulingPipelinedRegion region) {
        checkState(
                areRegionVerticesAllInCreatedState(region),
                "BUG: trying to schedule a region which is not in CREATED state");
        scheduledRegions.add(region);
        schedulerOperations.allocateSlotsAndDeploy(regionVerticesSorted.get(region));
    }

    private boolean areRegionInputsAllConsumable(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        for (ConsumedPartitionGroup consumedPartitionGroup :
                region.getAllNonPipelinedConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)) {
                if (!isDownstreamOfCrossRegionConsumedPartitionSchedulable(
                        consumedPartitionGroup, region, regionToSchedule)) {
                    return false;
                }
            } else if (isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                if (!consumableStatusCache.computeIfAbsent(
                        consumedPartitionGroup,
                        (group) ->
                                isDownstreamConsumedPartitionGroupSchedulable(
                                        group, regionToSchedule))) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isDownstreamConsumedPartitionGroupSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                SchedulingPipelinedRegion producerRegion = getProducerRegion(partitionId);
                if (!scheduledRegions.contains(producerRegion)
                        && !regionToSchedule.contains(producerRegion)) {
                    return false;
                }
            }
        } else {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (schedulingTopology.getResultPartition(partitionId).getState()
                        != ResultPartitionState.ALL_DATA_PRODUCED) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isDownstreamOfCrossRegionConsumedPartitionSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup,
            final SchedulingPipelinedRegion pipelinedRegion,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (isExternalConsumedPartition(partitionId, pipelinedRegion)) {
                    SchedulingPipelinedRegion producerRegion = getProducerRegion(partitionId);
                    if (!regionToSchedule.contains(producerRegion)
                            && !scheduledRegions.contains(producerRegion)) {
                        return false;
                    }
                }
            }
        } else {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (isExternalConsumedPartition(partitionId, pipelinedRegion)
                        && schedulingTopology.getResultPartition(partitionId).getState()
                                != ResultPartitionState.ALL_DATA_PRODUCED) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean areRegionVerticesAllInCreatedState(final SchedulingPipelinedRegion region) {
        for (SchedulingExecutionVertex vertex : region.getVertices()) {
            if (vertex.getState() != ExecutionState.CREATED) {
                return false;
            }
        }
        return true;
    }

    private boolean isExternalConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup,
            SchedulingPipelinedRegion pipelinedRegion) {

        return isExternalConsumedPartition(consumedPartitionGroup.getFirst(), pipelinedRegion);
    }

    private boolean isExternalConsumedPartition(
            IntermediateResultPartitionID partitionId, SchedulingPipelinedRegion pipelinedRegion) {
        return !pipelinedRegion.contains(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    @VisibleForTesting
    Set<ConsumedPartitionGroup> getCrossRegionConsumedPartitionGroups() {
        return Collections.unmodifiableSet(crossRegionConsumedPartitionGroups);
    }

    /** The factory for creating {@link PipelinedRegionSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);
        }
    }
}
