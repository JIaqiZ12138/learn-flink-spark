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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// JobMaster 侧的槽位（slot）账本，SlotPool 的声明式实现 —— 对应文档链路第 3 步。
// 调用链：ExecutionSlotAllocator → PhysicalSlotProvider → SlotPool.requestNewAllocatedSlot()
//   → 本类 internalRequestNewAllocatedSlot() → DeclarativeSlotPool.increaseResourceRequirementsBy()
//   → DeclarativeSlotPoolService.declareResourceRequirements()（连接管理器合并后下发）
//   → ResourceManagerGateway.declareRequiredResources()【跨界点：JobMaster → ResourceManager】
//   → ResourceManager 侧 declareRequiredResources() → FineGrainedSlotManager（见链路第 5 步）
// ★ "声明式"三个字的含义：本类不逐个向 ResourceManager 伸手要槽位，只声明"本作业总共需要 N 个某规格的槽位"；
//   等 TM 上出现空闲槽位时由 TaskExecutor 主动 offerSlots() 推过来（RM/TM → JM【推】，不是 JM 拉），
//   再由 RequestSlotMatchingStrategy 把"需求"与"实际槽位"撮合。这样 RM 能在全局视角下做更优的分配与复用。
/** {@link SlotPool} implementation which uses the {@link DeclarativeSlotPool} to allocate slots. */
public class DeclarativeSlotPoolBridge extends DeclarativeSlotPoolService implements SlotPool {

    // pendingRequests：还没等到槽位的请求（含 batch 请求），是"需求"的实体；fulfilledRequests：已满足的请求
    // → 实际拿到的 AllocationID，释放槽位时靠它反查。两者互斥，同一个 slotRequestId 只会出现在其中一边。
    private final Map<SlotRequestId, PendingRequest> pendingRequests;
    private final Map<SlotRequestId, AllocationID> fulfilledRequests;
    private final Time idleSlotTimeout;

    private final RequestSlotMatchingStrategy requestSlotMatchingStrategy;

    @Nullable private ComponentMainThreadExecutor componentMainThreadExecutor;

    private final Time batchSlotTimeout;
    private boolean isBatchSlotRequestTimeoutCheckDisabled;

    private boolean isJobRestarting = false;

    public DeclarativeSlotPoolBridge(
            JobID jobId,
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
            Clock clock,
            Time rpcTimeout,
            Time idleSlotTimeout,
            Time batchSlotTimeout,
            RequestSlotMatchingStrategy requestSlotMatchingStrategy) {
        super(jobId, declarativeSlotPoolFactory, clock, idleSlotTimeout, rpcTimeout);

        this.idleSlotTimeout = idleSlotTimeout;
        this.batchSlotTimeout = Preconditions.checkNotNull(batchSlotTimeout);

        log.debug(
                "Using the request slot matching strategy: {}",
                requestSlotMatchingStrategy.getClass().getSimpleName());
        this.requestSlotMatchingStrategy = requestSlotMatchingStrategy;

        this.isBatchSlotRequestTimeoutCheckDisabled = false;

        this.pendingRequests = new LinkedHashMap<>();
        this.fulfilledRequests = new HashMap<>();
    }

    @Override
    public <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass())) {
            return Optional.of(clazz.cast(this));
        }

        return Optional.empty();
    }

    @Override
    // 注册"有新槽位到达"的监听：★ 槽位是被推过来的，本类只能被动等在回调里匹配，不会主动去要。
    // 另外挂两个会自我续期的定时任务：idle 槽位回收、batch 请求超时判定；它们只负责超时，不参与匹配。
    protected void onStart(ComponentMainThreadExecutor componentMainThreadExecutor) {
        this.componentMainThreadExecutor = componentMainThreadExecutor;

        getDeclarativeSlotPool().registerNewSlotsListener(this::newSlotsAreAvailable);

        componentMainThreadExecutor.schedule(
                this::checkIdleSlotTimeout,
                idleSlotTimeout.toMilliseconds(),
                TimeUnit.MILLISECONDS);
        componentMainThreadExecutor.schedule(
                this::checkBatchSlotTimeout,
                batchSlotTimeout.toMilliseconds(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    protected void onClose() {
        final FlinkException cause = new FlinkException("Closing slot pool");
        cancelPendingRequests(request -> true, cause);
    }

    /**
     * To set whether the underlying is currently restarting or not. In the former case the slot
     * pool bridge will accept all incoming slot offers.
     *
     * @param isJobRestarting whether this is restarting or not
     */
    @Override
    public void setIsJobRestarting(boolean isJobRestarting) {
        this.isJobRestarting = isJobRestarting;
    }

    @Override
    // TM 注册到 RM 之后，由 RM 指挥 TM 把空闲槽位 offer 给作业；★ 方向是 RM/TM → JM【推】，JM 从不去拉。
    // 返回值是"被接受的 offer"，没被返回的槽位即隐含拒绝（RM 会转手给别人或回收）。
    // isJobRestarting 分支（见 setIsJobRestarting）：作业重启期间照单全收（registerSlots 不挑规格，
    // 内部按 ResourceProfile.ANY 记账），保证恢复时能尽快拿到槽位；平时才走 offerSlots 做规格匹配。
    public Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers) {
        assertHasBeenStarted();

        if (!isTaskManagerRegistered(taskManagerLocation.getResourceID())) {
            log.debug(
                    "Ignoring offered slots from unknown task manager {}.",
                    taskManagerLocation.getResourceID());
            return Collections.emptyList();
        }

        if (isJobRestarting) {
            return getDeclarativeSlotPool()
                    .registerSlots(
                            offers,
                            taskManagerLocation,
                            taskManagerGateway,
                            getRelativeTimeMillis());

        } else {
            return getDeclarativeSlotPool()
                    .offerSlots(
                            offers,
                            taskManagerLocation,
                            taskManagerGateway,
                            getRelativeTimeMillis());
        }
    }

    // 批量失败待处理请求，并把它们占用的资源需求【减回去】—— 否则 RM 会一直以为作业还需要这些槽位。
    // ★ 先拷贝一份再遍历：失败一个请求可能连带触发新的请求（例如 failover 重新申请），直接遍历会并发修改。
    private void cancelPendingRequests(
            Predicate<PendingRequest> requestPredicate, FlinkException cancelCause) {

        ResourceCounter decreasedResourceRequirements = ResourceCounter.empty();

        // need a copy since failing a request could trigger another request to be issued
        final Iterable<PendingRequest> pendingRequestsToFail =
                new ArrayList<>(pendingRequests.values());
        pendingRequests.clear();

        for (PendingRequest pendingRequest : pendingRequestsToFail) {
            if (requestPredicate.test(pendingRequest)) {
                pendingRequest.failRequest(cancelCause);
                decreasedResourceRequirements =
                        decreasedResourceRequirements.add(pendingRequest.getResourceProfile(), 1);
            } else {
                pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);
            }
        }

        getDeclarativeSlotPool().decreaseResourceRequirementsBy(decreasedResourceRequirements);
    }

    @Override
    protected void onReleaseTaskManager(ResourceCounter previouslyFulfilledRequirement) {
        getDeclarativeSlotPool().decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
    }

    @VisibleForTesting
    // 新槽位到达回调：把新槽位与待处理请求撮合（配对规则由 RequestSlotMatchingStrategy 决定）。
    // ★ 必须分两轮：先全部 reserveFreeSlot 预留，再统一 fulfill。若边预留边 fulfill，fulfill 会同步唤起
    //   调度器发起新请求，而那个新请求可能抢走本轮还没预留的新槽位（源码里明确点出的坑）。
    void newSlotsAreAvailable(Collection<? extends PhysicalSlot> newSlots) {
        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                requestSlotMatchingStrategy.matchRequestsAndSlots(
                        newSlots, pendingRequests.values());

        for (RequestSlotMatchingStrategy.RequestSlotMatch match : requestSlotMatches) {
            final PendingRequest pendingRequest = match.getPendingRequest();
            final PhysicalSlot slot = match.getSlot();

            log.debug("Matched pending request {} with slot {}.", pendingRequest, slot);

            Preconditions.checkNotNull(
                    pendingRequests.remove(pendingRequest.getSlotRequestId()),
                    "Cannot fulfill a non existing pending slot request.");

            reserveFreeSlot(
                    pendingRequest.getSlotRequestId(),
                    slot.getAllocationId(),
                    pendingRequest.getResourceProfile());
        }

        // we have to first reserve all matching slots before fulfilling the requests
        // otherwise it can happen that the scheduler reserves one of the new slots
        // for a request which has been triggered by fulfilling a pending request
        for (RequestSlotMatchingStrategy.RequestSlotMatch requestSlotMatch : requestSlotMatches) {
            final PendingRequest pendingRequest = requestSlotMatch.getPendingRequest();
            final PhysicalSlot slot = requestSlotMatch.getSlot();

            Preconditions.checkState(
                    pendingRequest.fulfill(slot), "Pending requests must be fulfillable.");
        }
    }

    // 预留成功后登记 slotRequestId → allocationId 的映射，后续 releaseSlot 全靠它才能找回槽位。
    private void reserveFreeSlot(
            SlotRequestId slotRequestId,
            AllocationID allocationId,
            ResourceProfile resourceProfile) {
        log.debug("Reserve slot {} for slot request id {}", allocationId, slotRequestId);
        getDeclarativeSlotPool().reserveFreeSlot(allocationId, resourceProfile);
        fulfilledRequests.put(slotRequestId, allocationId);
    }

    @Override
    // 复用"池中已有槽位"（而不是新建）的路径：调用方连 allocationID 都已经指定好了。
    // ★ 用 allocationID 反查一个空闲槽位并预留（reserveFreeSlotForResource），不需要任何 RPC 往返。
    public Optional<PhysicalSlot> allocateAvailableSlot(
            @Nonnull SlotRequestId slotRequestId,
            @Nonnull AllocationID allocationID,
            @Nonnull ResourceProfile requirementProfile) {
        assertRunningInMainThread();
        Preconditions.checkNotNull(requirementProfile, "The requiredSlotProfile must not be null.");

        log.debug(
                "Reserving free slot {} for slot request id {} and profile {}.",
                allocationID,
                slotRequestId,
                requirementProfile);

        return Optional.of(
                reserveFreeSlotForResource(slotRequestId, allocationID, requirementProfile));
    }

    // ★ 先 increaseResourceRequirementsBy 再 reserveFreeSlot，顺序有讲究：先把这份规格声明出去，
    //   再去预留池中已存在的槽位；若该槽位当初是按另一种规格被接受的，DeclarativeSlotPool 内部会
    //   adjustRequirements() 修正账本并更新规格映射，下次释放时才能对上账。
    private PhysicalSlot reserveFreeSlotForResource(
            SlotRequestId slotRequestId,
            AllocationID allocationId,
            ResourceProfile requiredSlotProfile) {
        getDeclarativeSlotPool()
                .increaseResourceRequirementsBy(
                        ResourceCounter.withResource(requiredSlotProfile, 1));
        final PhysicalSlot physicalSlot =
                getDeclarativeSlotPool().reserveFreeSlot(allocationId, requiredSlotProfile);
        fulfilledRequests.put(slotRequestId, allocationId);

        return physicalSlot;
    }

    @Override
    @Nonnull
    public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            @Nonnull SlotRequestId slotRequestId,
            @Nonnull ResourceProfile resourceProfile,
            @Nonnull Collection<AllocationID> preferredAllocations,
            @Nullable Time timeout) {
        assertRunningInMainThread();

        log.debug(
                "Request new allocated slot with slot request id {} and resource profile {}",
                slotRequestId,
                resourceProfile);

        final PendingRequest pendingRequest =
                PendingRequest.createNormalRequest(
                        slotRequestId, resourceProfile, preferredAllocations);

        return internalRequestNewSlot(pendingRequest, timeout);
    }

    @Override
    @Nonnull
    public CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
            @Nonnull SlotRequestId slotRequestId,
            @Nonnull ResourceProfile resourceProfile,
            @Nonnull Collection<AllocationID> preferredAllocations) {
        assertRunningInMainThread();

        log.debug(
                "Request new allocated batch slot with slot request id {} and resource profile {}",
                slotRequestId,
                resourceProfile);

        final PendingRequest pendingRequest =
                PendingRequest.createBatchRequest(
                        slotRequestId, resourceProfile, preferredAllocations);

        return internalRequestNewSlot(pendingRequest, null);
    }

    // 统一入口：把需求登记进账本，然后给 future 套一个超时（超时就当作释放该请求，见 timeoutPendingSlotRequest）。
    // ★ batch 请求传进来的 timeout 是 null，即不超时：批作业允许需求分波满足，不能因为暂时凑不齐就失败。
    private CompletableFuture<PhysicalSlot> internalRequestNewSlot(
            PendingRequest pendingRequest, @Nullable Time timeout) {
        internalRequestNewAllocatedSlot(pendingRequest);

        if (timeout == null) {
            return pendingRequest.getSlotFuture();
        } else {
            return FutureUtils.orTimeout(
                            pendingRequest.getSlotFuture(),
                            timeout.toMilliseconds(),
                            TimeUnit.MILLISECONDS,
                            componentMainThreadExecutor,
                            String.format(
                                    "Pending slot request %s timed out after %d ms.",
                                    pendingRequest.getSlotRequestId(), timeout.toMilliseconds()))
                    .whenComplete(
                            (physicalSlot, throwable) -> {
                                if (throwable instanceof TimeoutException) {
                                    timeoutPendingSlotRequest(pendingRequest.getSlotRequestId());
                                }
                            });
        }
    }

    private void timeoutPendingSlotRequest(SlotRequestId slotRequestId) {
        releaseSlot(
                slotRequestId,
                new TimeoutException("Pending slot request timed out in slot pool."));
    }

    // ★ 声明式的核心就在这里：只做两件事 —— 记下 PendingRequest、把资源需求 +1。
    //   全程没有"向谁要槽位"的同步 RPC，真正的下发由账本变化的回调异步完成（见类注释里的调用链）。
    private void internalRequestNewAllocatedSlot(PendingRequest pendingRequest) {
        pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);

        getDeclarativeSlotPool()
                .increaseResourceRequirementsBy(
                        ResourceCounter.withResource(pendingRequest.getResourceProfile(), 1));
    }

    @Override
    protected void onFailAllocation(ResourceCounter previouslyFulfilledRequirements) {
        getDeclarativeSlotPool().decreaseResourceRequirementsBy(previouslyFulfilledRequirements);
    }

    @Override
    // 释放槽位要区分两种状态：请求还没被满足（从 pendingRequests 摘掉并把需求减回去），
    // 或者已经被满足（freeReservedSlot 把槽位还回池子，并按当初满足它的规格减少需求）。
    public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
        log.debug("Release slot with slot request id {}", slotRequestId);
        assertRunningInMainThread();

        final PendingRequest pendingRequest = pendingRequests.remove(slotRequestId);

        if (pendingRequest != null) {
            getDeclarativeSlotPool()
                    .decreaseResourceRequirementsBy(
                            ResourceCounter.withResource(pendingRequest.getResourceProfile(), 1));
            pendingRequest.failRequest(
                    new FlinkException(
                            String.format(
                                    "Pending slot request with %s has been released.",
                                    pendingRequest.getSlotRequestId()),
                            cause));
        } else {
            final AllocationID allocationId = fulfilledRequests.remove(slotRequestId);

            if (allocationId != null) {
                ResourceCounter previouslyFulfilledRequirement =
                        getDeclarativeSlotPool()
                                .freeReservedSlot(allocationId, cause, getRelativeTimeMillis());
                getDeclarativeSlotPool()
                        .decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
            } else {
                log.debug(
                        "Could not find slot which has fulfilled slot request {}. Ignoring the release operation.",
                        slotRequestId);
            }
        }
    }

    @Override
    // RM 判定"资源凑不齐"时的回调入口（JobMaster 侧），最终会失败掉流式作业的待处理请求。
    // ★ 只失败非 batch 请求：流式作业要求所有并发顶点同时就绪，凑不齐根本起不来，必须尽早报错而不是无限等待。
    public void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {
        assertRunningInMainThread();

        failPendingRequests(acquiredResources);
    }

    private void failPendingRequests(Collection<ResourceRequirement> acquiredResources) {
        // only fails streaming requests because batch jobs do not require all resources
        // requirements to be fullfilled at the same time
        Predicate<PendingRequest> predicate = request -> !request.isBatchRequest();
        if (pendingRequests.values().stream().anyMatch(predicate)) {
            log.warn(
                    "Could not acquire the minimum required resources, failing slot requests. Acquired: {}. Current slot pool status: {}",
                    acquiredResources,
                    getSlotServiceStatus());
            cancelPendingRequests(
                    predicate,
                    NoResourceAvailableException.withoutStackTrace(
                            "Could not acquire the minimum required resources."));
        }
    }

    @Override
    public Collection<SlotInfo> getAllocatedSlotsInformation() {
        assertRunningInMainThread();

        final Collection<? extends SlotInfo> allSlotsInformation =
                getDeclarativeSlotPool().getAllSlotsInformation();
        final Set<AllocationID> freeSlots =
                getDeclarativeSlotPool().getFreeSlotInfoTracker().getAvailableSlots();

        return allSlotsInformation.stream()
                .filter(slotInfo -> !freeSlots.contains(slotInfo.getAllocationId()))
                .collect(Collectors.toList());
    }

    @Override
    public FreeSlotInfoTracker getFreeSlotInfoTracker() {
        assertRunningInMainThread();

        return getDeclarativeSlotPool().getFreeSlotInfoTracker();
    }

    @Override
    public void disableBatchSlotRequestTimeoutCheck() {
        isBatchSlotRequestTimeoutCheckDisabled = true;
    }

    private void assertRunningInMainThread() {
        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.assertRunningInMainThread();
        } else {
            throw new IllegalStateException("The FutureSlotPool has not been started yet.");
        }
    }

    // 周期性回收：把太久没被使用的空闲槽位还给 RM（releaseIdleSlots）。
    // ★ 每次执行后都重新排一次自己 —— ComponentMainThreadExecutor.schedule 是一次性的，不会自动重复。
    private void checkIdleSlotTimeout() {
        getDeclarativeSlotPool().releaseIdleSlots(getRelativeTimeMillis());

        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.schedule(
                    this::checkIdleSlotTimeout,
                    idleSlotTimeout.toMilliseconds(),
                    TimeUnit.MILLISECONDS);
        }
    }

    // batch 请求的特殊处理：把待处理请求按"池中是否已有匹配规格的槽位"分成可满足/不可满足两组；
    // 不可满足的状态持续超过 batchSlotTimeout 就超时失败（避免批作业死等一个永远不来的槽位）。
    // ★ 判定用的是"池中已有槽位"的规格集合，而不是发起新请求，符合声明式的思路。
    void checkBatchSlotTimeout() {
        assertRunningInMainThread();

        if (isBatchSlotRequestTimeoutCheckDisabled) {
            return;
        }

        final Collection<PendingRequest> pendingBatchRequests = getPendingBatchRequests();

        if (!pendingBatchRequests.isEmpty()) {
            final Set<ResourceProfile> allResourceProfiles = getResourceProfilesFromAllSlots();

            final Map<Boolean, List<PendingRequest>> fulfillableAndUnfulfillableRequests =
                    pendingBatchRequests.stream()
                            .collect(
                                    Collectors.partitioningBy(
                                            canBeFulfilledWithAnySlot(allResourceProfiles)));

            final List<PendingRequest> fulfillableRequests =
                    fulfillableAndUnfulfillableRequests.get(true);
            final List<PendingRequest> unfulfillableRequests =
                    fulfillableAndUnfulfillableRequests.get(false);

            final long currentTimestamp = getRelativeTimeMillis();

            for (PendingRequest fulfillableRequest : fulfillableRequests) {
                fulfillableRequest.markFulfillable();
            }

            for (PendingRequest unfulfillableRequest : unfulfillableRequests) {
                unfulfillableRequest.markUnfulfillable(currentTimestamp);

                if (unfulfillableRequest.getUnfulfillableSince() + batchSlotTimeout.toMilliseconds()
                        <= currentTimestamp) {
                    timeoutPendingSlotRequest(unfulfillableRequest.getSlotRequestId());
                }
            }
        }

        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.schedule(
                    this::checkBatchSlotTimeout,
                    batchSlotTimeout.toMilliseconds(),
                    TimeUnit.MILLISECONDS);
        }
    }

    private Set<ResourceProfile> getResourceProfilesFromAllSlots() {
        return Stream.concat(
                        getFreeSlotInfoTracker().getFreeSlotsInformation().stream(),
                        getAllocatedSlotsInformation().stream())
                .map(SlotInfo::getResourceProfile)
                .collect(Collectors.toSet());
    }

    private Collection<PendingRequest> getPendingBatchRequests() {
        return pendingRequests.values().stream()
                .filter(PendingRequest::isBatchRequest)
                .collect(Collectors.toList());
    }

    private static Predicate<PendingRequest> canBeFulfilledWithAnySlot(
            Set<ResourceProfile> allocatedResourceProfiles) {
        return pendingRequest -> {
            for (ResourceProfile allocatedResourceProfile : allocatedResourceProfiles) {
                if (allocatedResourceProfile.isMatching(pendingRequest.getResourceProfile())) {
                    return true;
                }
            }

            return false;
        };
    }

    @VisibleForTesting
    public int getNumPendingRequests() {
        return pendingRequests.size();
    }

    @VisibleForTesting
    void increaseResourceRequirementsBy(ResourceCounter increment) {
        getDeclarativeSlotPool().increaseResourceRequirementsBy(increment);
    }

    @VisibleForTesting
    boolean isBatchSlotRequestTimeoutCheckEnabled() {
        return !isBatchSlotRequestTimeoutCheckDisabled;
    }
}
