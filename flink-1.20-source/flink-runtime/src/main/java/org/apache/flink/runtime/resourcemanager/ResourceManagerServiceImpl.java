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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link ResourceManagerService}. */
public class ResourceManagerServiceImpl implements ResourceManagerService, LeaderContender {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerServiceImpl.class);

    private final ResourceManagerFactory<?> resourceManagerFactory;
    private final ResourceManagerProcessContext rmProcessContext;

    private final LeaderElection leaderElection;

    private final FatalErrorHandler fatalErrorHandler;
    private final Executor ioExecutor;

    private final ExecutorService handleLeaderEventExecutor;
    private final CompletableFuture<Void> serviceTerminationFuture;

    private final Object lock = new Object();

    @GuardedBy("lock")
    private boolean running;

    @Nullable
    @GuardedBy("lock")
    private ResourceManager<?> leaderResourceManager;

    @Nullable
    @GuardedBy("lock")
    private UUID leaderSessionID;

    @GuardedBy("lock")
    private CompletableFuture<Void> previousResourceManagerTerminationFuture;

    private ResourceManagerServiceImpl(
            ResourceManagerFactory<?> resourceManagerFactory,
            ResourceManagerProcessContext rmProcessContext)
            throws Exception {
        this.resourceManagerFactory = checkNotNull(resourceManagerFactory);
        this.rmProcessContext = checkNotNull(rmProcessContext);

        this.leaderElection =
                rmProcessContext.getHighAvailabilityServices().getResourceManagerLeaderElection();
        this.fatalErrorHandler = rmProcessContext.getFatalErrorHandler();
        this.ioExecutor = rmProcessContext.getIoExecutor();

        this.handleLeaderEventExecutor = Executors.newSingleThreadExecutor();
        this.serviceTerminationFuture = new CompletableFuture<>();

        this.running = false;
        this.leaderResourceManager = null;
        this.leaderSessionID = null;
        this.previousResourceManagerTerminationFuture = FutureUtils.completedVoidFuture();
    }

    // ------------------------------------------------------------------------
    //  ResourceManagerService
    // ------------------------------------------------------------------------

    /**
     * ★ 与 Dispatcher 完全同构的"异步参选"模式：这里只是登记参选，立即返回。
     *
     * <p>调用方（{@code DefaultDispatcherResourceManagerComponentFactory.create()}）
     * 在这一行返回时，<b>ResourceManager 实例还不存在</b>，更别说向 YARN 注册 AM 了。
     *
     * <p>真正的启动链发生在选主回调里：
     *
     * <pre>
     *   grantLeadership(leaderSessionId)
     *     → startNewLeaderResourceManager(leaderSessionId)                  :249
     *       ├─ resourceManagerFactory.createResourceManager(...)  → new YarnResourceManager(...)
     *       │    （构造过程中创建 FineGrainedSlotManager）
     *       └─ startResourceManagerIfIsLeader(rm)                           :281
     *           └─ resourceManager.start()                                  ★ 真正启动
     *               └─ ResourceManager.startResourceManagerServices()       ResourceManager:268
     *                   ├─ jobLeaderIdService.start(...)
     *                   ├─ registerMetrics()
     *                   ├─ startHeartbeatServices()
     *                   ├─ slotManager.start(...)          ← FineGrainedSlotManager
     *                   ├─ delegationTokenManager.start(this)
     *                   └─ initialize()                    ← ActiveResourceManager 覆盖
     *                       └─ resourceManagerDriver.initialize(...)
     *                           └─ AbstractResourceManagerDriver.initialize()
     *                               └─ initializeInternal()  ← YarnResourceManagerDriver 覆盖
     *                                   ├─ AMRMClientAsync 创建并启动
     *                                   ├─ registerApplicationMaster()   ★★ AM 注册，AM 正式上线
     *                                   └─ NMClientAsync 创建并启动
     * </pre>
     *
     * <p>注意最后还会 {@code confirmLeadershipAsync(...)} 向选主服务确认任期，
     * 这之后其他组件才能真正通过 LeaderRetrievalService 发现这个 RM。
     */
    @Override
    public void start() throws Exception {
        synchronized (lock) {
            if (running) {
                LOG.debug("Resource manager service has already started.");
                return;
            }
            running = true;
        }

        LOG.info("Starting resource manager service.");

        // ★ 异步：登记为 LeaderContender，选主成功后回调 grantLeadership()
        leaderElection.startLeaderElection(this);
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return serviceTerminationFuture;
    }

    @Override
    public CompletableFuture<Void> deregisterApplication(
            final ApplicationStatus applicationStatus, final @Nullable String diagnostics) {

        synchronized (lock) {
            if (!running || leaderResourceManager == null) {
                return deregisterWithoutLeaderRm();
            }

            final ResourceManager<?> currentLeaderRM = leaderResourceManager;
            return currentLeaderRM
                    .getStartedFuture()
                    .thenCompose(
                            ignore -> {
                                synchronized (lock) {
                                    if (isLeader(currentLeaderRM)) {
                                        return currentLeaderRM
                                                .getSelfGateway(ResourceManagerGateway.class)
                                                .deregisterApplication(
                                                        applicationStatus, diagnostics)
                                                .thenApply(ack -> null);
                                    } else {
                                        return deregisterWithoutLeaderRm();
                                    }
                                }
                            });
        }
    }

    private static CompletableFuture<Void> deregisterWithoutLeaderRm() {
        LOG.warn("Cannot deregister application. Resource manager service is not available.");
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (running) {
                LOG.info("Stopping resource manager service.");
                running = false;
                stopLeaderElectionService();
                stopLeaderResourceManager();
            } else {
                LOG.debug("Resource manager service is not running.");
            }

            FutureUtils.forward(previousResourceManagerTerminationFuture, serviceTerminationFuture);
        }

        handleLeaderEventExecutor.shutdownNow();

        return serviceTerminationFuture;
    }

    // ------------------------------------------------------------------------
    //  LeaderContender
    // ------------------------------------------------------------------------

    @Override
    public void grantLeadership(UUID newLeaderSessionID) {
        handleLeaderEventExecutor.execute(
                () -> {
                    synchronized (lock) {
                        if (!running) {
                            LOG.info(
                                    "Resource manager service is not running. Ignore granting leadership with session ID {}.",
                                    newLeaderSessionID);
                            return;
                        }

                        LOG.info(
                                "Resource manager service is granted leadership with session id {}.",
                                newLeaderSessionID);

                        try {
                            startNewLeaderResourceManager(newLeaderSessionID);
                        } catch (Throwable t) {
                            fatalErrorHandler.onFatalError(
                                    new FlinkException("Cannot start resource manager.", t));
                        }
                    }
                });
    }

    @Override
    public void revokeLeadership() {
        handleLeaderEventExecutor.execute(
                () -> {
                    synchronized (lock) {
                        if (!running) {
                            LOG.info(
                                    "Resource manager service is not running. Ignore revoking leadership.");
                            return;
                        }

                        LOG.info(
                                "Resource manager service is revoked leadership with session id {}.",
                                leaderSessionID);

                        stopLeaderResourceManager();

                        if (!resourceManagerFactory.supportMultiLeaderSession()) {
                            closeAsync();
                        }
                    }
                });
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(
                new FlinkException(
                        "Exception during leader election of resource manager occurred.",
                        exception));
    }

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    /**
     * ★★ 选主成功后的资源管理器创建与启动。
     *
     * <p>三步走，顺序有依赖：
     *
     * <ol>
     *   <li><b>createResourceManager</b> —— 由 {@code ResourceManagerFactory} 造出具体实现。
     *       对 YARN 来说是 {@code YarnResourceManager}（继承链
     *       {@code YarnResourceManager → ActiveResourceManager → ResourceManager}）。
     *       构造过程中会创建 {@code FineGrainedSlotManager}（1.20 唯一的 SlotManager 实现）。
     *   <li><b>start</b> —— 但必须等"上一个 RM 彻底终止"之后才启动，理由同 Dispatcher：
     *       避免两个 RM 实例同时对外服务。
     *   <li><b>confirmLeadershipAsync</b> —— 向选主服务确认任期，之后外部才能发现这个 RM。
     * </ol>
     */
    @GuardedBy("lock")
    private void startNewLeaderResourceManager(UUID newLeaderSessionID) throws Exception {
        // 停掉上一个（如果还在跑）
        stopLeaderResourceManager();

        this.leaderSessionID = newLeaderSessionID;
        this.leaderResourceManager =
                resourceManagerFactory.createResourceManager(rmProcessContext, newLeaderSessionID);

        final ResourceManager<?> newLeaderResourceManager = this.leaderResourceManager;

        // 先等旧 RM 终止，再启动新 RM（同样是"先停干净再起新的"）
        previousResourceManagerTerminationFuture
                .thenComposeAsync(
                        (ignore) -> {
                            synchronized (lock) {
                                return startResourceManagerIfIsLeader(newLeaderResourceManager);
                            }
                        },
                        handleLeaderEventExecutor)
                .thenAcceptAsync(
                        (isStillLeader) -> {
                            if (isStillLeader) {
                                // 确认任期，让 LeaderRetrievalService 的订阅者能发现这个 RM
                                leaderElection.confirmLeadershipAsync(
                                        newLeaderSessionID, newLeaderResourceManager.getAddress());
                            }
                        },
                        ioExecutor);
    }

    /**
     * Returns a future that completes as {@code true} if the resource manager is still leader and
     * started, and {@code false} if it's no longer leader.
     */
    @GuardedBy("lock")
    private CompletableFuture<Boolean> startResourceManagerIfIsLeader(
            ResourceManager<?> resourceManager) {
        if (isLeader(resourceManager)) {
            // ★ 真正启动：进入 ResourceManager.startResourceManagerServices()，
            //   最终在 YarnResourceManagerDriver.initializeInternal() 里向 YARN 注册 AM。
            resourceManager.start();
            forwardTerminationFuture(resourceManager);
            // getStartedFuture() 完成才表示 RM 可以对外服务（onStart 全过程走完）
            return resourceManager.getStartedFuture().thenApply(ignore -> true);
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    private void forwardTerminationFuture(ResourceManager<?> resourceManager) {
        resourceManager
                .getTerminationFuture()
                .whenComplete(
                        (ignore, throwable) -> {
                            synchronized (lock) {
                                if (isLeader(resourceManager)) {
                                    if (throwable != null) {
                                        serviceTerminationFuture.completeExceptionally(throwable);
                                    } else {
                                        serviceTerminationFuture.complete(null);
                                    }
                                }
                            }
                        });
    }

    @GuardedBy("lock")
    private boolean isLeader(ResourceManager<?> resourceManager) {
        return running && this.leaderResourceManager == resourceManager;
    }

    @GuardedBy("lock")
    private void stopLeaderResourceManager() {
        if (leaderResourceManager != null) {
            previousResourceManagerTerminationFuture =
                    previousResourceManagerTerminationFuture.thenCombine(
                            leaderResourceManager.closeAsync(), (ignore1, ignore2) -> null);
            leaderResourceManager = null;
            leaderSessionID = null;
        }
    }

    private void stopLeaderElectionService() {
        try {
            if (leaderElection != null) {
                leaderElection.close();
            }
        } catch (Exception e) {
            serviceTerminationFuture.completeExceptionally(
                    new FlinkException("Cannot stop leader election service.", e));
        }
    }

    @VisibleForTesting
    @Nullable
    public ResourceManager<?> getLeaderResourceManager() {
        synchronized (lock) {
            return leaderResourceManager;
        }
    }

    public static ResourceManagerServiceImpl create(
            ResourceManagerFactory<?> resourceManagerFactory,
            Configuration configuration,
            ResourceID resourceId,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            FatalErrorHandler fatalErrorHandler,
            ClusterInformation clusterInformation,
            @Nullable String webInterfaceUrl,
            MetricRegistry metricRegistry,
            String hostname,
            Executor ioExecutor)
            throws Exception {

        return new ResourceManagerServiceImpl(
                resourceManagerFactory,
                resourceManagerFactory.createResourceManagerProcessContext(
                        configuration,
                        resourceId,
                        rpcService,
                        highAvailabilityServices,
                        heartbeatServices,
                        delegationTokenManager,
                        fatalErrorHandler,
                        clusterInformation,
                        webInterfaceUrl,
                        metricRegistry,
                        hostname,
                        ioExecutor));
    }
}
