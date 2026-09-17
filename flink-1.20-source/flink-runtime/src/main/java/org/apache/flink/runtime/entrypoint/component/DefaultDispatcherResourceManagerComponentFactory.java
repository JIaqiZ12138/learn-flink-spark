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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherOperationCaches;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.HaServicesJobPersistenceComponentFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerServiceImpl;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.rest.RestEndpointFactory;
import org.apache.flink.runtime.rest.SessionRestEndpointFactory;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract class which implements the creation of the {@link DispatcherResourceManagerComponent}
 * components.
 */
public class DefaultDispatcherResourceManagerComponentFactory
        implements DispatcherResourceManagerComponentFactory {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Nonnull private final DispatcherRunnerFactory dispatcherRunnerFactory;

    @Nonnull private final ResourceManagerFactory<?> resourceManagerFactory;

    @Nonnull private final RestEndpointFactory<?> restEndpointFactory;

    public DefaultDispatcherResourceManagerComponentFactory(
            @Nonnull DispatcherRunnerFactory dispatcherRunnerFactory,
            @Nonnull ResourceManagerFactory<?> resourceManagerFactory,
            @Nonnull RestEndpointFactory<?> restEndpointFactory) {
        this.dispatcherRunnerFactory = dispatcherRunnerFactory;
        this.resourceManagerFactory = resourceManagerFactory;
        this.restEndpointFactory = restEndpointFactory;
    }

    /**
     * ★★ 把 AM 容器里的三大组件"启动"起来。
     *
     * <p>⚠️ <b>重要认知：只有 WebMonitorEndpoint 是同步启动的。</b>
     * Dispatcher 和 ResourceManager 都只是<b>登记参选</b>（{@code startLeaderElection}）后立即返回，
     * 真正的实例创建发生在<b>选主成功的异步回调</b>里。所以本方法返回时，
     * Dispatcher 和 ResourceManager 可能<b>还没被创建出来</b>，AM 也还没向 YARN 注册。
     *
     * <p>三条线各自的形态：
     *
     * <ol>
     *   <li><b>WebMonitorEndpoint</b> —— <b>同步</b>：{@code start()} 返回即已就绪。
     *       必须最先起，因为 ResourceManager 初始化时要把 Web URL 报给 YARN。
     *   <li><b>Dispatcher</b> —— <b>异步</b>：{@code createDispatcherRunner()} 只是登记参选。
     *       选主成功 → 创建领导进程 → 创建 {@code MiniDispatcher}/{@code StandaloneDispatcher}
     *       → {@code dispatcher.start()} → {@code startRecoveredJobs()}。
     *       per-job 的作业就是在这条异步链上被提交的。
     *   <li><b>ResourceManager</b> —— <b>异步</b>：{@code start()} 也只是登记参选。
     *       选主成功 → 创建 RM → {@code resourceManager.start()} →
     *       驱动初始化 → {@code registerApplicationMaster()}。AM 在此正式上线。
     * </ol>
     *
     * <p>两条异步线<b>互不等待</b>：AM 注册与作业提交并行推进，
     * 因此作业有可能先于"AM 注册完成"就开始调度了。
     *
     * <p>本方法返回的 {@link DispatcherResourceManagerComponent} 持有这三者以及两个
     * LeaderRetrievalService，供后续统一关闭。
     */
    @Override
    public DispatcherResourceManagerComponent create(
            Configuration configuration,
            ResourceID resourceId,
            Executor ioExecutor,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            BlobServer blobServer,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            MetricRegistry metricRegistry,
            ExecutionGraphInfoStore executionGraphInfoStore,
            MetricQueryServiceRetriever metricQueryServiceRetriever,
            Collection<FailureEnricher> failureEnrichers,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {

        LeaderRetrievalService dispatcherLeaderRetrievalService = null;
        LeaderRetrievalService resourceManagerRetrievalService = null;
        WebMonitorEndpoint<?> webMonitorEndpoint = null;
        ResourceManagerService resourceManagerService = null;
        DispatcherRunner dispatcherRunner = null;

        try {
            // LeaderRetrievalService：用于"订阅"某个组件的当前领导节点。
            // 这里先拿到句柄，最后才 start()（见方法末尾）。
            dispatcherLeaderRetrievalService =
                    highAvailabilityServices.getDispatcherLeaderRetriever();

            resourceManagerRetrievalService =
                    highAvailabilityServices.getResourceManagerLeaderRetriever();

            final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever =
                    new RpcGatewayRetriever<>(
                            rpcService,
                            DispatcherGateway.class,
                            DispatcherId::fromUuid,
                            new ExponentialBackoffRetryStrategy(
                                    12, Duration.ofMillis(10), Duration.ofMillis(50)));

            final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever =
                    new RpcGatewayRetriever<>(
                            rpcService,
                            ResourceManagerGateway.class,
                            ResourceManagerId::fromUuid,
                            new ExponentialBackoffRetryStrategy(
                                    12, Duration.ofMillis(10), Duration.ofMillis(50)));

            final ScheduledExecutorService executor =
                    WebMonitorEndpoint.createExecutorService(
                            configuration.get(RestOptions.SERVER_NUM_THREADS),
                            configuration.get(RestOptions.SERVER_THREAD_PRIORITY),
                            "DispatcherRestEndpoint");

            final long updateInterval =
                    configuration.get(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL).toMillis();
            final MetricFetcher metricFetcher =
                    updateInterval == 0
                            ? VoidMetricFetcher.INSTANCE
                            : MetricFetcherImpl.fromConfiguration(
                                    configuration,
                                    metricQueryServiceRetriever,
                                    dispatcherGatewayRetriever,
                                    executor);

            webMonitorEndpoint =
                    restEndpointFactory.createRestEndpoint(
                            configuration,
                            dispatcherGatewayRetriever,
                            resourceManagerGatewayRetriever,
                            blobServer,
                            executor,
                            metricFetcher,
                            highAvailabilityServices.getClusterRestEndpointLeaderElection(),
                            fatalErrorHandler);

            // 【组件 1/3】先起 REST 端点。
            // 它负责把 Web UI、REST API（含 /jobs 提交、/jars 上传等）挂起来。
            // 必须最先启动：紧接着创建 ResourceManager 时要把 Web URL 报给 YARN。
            log.debug("Starting Dispatcher REST endpoint.");
            webMonitorEndpoint.start();

            final String hostname = RpcUtils.getHostname(rpcService); // 如本地 localhost:8081

            // create resourceManagerService
            resourceManagerService =
                    ResourceManagerServiceImpl.create(
                            resourceManagerFactory,
                            configuration,
                            resourceId,
                            rpcService,
                            highAvailabilityServices,
                            heartbeatServices,
                            delegationTokenManager,
                            fatalErrorHandler,
                            new ClusterInformation(hostname, blobServer.getPort()),
                            webMonitorEndpoint.getRestBaseUrl(),
                            metricRegistry,
                            hostname,
                            ioExecutor);

            final HistoryServerArchivist historyServerArchivist =
                    HistoryServerArchivist.createHistoryServerArchivist(
                            configuration, webMonitorEndpoint, ioExecutor);

            final DispatcherOperationCaches dispatcherOperationCaches =
                    new DispatcherOperationCaches(
                            configuration.get(RestOptions.ASYNC_OPERATION_STORE_DURATION));

            final PartialDispatcherServices partialDispatcherServices =
                    new PartialDispatcherServices(
                            configuration,
                            highAvailabilityServices,
                            resourceManagerGatewayRetriever,
                            blobServer,
                            heartbeatServices,
                            () ->
                                    JobManagerMetricGroup.createJobManagerMetricGroup(
                                            metricRegistry, hostname),
                            executionGraphInfoStore,
                            fatalErrorHandler,
                            historyServerArchivist,
                            metricRegistry.getMetricQueryServiceGatewayRpcAddress(),
                            ioExecutor,
                            dispatcherOperationCaches,
                            failureEnrichers);

            // 【组件 2/3】创建 DispatcherRunner —— 注意是【异步】的。
            // createDispatcherRunner() 内部只做了 leaderElection.startLeaderElection(this)，
            // 即"登记参选"后立刻返回。此时 Dispatcher 实例还不存在。
            // 选主成功后的异步链路：
            //   grantLeadership → JobDispatcherLeaderProcess.onStart()
            //     → JobDispatcherFactory.createDispatcher(recoveredJobs=[jobGraph])
            //       → new MiniDispatcher(...) → dispatcher.start() → startRecoveredJobs()
            // ★ per-job 的作业就是在这条异步链上被提交的。
            log.debug("Starting Dispatcher.");
            dispatcherRunner =
                    dispatcherRunnerFactory.createDispatcherRunner(
                            highAvailabilityServices.getDispatcherLeaderElection(),
                            fatalErrorHandler,
                            new HaServicesJobPersistenceComponentFactory(highAvailabilityServices),
                            ioExecutor,
                            rpcService,
                            partialDispatcherServices);

            // 【组件 3/3】启动 ResourceManagerService —— 同样是【异步】的。
            // start() 内部也只做 leaderElection.startLeaderElection(this)。
            // 选主成功后的异步链路：
            //   grantLeadership → startNewLeaderResourceManager()
            //     → resourceManagerFactory.createResourceManager(...) → new YarnResourceManager(...)
            //     → resourceManager.start() → startResourceManagerServices()
            //       → initialize() → ResourceManagerDriver.initializeInternal()
            //         → registerApplicationMaster()   ★★ 到这里 AM 才正式注册到 YARN
            // 注意它和上面 Dispatcher 那条线互不等待，并行推进。
            log.debug("Starting ResourceManagerService.");
            resourceManagerService.start();

            // 最后启动两个"领导节点发现"服务：让外部（REST 网关 / 其他组件）
            // 能订阅到当前的 Dispatcher / ResourceManager 领导地址。
            // HA 切换时，领导节点变了，订阅者会自动收到新地址并重新绑定。
            resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);
            dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

            return new DispatcherResourceManagerComponent(
                    dispatcherRunner,
                    resourceManagerService,
                    dispatcherLeaderRetrievalService,
                    resourceManagerRetrievalService,
                    webMonitorEndpoint,
                    fatalErrorHandler,
                    dispatcherOperationCaches);

        } catch (Exception exception) {
            // clean up all started components
            if (dispatcherLeaderRetrievalService != null) {
                try {
                    dispatcherLeaderRetrievalService.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            if (resourceManagerRetrievalService != null) {
                try {
                    resourceManagerRetrievalService.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

            if (webMonitorEndpoint != null) {
                terminationFutures.add(webMonitorEndpoint.closeAsync());
            }

            if (resourceManagerService != null) {
                terminationFutures.add(resourceManagerService.closeAsync());
            }

            if (dispatcherRunner != null) {
                terminationFutures.add(dispatcherRunner.closeAsync());
            }

            final FutureUtils.ConjunctFuture<Void> terminationFuture =
                    FutureUtils.completeAll(terminationFutures);

            try {
                terminationFuture.get();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            throw new FlinkException(
                    "Could not create the DispatcherResourceManagerComponent.", exception);
        }
    }

    public static DefaultDispatcherResourceManagerComponentFactory createSessionComponentFactory(
            ResourceManagerFactory<?> resourceManagerFactory) {
        return new DefaultDispatcherResourceManagerComponentFactory(
                DefaultDispatcherRunnerFactory.createSessionRunner(
                        SessionDispatcherFactory.INSTANCE),
                resourceManagerFactory,
                SessionRestEndpointFactory.INSTANCE);
    }

    // 内部装载一个 DispatcherRunnerFactory 和 resourceManagerFactory
    public static DefaultDispatcherResourceManagerComponentFactory createJobComponentFactory(
            ResourceManagerFactory<?> resourceManagerFactory, JobGraphRetriever jobGraphRetriever) {
        return new DefaultDispatcherResourceManagerComponentFactory(
                DefaultDispatcherRunnerFactory.createJobRunner(jobGraphRetriever),
                resourceManagerFactory,
                JobRestEndpointFactory.INSTANCE);
    }
}
