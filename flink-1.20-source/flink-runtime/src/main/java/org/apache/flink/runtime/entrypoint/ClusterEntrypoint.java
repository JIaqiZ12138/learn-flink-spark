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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.management.jmx.JMXService;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.MiniDispatcher;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.TraceReporterSetup;
import org.apache.flink.runtime.metrics.groups.ProcessMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcSystemUtils;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.security.token.DefaultDelegationTokenManagerFactory;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcMetricQueryServiceRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.Reference;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for the Flink cluster entry points.
 *
 * <p>Specialization of this class can be used for the session mode and the per-job mode
 */
public abstract class ClusterEntrypoint implements AutoCloseableAsync, FatalErrorHandler {

    @Internal
    public static final ConfigOption<String> INTERNAL_CLUSTER_EXECUTION_MODE =
            ConfigOptions.key("internal.cluster.execution-mode")
                    .stringType()
                    .defaultValue(ExecutionMode.NORMAL.toString());

    protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypoint.class);

    protected static final int STARTUP_FAILURE_RETURN_CODE = 1;
    protected static final int RUNTIME_FAILURE_RETURN_CODE = 2;

    private static final Time INITIALIZATION_SHUTDOWN_TIMEOUT = Time.seconds(30L);

    /** The lock to guard startup / shutdown / manipulation methods. */
    private final Object lock = new Object();

    private final Configuration configuration;

    private final CompletableFuture<ApplicationStatus> terminationFuture;

    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    @GuardedBy("lock")
    private DeterminismEnvelope<ResourceID> resourceId;

    @GuardedBy("lock")
    private DispatcherResourceManagerComponent clusterComponent;

    @GuardedBy("lock")
    private MetricRegistryImpl metricRegistry;

    @GuardedBy("lock")
    private ProcessMetricGroup processMetricGroup;

    @GuardedBy("lock")
    private HighAvailabilityServices haServices;

    @GuardedBy("lock")
    private BlobServer blobServer;

    @GuardedBy("lock")
    private HeartbeatServices heartbeatServices;

    @GuardedBy("lock")
    private Collection<FailureEnricher> failureEnrichers;

    @GuardedBy("lock")
    private DelegationTokenManager delegationTokenManager;

    @GuardedBy("lock")
    private RpcService commonRpcService;

    @GuardedBy("lock")
    private ExecutorService ioExecutor;

    @GuardedBy("lock")
    private DeterminismEnvelope<WorkingDirectory> workingDirectory;

    private ExecutionGraphInfoStore executionGraphInfoStore;

    private final Thread shutDownHook;
    private RpcSystem rpcSystem;

    protected ClusterEntrypoint(Configuration configuration) {
        this.configuration = generateClusterConfiguration(configuration);
        this.terminationFuture = new CompletableFuture<>();

        if (configuration.get(JobManagerOptions.SCHEDULER_MODE) == SchedulerExecutionMode.REACTIVE
                && !supportsReactiveMode()) {
            final String msg =
                    "Reactive mode is configured for an unsupported cluster type. At the moment, reactive mode is only supported by standalone application clusters (bin/standalone-job.sh).";
            // log message as well, otherwise the error is only shown in the .out file of the
            // cluster
            LOG.error(msg);
            throw new IllegalConfigurationException(msg);
        }

        shutDownHook =
                ShutdownHookUtil.addShutdownHook(
                        () -> this.closeAsync().join(), getClass().getSimpleName(), LOG);
    }

    public int getRestPort() {
        synchronized (lock) {
            assertClusterEntrypointIsStarted();

            return clusterComponent.getRestPort();
        }
    }

    public int getRpcPort() {
        synchronized (lock) {
            assertClusterEntrypointIsStarted();

            return commonRpcService.getPort();
        }
    }

    @GuardedBy("lock")
    private void assertClusterEntrypointIsStarted() {
        Preconditions.checkNotNull(
                commonRpcService,
                String.format("%s has not been started yet.", getClass().getSimpleName()));
    }

    public CompletableFuture<ApplicationStatus> getTerminationFuture() {
        return terminationFuture;
    }

    /**
     * ★ 启动集群：进程级准备 + 起组件。返回即表示 JobManager 已经可用。
     *
     * <p>执行顺序是有讲究的：安全上下文必须在 RPC/文件系统之前装好（否则连 HDFS 都没权限），
     * 而 {@code runCluster()} 又必须在安全上下文里执行（{@code runSecured}），
     * 这样它内部创建的所有线程都会继承该 UGI。
     */
    public void startCluster() throws ClusterEntrypointException {
        LOG.info("Starting {}.", getClass().getSimpleName());

        try {
            // 按配置设置 Java 安全管理器（默认不启用）
            FlinkSecurityManager.setFromConfiguration(configuration);

            // 插件（plugins/ 目录）加载器——连接器、格式、文件系统都靠它
            PluginManager pluginManager =
                    PluginUtils.createPluginManagerFromRootFolder(configuration);

            // 初始化 Flink 的文件系统抽象（HDFS/S3/OSS 等）
            configureFileSystems(configuration, pluginManager);

            // ★ 安装安全上下文：Kerberos 登录、Hadoop UGI 设置等都在这里完成。
            //   没有它，后面访问 HDFS / YARN 都会因权限失败。
            SecurityContext securityContext = installSecurityContext(configuration);

            // 配置未捕获异常处理器，避免线程悄悄死掉导致"作业卡住没日志"
            ClusterEntrypointUtils.configureUncaughtExceptionHandler(configuration);

            // ★ runSecured：把 runCluster() 跑在刚装好的安全上下文里，
            //   保证其中创建的所有线程都继承该登录用户（Hadoop UGI 是 ThreadLocal 语义）。
            securityContext.runSecured(
                    (Callable<Void>)
                            () -> {
                                runCluster(configuration, pluginManager);

                                return null;
                            });
        } catch (Throwable t) {
            final Throwable strippedThrowable =
                    ExceptionUtils.stripException(t, UndeclaredThrowableException.class);

            try {
                // 启动失败时清理已经起来的半成品组件，避免留下"僵尸"RPC 端点/端口占用
                // clean up any partial state
                shutDownAsync(
                                ApplicationStatus.FAILED,
                                ShutdownBehaviour.GRACEFUL_SHUTDOWN,
                                ExceptionUtils.stringifyException(strippedThrowable),
                                false)
                        .get(
                                INITIALIZATION_SHUTDOWN_TIMEOUT.toMilliseconds(),
                                TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                strippedThrowable.addSuppressed(e);
            }

            throw new ClusterEntrypointException(
                    String.format(
                            "Failed to initialize the cluster entrypoint %s.",
                            getClass().getSimpleName()),
                    strippedThrowable);
        }
    }

    protected boolean supportsReactiveMode() {
        return false;
    }

    private void configureFileSystems(Configuration configuration, PluginManager pluginManager) {
        LOG.info("Install default filesystem.");
        FileSystem.initialize(configuration, pluginManager);
    }

    public static SecurityContext installSecurityContext(Configuration configuration)
            throws Exception {
        LOG.info("Install security context.");

        SecurityUtils.install(new SecurityConfiguration(configuration));

        return SecurityUtils.getInstalledContext();
    }

    /**
     * ★ 起组件：先备齐基础设施（{@code initializeServices}），再创建并启动三大组件。
     *
     * <p>这是「entrypoint 家族的差异点」与「公共骨架」的交界处：
     * 上面全是共用的，下面 {@code createDispatcherResourceManagerComponentFactory()} 是
     * 各子类的多态点——YARN session / per-job / application 的区别就在这一个调用上。
     */
    private void runCluster(Configuration configuration, PluginManager pluginManager)
            throws Exception {
        synchronized (lock) {
            // ---- 第 1 步：基础设施（RPC / HA / BlobServer / 指标 / ExecutionGraphInfoStore）----
            initializeServices(configuration, pluginManager);

            // 把 RPC 的实际地址端口回写进配置，后续组件（以及传给 TM 的配置）都用它，
            // 否则可能出现"绑定了随机端口但别人还按配置里的端口去连"的问题。
            // write host information into configuration
            configuration.set(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
            configuration.set(JobManagerOptions.PORT, commonRpcService.getPort());

            // ★★ 多态分派点：子类决定"搭出什么样的 JobManager"
            //     YarnSessionClusterEntrypoint   → createSessionComponentFactory（StandaloneDispatcher）
            //     YarnJobClusterEntrypoint       → createJobComponentFactory（MiniDispatcher + 单作业）
            //     YarnApplicationClusterEntryPoint→ ApplicationDispatcher

            // factory 模式
            final DispatcherResourceManagerComponentFactory
                    dispatcherResourceManagerComponentFactory =
                            createDispatcherResourceManagerComponentFactory(configuration);

            // ---- 第 2 步：创建并启动三大组件 ----
            // create() 内部会依次启动 WebMonitorEndpoint → ResourceManager → Dispatcher，
            // 其中 ResourceManager 的启动过程里会向 YARN 注册 AM（详见该方法注释）。
            clusterComponent =
                    dispatcherResourceManagerComponentFactory.create(
                            configuration,
                            resourceId.unwrap(),
                            ioExecutor,
                            commonRpcService,
                            haServices,
                            blobServer,
                            heartbeatServices,
                            delegationTokenManager,
                            metricRegistry,
                            executionGraphInfoStore,
                            new RpcMetricQueryServiceRetriever(
                                    metricRegistry.getMetricQueryServiceRpcService()),
                            failureEnrichers,
                            this);

            // ---- 第 3 步：挂关机钩子 ----
            // 三大组件里任何一个决定关闭（作业跑完、收到 stop、致命错误），
            // 都会通过 getShutDownFuture() 冒泡到这里，进而触发整个集群的优雅关闭。
            // 这个 future 最终会让 runClusterEntrypoint() 里的
            // getTerminationFuture().get() 返回，进程随之退出。
            clusterComponent
                    .getShutDownFuture()
                    .whenComplete(
                            (ApplicationStatus applicationStatus, Throwable throwable) -> {
                                if (throwable != null) {
                                    shutDownAsync(
                                            ApplicationStatus.UNKNOWN,
                                            ShutdownBehaviour.GRACEFUL_SHUTDOWN,
                                            ExceptionUtils.stringifyException(throwable),
                                            false);
                                } else {
                                    // This is the general shutdown path. If a separate more
                                    // specific shutdown was
                                    // already triggered, this will do nothing
                                    shutDownAsync(
                                            applicationStatus,
                                            ShutdownBehaviour.GRACEFUL_SHUTDOWN,
                                            null,
                                            true);
                                }
                            });
        }
    }

    /**
     * ★ 备齐集群基础设施。这些是所有组件（Dispatcher / ResourceManager / TaskExecutor 通信）
     * 共同依赖的底座，必须在创建三大组件<b>之前</b>就绪。
     *
     * <p>产出物（都是本类的字段，供后续复用）：
     * {@code resourceId}、{@code workingDirectory}、{@code rpcSystem}、{@code commonRpcService}、
     * {@code ioExecutor}、{@code delegationTokenManager}、{@code haServices}、{@code blobServer}、
     * {@code heartbeatServices}、{@code metricRegistry}、{@code executionGraphInfoStore}。
     */
    protected void initializeServices(Configuration configuration, PluginManager pluginManager)
            throws Exception {

        LOG.info("Initializing cluster services.");

        synchronized (lock) {
            // JDK 自带的命令行管理接口，用于暴露 JVM 指标
            // JMX 端口等由 JMXServerOptions 配置——注意这是 JVM 层的，不是 Flink 的 RPC
            resourceId =
                    configuration
                            .getOptional(JobManagerOptions.JOB_MANAGER_RESOURCE_ID)
                            .map(
                                    value ->
                                            DeterminismEnvelope.deterministicValue(
                                                    new ResourceID(value)))
                            .orElseGet(
                                    () ->
                                            DeterminismEnvelope.nondeterministicValue(
                                                    ResourceID.generate()));

            LOG.debug(
                    "Initialize cluster entrypoint {} with resource id {}.",
                    getClass().getSimpleName(),
                    resourceId);

            // 每个 JobManager 一个工作目录，存放 blob 存储、临时文件等。
            // 用 resourceId 隔离，保证同一台机器上多个 JM 实例互不干扰。
            workingDirectory =
                    ClusterEntrypointUtils.createJobManagerWorkingDirectory(
                            configuration, resourceId);

            LOG.info("Using working directory: {}.", workingDirectory);

            // ★ RPC 框架从 SPI 加载（默认 Pekko 实现），并创建一个对外服务端点。
            //   这个 commonRpcService 是 JM 上所有 RpcEndpoint（Dispatcher/ResourceManager/...）
            //   的宿主，也是 TM 能连上 JobManager 的入口。
            rpcSystem = RpcSystem.load(configuration);

            commonRpcService =
                    RpcUtils.createRemoteRpcService(
                            rpcSystem,
                            configuration,
                            configuration.get(JobManagerOptions.ADDRESS),
                            // ★ 端口范围是子类多态的：YARN 用 yarn.application-master.port，
                            //   standalone 用 jobmanager.rpc.port，ZK HA 模式用 HA_JOB_MANAGER_PORT_RANGE
                            getRPCPortRange(configuration),
                            configuration.get(JobManagerOptions.BIND_HOST),
                            configuration.getOptional(JobManagerOptions.RPC_BIND_PORT));

            JMXService.startInstance(configuration.get(JMXServerOptions.JMX_SERVER_PORT));

            // 这里拿到的可能是实际绑定的地址（比如配的是 0.0.0.0 或端口是范围时），
            // 所以要把真实值回写进配置，后面创建 HA 服务时才能用对。
            // update the configuration used to create the high availability services
            configuration.set(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
            configuration.set(JobManagerOptions.PORT, commonRpcService.getPort());

            // 统一的 IO 线程池：blob 读写、HA 操作、历史归档等阻塞型操作都丢给它，
            // 避免阻塞 RPC 线程。
            ioExecutor =
                    Executors.newFixedThreadPool(
                            ClusterEntrypointUtils.getPoolSize(configuration),
                            new ExecutorThreadFactory("cluster-io"));
            delegationTokenManager =
                    DefaultDelegationTokenManagerFactory.create(
                            configuration,
                            pluginManager,
                            commonRpcService.getScheduledExecutor(),
                            ioExecutor);
            // Obtaining delegation tokens and propagating them to the local JVM receivers in a
            // one-time fashion is required because BlobServer may connect to external file systems
            delegationTokenManager.obtainDelegationTokens();
            // HA 服务：选主（LeaderElection）、JobGraph 存储、checkpoint 恢复等。
            // 没配 HA 时是 EmbeddedHaServices 的内存实现；配了 ZK 才是分布式实现。
            haServices = createHaServices(configuration, ioExecutor, rpcSystem);

            // BlobServer：大文件（用户 jar、JobGraph 序列化后的 blob 等）的中转站。
            // 提交作业时 jar 会被上传到这里，TM 再从这里拉取——这就是"JobGraph 里只存 blob key"
            // 的落点。
            // 它绑定一个随机端口，端口号会回写进配置传给 TM。
            blobServer =
                    BlobUtils.createBlobServer(
                            configuration,
                            Reference.borrowed(workingDirectory.unwrap().getBlobStorageDirectory()),
                            haServices.createBlobStore());
            blobServer.start();
            configuration.set(BlobServerOptions.PORT, String.valueOf(blobServer.getPort()));

            // 心跳服务：JM↔TM、RM↔TM 的心跳与超时检测
            heartbeatServices = createHeartbeatServices(configuration);

            // 失败信息增强器（例如把 K8s 容器退出原因附加到异常上）
            failureEnrichers = FailureEnricherUtils.getFailureEnrichers(configuration);

            // 指标注册中心 + 独立的指标查询 RPC 服务（Web UI 拉指标走这条链路）
            metricRegistry = createMetricRegistry(configuration, pluginManager, rpcSystem);

            final RpcService metricQueryServiceRpcService =
                    MetricUtils.startRemoteMetricsRpcService(
                            configuration,
                            commonRpcService.getAddress(),
                            configuration.get(JobManagerOptions.BIND_HOST),
                            rpcSystem);
            metricRegistry.startQueryService(metricQueryServiceRpcService, null);

            final String hostname = RpcUtils.getHostname(commonRpcService);

            processMetricGroup =
                    MetricUtils.instantiateProcessMetricGroup(
                            metricRegistry,
                            hostname,
                            ConfigurationUtils.getSystemResourceMetricsProbingInterval(
                                    configuration));

            // 已完成作业的信息存储（供 Web UI 展示历史作业）。
            // 注意它是"可序列化"的：HA 场景下要能把作业摘要持久化出去。
            executionGraphInfoStore =
                    createSerializableExecutionGraphStore(
                            configuration, commonRpcService.getScheduledExecutor());
        }
    }

    /**
     * Returns the port range for the common {@link RpcService}.
     *
     * @param configuration to extract the port range from
     * @return Port range for the common {@link RpcService}
     */
    protected String getRPCPortRange(Configuration configuration) {
        if (ZooKeeperUtils.isZooKeeperRecoveryMode(configuration)) {
            return configuration.get(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE);
        } else {
            return String.valueOf(configuration.get(JobManagerOptions.PORT));
        }
    }

    protected HighAvailabilityServices createHaServices(
            Configuration configuration, Executor executor, RpcSystemUtils rpcSystemUtils)
            throws Exception {
        return HighAvailabilityServicesUtils.createHighAvailabilityServices(
                configuration,
                executor,
                AddressResolution.NO_ADDRESS_RESOLUTION,
                rpcSystemUtils,
                this);
    }

    protected HeartbeatServices createHeartbeatServices(Configuration configuration) {
        return HeartbeatServices.fromConfiguration(configuration);
    }

    protected MetricRegistryImpl createMetricRegistry(
            Configuration configuration,
            PluginManager pluginManager,
            RpcSystemUtils rpcSystemUtils) {
        return new MetricRegistryImpl(
                MetricRegistryConfiguration.fromConfiguration(
                        configuration, rpcSystemUtils.getMaximumMessageSizeInBytes(configuration)),
                ReporterSetup.fromConfiguration(configuration, pluginManager),
                TraceReporterSetup.fromConfiguration(configuration, pluginManager));
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        ShutdownHookUtil.removeShutdownHook(shutDownHook, getClass().getSimpleName(), LOG);

        return shutDownAsync(
                        ApplicationStatus.UNKNOWN,
                        ShutdownBehaviour.PROCESS_FAILURE,
                        "Cluster entrypoint has been closed externally.",
                        false)
                .thenAccept(ignored -> {});
    }

    protected CompletableFuture<Void> stopClusterServices(boolean cleanupHaData) {
        final long shutdownTimeout =
                configuration.get(ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT).toMillis();

        synchronized (lock) {
            Throwable exception = null;

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

            if (blobServer != null) {
                try {
                    blobServer.close();
                } catch (Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if (haServices != null) {
                try {
                    haServices.closeWithOptionalClean(cleanupHaData);
                } catch (Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if (executionGraphInfoStore != null) {
                try {
                    executionGraphInfoStore.close();
                } catch (Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if (processMetricGroup != null) {
                processMetricGroup.close();
            }

            if (metricRegistry != null) {
                terminationFutures.add(metricRegistry.closeAsync());
            }

            if (ioExecutor != null) {
                terminationFutures.add(
                        ExecutorUtils.nonBlockingShutdown(
                                shutdownTimeout, TimeUnit.MILLISECONDS, ioExecutor));
            }

            if (commonRpcService != null) {
                terminationFutures.add(commonRpcService.closeAsync());
            }

            try {
                JMXService.stopInstance();
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            if (exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }

            return FutureUtils.completeAll(terminationFutures);
        }
    }

    @Override
    public void onFatalError(Throwable exception) {
        ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(exception);
        LOG.error("Fatal error occurred in the cluster entrypoint.", exception);

        FlinkSecurityManager.forceProcessExit(RUNTIME_FAILURE_RETURN_CODE);
    }

    // --------------------------------------------------
    // Internal methods
    // --------------------------------------------------

    private Configuration generateClusterConfiguration(Configuration configuration) {
        final Configuration resultConfiguration =
                new Configuration(Preconditions.checkNotNull(configuration));

        final String webTmpDir = configuration.get(WebOptions.TMP_DIR);
        final File uniqueWebTmpDir = new File(webTmpDir, "flink-web-" + UUID.randomUUID());

        resultConfiguration.set(WebOptions.TMP_DIR, uniqueWebTmpDir.getAbsolutePath());

        return resultConfiguration;
    }

    private CompletableFuture<ApplicationStatus> shutDownAsync(
            ApplicationStatus applicationStatus,
            ShutdownBehaviour shutdownBehaviour,
            @Nullable String diagnostics,
            boolean cleanupHaData) {
        if (isShutDown.compareAndSet(false, true)) {
            LOG.info(
                    "Shutting {} down with application status {}. Diagnostics {}.",
                    getClass().getSimpleName(),
                    applicationStatus,
                    diagnostics);

            final CompletableFuture<Void> shutDownApplicationFuture =
                    closeClusterComponent(applicationStatus, shutdownBehaviour, diagnostics);

            final CompletableFuture<Void> serviceShutdownFuture =
                    FutureUtils.composeAfterwards(
                            shutDownApplicationFuture, () -> stopClusterServices(cleanupHaData));

            final CompletableFuture<Void> rpcSystemClassLoaderCloseFuture =
                    rpcSystem != null
                            ? FutureUtils.runAfterwards(serviceShutdownFuture, rpcSystem::close)
                            : FutureUtils.completedVoidFuture();

            final CompletableFuture<Void> cleanupDirectoriesFuture =
                    FutureUtils.runAfterwards(
                            rpcSystemClassLoaderCloseFuture,
                            () -> cleanupDirectories(shutdownBehaviour));

            cleanupDirectoriesFuture.whenComplete(
                    (Void ignored2, Throwable serviceThrowable) -> {
                        if (serviceThrowable != null) {
                            terminationFuture.completeExceptionally(serviceThrowable);
                        } else {
                            terminationFuture.complete(applicationStatus);
                        }
                    });
        }

        return terminationFuture;
    }

    /**
     * Close cluster components and deregister the Flink application from the resource management
     * system by signalling the {@link ResourceManager}.
     *
     * @param applicationStatus to terminate the application with
     * @param shutdownBehaviour shutdown behaviour
     * @param diagnostics additional information about the shut down, can be {@code null}
     * @return Future which is completed once the shut down
     */
    private CompletableFuture<Void> closeClusterComponent(
            ApplicationStatus applicationStatus,
            ShutdownBehaviour shutdownBehaviour,
            @Nullable String diagnostics) {
        synchronized (lock) {
            if (clusterComponent != null) {
                switch (shutdownBehaviour) {
                    case GRACEFUL_SHUTDOWN:
                        return clusterComponent.stopApplication(applicationStatus, diagnostics);
                    case PROCESS_FAILURE:
                    default:
                        return clusterComponent.stopProcess();
                }
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    /**
     * Clean up of temporary directories created by the {@link ClusterEntrypoint}.
     *
     * @param shutdownBehaviour specifying the shutdown behaviour
     * @throws IOException if the temporary directories could not be cleaned up
     */
    protected void cleanupDirectories(ShutdownBehaviour shutdownBehaviour) throws IOException {
        IOException ioException = null;

        final String webTmpDir = configuration.get(WebOptions.TMP_DIR);

        try {
            FileUtils.deleteDirectory(new File(webTmpDir));
        } catch (IOException ioe) {
            ioException = ioe;
        }

        synchronized (lock) {
            if (workingDirectory != null) {
                // We only clean up the working directory if we gracefully shut down or if its path
                // is nondeterministic. If it is a process failure, then we want to keep the working
                // directory for potential recoveries.
                if (!workingDirectory.isDeterministic()
                        || shutdownBehaviour == ShutdownBehaviour.GRACEFUL_SHUTDOWN) {
                    try {
                        workingDirectory.unwrap().delete();
                    } catch (IOException ioe) {
                        ioException = ExceptionUtils.firstOrSuppressed(ioe, ioException);
                    }
                }
            }
        }

        if (ioException != null) {
            throw ioException;
        }
    }

    // --------------------------------------------------
    // Abstract methods
    // --------------------------------------------------

    protected abstract DispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(Configuration configuration)
                    throws IOException;

    protected abstract ExecutionGraphInfoStore createSerializableExecutionGraphStore(
            Configuration configuration, ScheduledExecutor scheduledExecutor) throws IOException;

    public static EntrypointClusterConfiguration parseArguments(String[] args)
            throws FlinkParseException {
        final CommandLineParser<EntrypointClusterConfiguration> clusterConfigurationParser =
                new CommandLineParser<>(new EntrypointClusterConfigurationParserFactory());

        return clusterConfigurationParser.parse(args);
    }

    protected static Configuration loadConfiguration(
            EntrypointClusterConfiguration entrypointClusterConfiguration) {
        final Configuration dynamicProperties =
                ConfigurationUtils.createConfiguration(
                        entrypointClusterConfiguration.getDynamicProperties());
        final Configuration configuration =
                GlobalConfiguration.loadConfiguration(
                        entrypointClusterConfiguration.getConfigDir(), dynamicProperties);

        final int restPort = entrypointClusterConfiguration.getRestPort();

        if (restPort >= 0) {
            LOG.warn(
                    "The 'webui-port' parameter of 'jobmanager.sh' has been deprecated. Please use '-D {}=<port> instead.",
                    RestOptions.PORT);
            configuration.set(RestOptions.PORT, restPort);
        }

        final String hostname = entrypointClusterConfiguration.getHostname();

        if (hostname != null) {
            LOG.warn(
                    "The 'host' parameter of 'jobmanager.sh' has been deprecated. Please use '-D {}=<host> instead.",
                    JobManagerOptions.ADDRESS);
            configuration.set(JobManagerOptions.ADDRESS, hostname);
        }

        return configuration;
    }

    // --------------------------------------------------
    // Helper methods
    // --------------------------------------------------

    /**
     * ★★ 所有 ClusterEntrypoint 的公共启动骨架（YARN / K8s / standalone 共用）。
     *
     * <p>它做两件事，而且顺序很关键：
     *
     * <ol>
     *   <li><b>启动集群</b>：{@link #startCluster()}。成功返回时，JobManager 的三大组件
     *       （WebMonitorEndpoint + ResourceManager + Dispatcher）已经在跑了。
     *       —— 对 YARN 来说，其中的 ResourceManager 启动过程里会调用
     *       {@code registerApplicationMaster()} 向 YARN 注册，这才是"AM 正式上线"的时刻。
     *   <li><b>阻塞并退出</b>：等待 {@code getTerminationFuture()}，然后 {@code System.exit}。
     *       所以这个方法<b>永远不会正常返回</b>——它就是这个 JVM 的主循环。
     * </ol>
     *
     * <p>退出码语义：启动失败 → {@code STARTUP_FAILURE_RETURN_CODE}；
     * 运行期异常或作业失败 → {@code RUNTIME_FAILURE_RETURN_CODE}；
     * 正常终止 → {@code getTerminationFuture()} 里携带的 {@code processExitCode()}。
     */
    public static void runClusterEntrypoint(ClusterEntrypoint clusterEntrypoint) {

        final String clusterEntrypointName = clusterEntrypoint.getClass().getSimpleName();

        // ---- 阶段一：启动集群 ----
        try {
            clusterEntrypoint.startCluster();
        } catch (ClusterEntrypointException e) {
            // 启动阶段失败：打印并直接以指定退出码结束进程。
            // 这个退出码会被 YARN 看到，进而决定 Application 的最终状态。
            LOG.error(
                    String.format("Could not start cluster entrypoint %s.", clusterEntrypointName),
                    e);
            System.exit(STARTUP_FAILURE_RETURN_CODE);
        }

        // ---- 阶段二：等待终止 ----
        int returnCode;
        Throwable throwable = null;

        try {
            // ★ 关键：这里阻塞住，直到集群决定终止。
            //   终止可能来自：作业跑完（per-job 模式）、收到关闭信号、或发生不可恢复错误。
            //   注意这行之前，AM 已经完成了向 YARN 的注册，作业也可能早就跑完了。
            returnCode = clusterEntrypoint.getTerminationFuture().get().processExitCode();
        } catch (Throwable e) {
            throwable = ExceptionUtils.stripExecutionException(e);
            returnCode = RUNTIME_FAILURE_RETURN_CODE;
        }

        LOG.info(
                "Terminating cluster entrypoint process {} with exit code {}.",
                clusterEntrypointName,
                returnCode,
                throwable);

        // ★ 显式退出：AM 容器进程到此结束，YARN 随后回收整个 Application 的资源。
        System.exit(returnCode);
    }

    /** Execution mode of the {@link MiniDispatcher}. */
    public enum ExecutionMode {
        /** Waits until the job result has been served. */
        NORMAL,

        /** Directly stops after the job has finished. */
        DETACHED
    }

    /** Shutdown behaviour of a {@link ClusterEntrypoint}. */
    protected enum ShutdownBehaviour {
        // Graceful shutdown means that the process wants to terminate and will clean everything up
        GRACEFUL_SHUTDOWN,
        // Process failure means that we don't clean up things so that they could be recovered
        PROCESS_FAILURE,
    }
}
