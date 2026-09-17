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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.NoOpDispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServicesWithJobPersistenceComponents;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;

/** Factory for the {@link DefaultDispatcherGatewayService}. */
class DefaultDispatcherGatewayServiceFactory
        implements AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory {

    private final DispatcherFactory dispatcherFactory;

    private final RpcService rpcService;

    private final PartialDispatcherServices partialDispatcherServices;

    DefaultDispatcherGatewayServiceFactory(
            DispatcherFactory dispatcherFactory,
            RpcService rpcService,
            PartialDispatcherServices partialDispatcherServices) {
        this.dispatcherFactory = dispatcherFactory;
        this.rpcService = rpcService;
        this.partialDispatcherServices = partialDispatcherServices;
    }

    /**
     * ★★ Dispatcher 实例真正被创建并启动的地方。
     *
     * <p>这一行 {@code dispatcherFactory.createDispatcher(...)} 是<b>多态的最后一跳</b>：
     *
     * <pre>
     *   per-job     → JobDispatcherFactory.createDispatcher(recoveredJobs=[jobGraph], ...)
     *                 → new MiniDispatcher(...)            （单作业，绕过 submitJob）
     *   session     → SessionDispatcherFactory.createDispatcher(...)
     *                 → new StandaloneDispatcher(...)      （可随时接新作业）
     *   application → ApplicationDispatcherFactory
     *                 → new ApplicationDispatcher(...)
     * </pre>
     *
     * <p>{@code recoveredJobs} 就是 per-job 场景下从 {@code job.graph} 读出来的那个作业；
     * session/application 场景是空集合。
     *
     * <p>紧接着的 {@code dispatcher.start()} 才会触发
     * {@code Dispatcher.onStart()} → {@code startRecoveredJobs()}，
     * 到这一刻 per-job 的作业才真正开始走调度链路。
     *
     * <p>顺带注意 {@code NoOpDispatcherBootstrap}：非 application 模式的 Dispatcher
     * 不需要额外的启动引导，所以塞一个空实现。
     */
    @Override
    public AbstractDispatcherLeaderProcess.DispatcherGatewayService create(
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobResults,
            JobGraphWriter jobGraphWriter,
            JobResultStore jobResultStore) {

        final Dispatcher dispatcher;
        try {
            dispatcher =
                    dispatcherFactory.createDispatcher(
                            rpcService,
                            fencingToken, // fencing token：防止脑裂的"任期号"
                            recoveredJobs,
                            recoveredDirtyJobResults,
                            (dispatcherGateway, scheduledExecutor, errorHandler) ->
                                    new NoOpDispatcherBootstrap(),
                            PartialDispatcherServicesWithJobPersistenceComponents.from(
                                    partialDispatcherServices, jobGraphWriter, jobResultStore));
        } catch (Exception e) {
            throw new FlinkRuntimeException("Could not create the Dispatcher rpc endpoint.", e);
        }

        // ★ 启动 Dispatcher：内部会 startDispatcherServices() + startCleanupRetries()
        //   + startRecoveredJobs()，最后一步才是提交那个 per-job 作业。
        dispatcher.start();

        return DefaultDispatcherGatewayService.from(dispatcher);
    }
}
