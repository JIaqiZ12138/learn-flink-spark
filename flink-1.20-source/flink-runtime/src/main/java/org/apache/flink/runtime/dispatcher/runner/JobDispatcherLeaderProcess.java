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

import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.ThrowingJobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.UUID;

/** {@link DispatcherLeaderProcess} implementation for the per-job mode. */
public class JobDispatcherLeaderProcess extends AbstractDispatcherLeaderProcess {

    private final DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory;

    @Nullable private final JobGraph jobGraph;
    @Nullable private final JobResult recoveredDirtyJobResult;

    private final JobResultStore jobResultStore;

    JobDispatcherLeaderProcess(
            UUID leaderSessionId,
            DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory,
            @Nullable JobGraph jobGraph,
            @Nullable JobResult recoveredDirtyJobResult,
            JobResultStore jobResultStore,
            FatalErrorHandler fatalErrorHandler) {
        super(leaderSessionId, fatalErrorHandler);
        this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
        this.jobGraph = jobGraph;
        this.recoveredDirtyJobResult = recoveredDirtyJobResult;
        this.jobResultStore = Preconditions.checkNotNull(jobResultStore);
    }

    /**
     * ★★ 领导进程启动 —— per-job 模式下 JobGraph 转成"已恢复作业"的关键一跳。
     *
     * <p>注意第 2 个参数：{@code CollectionUtil.ofNullable(jobGraph)}。
     * 这个 {@code jobGraph} 是一路从 {@code FileJobGraphRetriever} 读出来、
     * 经 {@code JobDispatcherFactory} 传进来的，在这里被当成
     * <b>{@code recoveredJobs}</b>（"已恢复的作业集合"）传给 Dispatcher。
     *
     * <p>这就是为什么 per-job 的作业<b>不走 {@code submitJob()} RPC</b>：
     * 它是以"启动时就已存在的作业"的身份进入 Dispatcher 的，
     * 后续由 {@code Dispatcher.startRecoveredJobs()} 提交。
     *
     * <p>另外两个参数也值得留意：
     *
     * <ul>
     *   <li>{@code ThrowingJobGraphWriter.INSTANCE} —— per-job 模式<b>不写</b> JobGraphStore
     *       （作业来自文件，不需要 HA 持久化；误调会直接抛异常）；
     *   <li>{@code jobResultStore} —— 作业执行结果仍然要落盘，用于保证"已终结的作业不会被重复执行"。
     * </ul>
     */
    @Override
    protected void onStart() {
        final DispatcherGatewayService dispatcherService =
                dispatcherGatewayServiceFactory.create(
                        DispatcherId.fromUuid(getLeaderSessionId()),
                        // ★ JobGraph 在这里"变成" recoveredJobs
                        CollectionUtil.ofNullable(jobGraph),
                        CollectionUtil.ofNullable(recoveredDirtyJobResult),
                        ThrowingJobGraphWriter.INSTANCE,
                        jobResultStore);

        completeDispatcherSetup(dispatcherService);
    }
}
