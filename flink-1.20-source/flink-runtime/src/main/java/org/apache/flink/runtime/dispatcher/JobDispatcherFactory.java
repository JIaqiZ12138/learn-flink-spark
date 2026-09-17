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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.cleanup.CheckpointResourcesCleanupRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import java.util.Collection;

import static org.apache.flink.runtime.entrypoint.ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE;

// 【异步创建链的最后一环】DispatcherFactory 的 per-job 实现（枚举单例 INSTANCE，无状态、可随时被回调）：
//   由选主成功后的 JobDispatcherLeaderProcess.onStart() 回调，走到这里才真正 new 出 Dispatcher。
/** {@link DispatcherFactory} which creates a {@link MiniDispatcher}. */
public enum JobDispatcherFactory implements DispatcherFactory {
    INSTANCE;

    // ★ 关键入参 recoveredJobs：per-job 模式下由 AM 启动时的 FileJobGraphRetriever 从 YARN local
    //   resource 的 job.graph 读回、经 JobDispatcherLeaderProcess 原样透传，长度恰好为 1；它作为
    //   MiniDispatcher 的 jobGraph 继续下传，最终由父类 startRecoveredJobs 以 ExecutionType.RECOVERY
    //   拉起 —— 第一个作业是"恢复"而不是 SUBMISSION。fencingToken 是本轮选主拿到的 DispatcherId
    //   （栅栏令牌，让被抢占的旧 Dispatcher 失效），其余 HA 服务由 partialDispatcherServices 注入。
    @Override
    public MiniDispatcher createDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobResults,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            PartialDispatcherServicesWithJobPersistenceComponents
                    partialDispatcherServicesWithJobPersistenceComponents)
            throws Exception {
        final JobGraph recoveredJobGraph = Iterables.getOnlyElement(recoveredJobs, null);
        final JobResult recoveredDirtyJob =
                Iterables.getOnlyElement(recoveredDirtyJobResults, null);

        // ★ 异或校验：恢复一个"待跑的作业图"和恢复一个"待清理的脏 JobResult"是两条互斥路径，不能同时给。
        Preconditions.checkArgument(
                recoveredJobGraph == null ^ recoveredDirtyJob == null,
                "Either the JobGraph or the recovered JobResult needs to be specified.");

        final Configuration configuration =
                partialDispatcherServicesWithJobPersistenceComponents.getConfiguration();
        final String executionModeValue = configuration.get(INTERNAL_CLUSTER_EXECUTION_MODE);
        final ClusterEntrypoint.ExecutionMode executionMode =
                ClusterEntrypoint.ExecutionMode.valueOf(executionModeValue);

        return new MiniDispatcher(
                rpcService,
                fencingToken,
                DispatcherServices.from(
                        partialDispatcherServicesWithJobPersistenceComponents,
                        JobMasterServiceLeadershipRunnerFactory.INSTANCE,
                        CheckpointResourcesCleanupRunnerFactory.INSTANCE),
                recoveredJobGraph,
                recoveredDirtyJob,
                dispatcherBootstrapFactory,
                executionMode);
    }
}
