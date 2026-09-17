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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleanerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

// ★ per-job（YARN application / 单作业）模式专用的 Dispatcher，由 JobClusterEntrypoint 创建；
//   session 模式用的是 StandaloneDispatcher。本类的差别有三点：只服务一个 JobGraph（构造期注入，
//   不走 submitJob RPC）；该作业以 ExecutionType.RECOVERY 拉起；作业终态或结果被取走后直接关集群。
//   异步创建链路：grantLeadership → JobDispatcherLeaderProcess.onStart()
//     → JobDispatcherFactory.createDispatcher(recoveredJobs=[jobGraph]) → new MiniDispatcher(...)
/**
 *
 * Per-job 模式使用的 Dispatcher
 *
 * Mini Dispatcher which is instantiated as the dispatcher component by the {@link
 * JobClusterEntrypoint}.
 *
 * <p>The mini dispatcher is initialized with a single {@link JobGraph} which it runs.
 *
 * <p>Depending on the {@link ClusterEntrypoint.ExecutionMode}, the mini dispatcher will directly
 * terminate after job completion if its execution mode is {@link
 * ClusterEntrypoint.ExecutionMode#DETACHED}.
 */
public class MiniDispatcher extends Dispatcher {

    private final JobClusterEntrypoint.ExecutionMode executionMode;
    // 作业是否被显式取消：取消说明这个 per-job 集群已无存在意义，终态回调里据此直接关集群。
    private boolean jobCancelled = false;

    // ★ 构造期就把"要跑的那个作业"交给父类：这里的 jobGraph 不是客户端 RPC 提交进来的，而是 AM
    //   启动时由 FileJobGraphRetriever 从 YARN local resource 读回的 job.graph，父类会把它记入
    //   recoveredJobs 集合（随后由 onStart → startRecoveredJobs 拉起）；recoveredDirtyJob 则是另一条
    //   恢复入口——上次运行留下的、尚未清理干净的 JobResult，二者互斥，由 JobDispatcherFactory 校验。
    public MiniDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            DispatcherServices dispatcherServices,
            @Nullable JobGraph jobGraph,
            @Nullable JobResult recoveredDirtyJob,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            JobClusterEntrypoint.ExecutionMode executionMode)
            throws Exception {
        super(
                rpcService,
                fencingToken,
                CollectionUtil.ofNullable(jobGraph),
                CollectionUtil.ofNullable(recoveredDirtyJob),
                dispatcherBootstrapFactory,
                dispatcherServices);

        this.executionMode = checkNotNull(executionMode);
    }

    @VisibleForTesting
    public MiniDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            DispatcherServices dispatcherServices,
            @Nullable JobGraph jobGraph,
            @Nullable JobResult recoveredDirtyJob,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            JobManagerRunnerRegistry jobManagerRunnerRegistry,
            ResourceCleanerFactory resourceCleanerFactory,
            JobClusterEntrypoint.ExecutionMode executionMode)
            throws Exception {
        super(
                rpcService,
                fencingToken,
                CollectionUtil.ofNullable(jobGraph),
                CollectionUtil.ofNullable(recoveredDirtyJob),
                dispatcherBootstrapFactory,
                dispatcherServices,
                jobManagerRunnerRegistry,
                resourceCleanerFactory);

        this.executionMode = checkNotNull(executionMode);
    }

    // ★ 第一个作业不经过这里：它以 recovered job 的身份由父类在 onStart 时直接拉起（RECOVERY）；
    //   客户端在 per-job 模式下只把 job.graph 交给 YARN，之后仅轮询结果。本方法只在有人用 REST/CLI
    //   直连该 JobManager 额外提交作业时才生效，且一失败就走 onFatalError —— per-job 集群不容忍多作业。
    // per-job 模式 的 dispatcher 在此提交
    @Override
    public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
        final CompletableFuture<Acknowledge> acknowledgeCompletableFuture =
                super.submitJob(jobGraph, timeout);

        acknowledgeCompletableFuture.whenComplete(
                (Acknowledge ignored, Throwable throwable) -> {
                    if (throwable != null) {
                        onFatalError(
                                new FlinkException(
                                        "Failed to submit job "
                                                + jobGraph.getJobID()
                                                + " in job mode.",
                                        throwable));
                    }
                });

        return acknowledgeCompletableFuture;
    }

    @Override
    public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
        final CompletableFuture<JobResult> jobResultFuture = super.requestJobResult(jobId, timeout);

        // ★ NORMAL 模式表示"提交方会来取结果"：一旦有人成功取到 JobResult，这个 per-job 集群的使命
        //   就算完成，于是完成 shutDownFuture 触发整个集群退出。DETACHED 模式下没人取结果，关闭动作
        //   改由 runPostJobGloballyTerminated 负责，所以这里刻意什么都不做。
        if (executionMode == ClusterEntrypoint.ExecutionMode.NORMAL) {
            // terminate the MiniDispatcher once we served the first JobResult successfully
            jobResultFuture.thenAccept(
                    (JobResult result) -> {
                        ApplicationStatus status =
                                result.getSerializedThrowable().isPresent()
                                        ? ApplicationStatus.FAILED
                                        : ApplicationStatus.SUCCEEDED;

                        if (!ApplicationStatus.UNKNOWN.equals(result.getApplicationStatus())) {
                            log.info(
                                    "Shutting down cluster because someone retrieved the job result"
                                            + " and the status is globally terminal.");
                            shutDownFuture.complete(status);
                        }
                    });
        } else {
            log.info("Not shutting down cluster after someone retrieved the job result.");
        }

        return jobResultFuture;
    }

    @Override
    public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
        jobCancelled = true;
        return super.cancelJob(jobId, timeout);
    }

    // ★ 作业终态即集群终态：per-job 集群只服务这一个作业，所以被取消（jobCancelled）或 DETACHED 模式
    //   （提交完就放手、不等结果）时，直接完成 shutDownFuture 关掉整个 MiniCluster。
    //   父类里该方法是空实现，专门留出这个钩子给 MiniDispatcher 收尾（清理跑完后再关集群）。
    @Override
    protected void runPostJobGloballyTerminated(JobID jobId, JobStatus jobStatus) {
        super.runPostJobGloballyTerminated(jobId, jobStatus);

        if (jobCancelled || executionMode == ClusterEntrypoint.ExecutionMode.DETACHED) {
            // shut down if job is cancelled or we don't have to wait for the execution
            // result retrieval
            log.info(
                    "Shutting down cluster after job with state {}, jobCancelled: {}, executionMode: {}",
                    jobStatus,
                    jobCancelled,
                    executionMode);
            shutDownFuture.complete(ApplicationStatus.fromJobStatus(jobStatus));
        }
    }
}
