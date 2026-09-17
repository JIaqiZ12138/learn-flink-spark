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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Default {@link JobMasterServiceProcess} which is responsible for creating and running a {@link
 * JobMasterService}. The process is responsible for receiving the signals from the {@link
 * JobMasterService} and to create the respective {@link JobManagerRunnerResult} from it.
 *
 * <p>The {@link JobMasterService} can be created asynchronously and the creation can also fail.
 * That is why the process needs to observe the creation operation and complete the {@link
 * #resultFuture} with an initialization failure.
 *
 * <p>The {@link #resultFuture} can be completed with the following values:
 *
 * <ul>
 *   <li>{@link JobManagerRunnerResult} to signal an initialization failure of the {@link
 *       JobMasterService} or the completion of a job
 *   <li>{@link JobNotFinishedException} to signal that the job has not been completed by the {@link
 *       JobMasterService}
 *   <li>{@link Exception} to signal an unexpected failure
 * </ul>
 */
// JobMasterService 的状态机持有者：未启动 / 运行中 / 已关闭。
// 分工：JobMasterServiceLeadershipRunner 管“选主”，本类管“这一任 leader 的 JobMaster 生命周期”。
// ★ 构造函数里就异步发起 JobMaster 创建，此时属于“运行中但未就绪”；closeAsync() 置 isRunning=false
//   并关掉 JobMaster，进入“已关闭”；若期间 JobMaster 意外死亡，则判定作业失败。
// 结果统一从 resultFuture 冒泡给 runner：初始化失败 / 作业全局终态 / 异常终止。
public class DefaultJobMasterServiceProcess
        implements JobMasterServiceProcess, OnCompletionActions {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultJobMasterServiceProcess.class);

    private final Object lock = new Object();

    private final JobID jobId;

    private final UUID leaderSessionId;

    private final CompletableFuture<JobMasterService> jobMasterServiceFuture;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private final CompletableFuture<JobManagerRunnerResult> resultFuture =
            new CompletableFuture<>();

    private final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture =
            new CompletableFuture<>();

    private final CompletableFuture<String> leaderAddressFuture = new CompletableFuture<>();

    @GuardedBy("lock")
    private boolean isRunning = true;

    public DefaultJobMasterServiceProcess(
            JobID jobId,
            UUID leaderSessionId,
            JobMasterServiceFactory jobMasterServiceFactory,
            Function<Throwable, ArchivedExecutionGraph> failedArchivedExecutionGraphFactory) {
        this.jobId = jobId;
        this.leaderSessionId = leaderSessionId;
        // ★【执行图在这里被构建】createJobMasterService 内部异步 new JobMaster(...)，
        //   而 JobMaster 构造函数里就会调 createScheduler() → DefaultScheduler
        //   → DefaultExecutionGraphBuilder.buildGraph()，所以“建图”就发生在这一刻。
        this.jobMasterServiceFuture =
                jobMasterServiceFactory.createJobMasterService(leaderSessionId, this);

        // ★ 必须观察创建结果：JobMaster 建图失败（例如用户 jar 缺类）不能静默，
        //   要包装成 JobInitializationException 并以 forInitializationFailure 结束 resultFuture。
        jobMasterServiceFuture.whenComplete(
                (jobMasterService, throwable) -> {
                    if (throwable != null) {
                        final JobInitializationException jobInitializationException =
                                new JobInitializationException(
                                        jobId, "Could not start the JobMaster.", throwable);

                        LOG.debug(
                                "Initialization of the JobMasterService for job {} under leader id {} failed.",
                                jobId,
                                leaderSessionId,
                                jobInitializationException);

                        resultFuture.complete(
                                JobManagerRunnerResult.forInitializationFailure(
                                        new ExecutionGraphInfo(
                                                failedArchivedExecutionGraphFactory.apply(
                                                        jobInitializationException)),
                                        jobInitializationException));
                    } else {
                        registerJobMasterServiceFutures(jobMasterService);
                    }
                });
    }

    // ★ leaderAddressFuture 完成会让 runner 去 confirmLeadership —— 这一步做完才算真正“当上主”。
    private void registerJobMasterServiceFutures(JobMasterService jobMasterService) {
        LOG.debug(
                "Successfully created the JobMasterService for job {} under leader id {}.",
                jobId,
                leaderSessionId);
        jobMasterGatewayFuture.complete(jobMasterService.getGateway());
        leaderAddressFuture.complete(jobMasterService.getAddress());

        // ★ 运行期间 JobMaster 意外退出（例如致命错误）会走到这里；此时 isRunning 仍为 true，
        //   说明是非预期死亡，直接把作业判失败。
        jobMasterService
                .getTerminationFuture()
                .whenComplete(
                        (unused, throwable) -> {
                            synchronized (lock) {
                                if (isRunning) {
                                    LOG.warn(
                                            "Unexpected termination of the JobMasterService for job {} under leader id {}.",
                                            jobId,
                                            leaderSessionId);
                                    jobMasterFailed(
                                            new FlinkException(
                                                    "Unexpected termination of the JobMasterService.",
                                                    throwable));
                                }
                            }
                        });
    }

    // 关闭语义 = 这一任 leader 结束：★ 用 JobNotFinishedException 结束 resultFuture，
    // 让 runner 知道“本进程没把作业跑完”，以此区别于作业真正完成。
    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (isRunning) {
                isRunning = false;

                LOG.debug(
                        "Terminating the JobMasterService process for job {} under leader id {}.",
                        jobId,
                        leaderSessionId);

                resultFuture.completeExceptionally(new JobNotFinishedException(jobId));
                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException("Process has been closed."));

                jobMasterServiceFuture.whenComplete(
                        (jobMasterService, throwable) -> {
                            if (throwable != null) {
                                // JobMasterService creation has failed. Nothing to stop then :-)
                                terminationFuture.complete(null);
                            } else {
                                FutureUtils.forward(
                                        jobMasterService.closeAsync(), terminationFuture);
                            }
                        });

                terminationFuture.whenComplete(
                        (unused, throwable) ->
                                LOG.debug(
                                        "JobMasterService process for job {} under leader id {} has been terminated.",
                                        jobId,
                                        leaderSessionId));
            }
        }
        return terminationFuture;
    }

    @Override
    public UUID getLeaderSessionId() {
        return leaderSessionId;
    }

    @Override
    public boolean isInitializedAndRunning() {
        synchronized (lock) {
            return jobMasterServiceFuture.isDone()
                    && !jobMasterServiceFuture.isCompletedExceptionally()
                    && isRunning;
        }
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture() {
        return jobMasterGatewayFuture;
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public CompletableFuture<String> getLeaderAddressFuture() {
        return leaderAddressFuture;
    }

    // OnCompletionActions 回调之一：作业到达 FINISHED/CANCELED/FAILED 全局终态 → 以成功结果结束。
    @Override
    public void jobReachedGloballyTerminalState(ExecutionGraphInfo executionGraphInfo) {
        LOG.debug(
                "Job {} under leader id {} reached a globally terminal state {}.",
                jobId,
                leaderSessionId,
                executionGraphInfo.getArchivedExecutionGraph().getState());
        resultFuture.complete(JobManagerRunnerResult.forSuccess(executionGraphInfo));
    }

    // ★ 失败路径：JobMaster 报错 → resultFuture 异常完成 → JobMasterServiceLeadershipRunner 在
    //   forwardResultFuture/onJobCompletion 接住 → 最终成为 Dispatcher 看到的作业失败。
    @Override
    public void jobMasterFailed(Throwable cause) {
        LOG.debug("Job {} under leader id {} failed.", jobId, leaderSessionId);
        resultFuture.completeExceptionally(cause);
    }
}
