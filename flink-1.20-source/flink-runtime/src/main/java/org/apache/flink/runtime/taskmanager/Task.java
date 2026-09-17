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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.JobInfoImpl;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.runtime.jobgraph.tasks.CoordinatedTask;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotPayload;
import org.apache.flink.types.Either;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.MdcUtils.MdcCloseable;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TaskManagerExceptionUtils;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.WrappingRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The Task represents one execution of a parallel subtask on a TaskManager. A Task wraps a Flink
 * operator (which may be a user function) and runs it, providing all services necessary for example
 * to consume input data, produce its results (intermediate result partitions) and communicate with
 * the JobManager.
 *
 * <p>The Flink operators (implemented as subclasses of {@link TaskInvokable} have only data
 * readers, writers, and certain event callbacks. The task connects those to the network stack and
 * actor messages, and tracks the state of the execution and handles exceptions.
 *
 * <p>Tasks have no knowledge about how they relate to other tasks, or whether they are the first
 * attempt to execute the task, or a repeated attempt. All of that is only known to the JobManager.
 * All the task knows are its own runnable code, the task's configuration, and the IDs of the
 * intermediate results to consume and produce (if any).
 *
 * <p>Each Task is run by one dedicated thread.
 */
// ★ Task = "一个并行子任务的一次执行尝试"（one attempt），是 TM 侧最小的调度与执行单元。
//   对应关系自顶向下（这是最容易绕晕的地方）：
//     JobGraph 的 JobVertex（一个算子/算子链）
//       → ExecutionGraph 按并行度展开成 parallelism 个 ExecutionVertex
//         → 每个 ExecutionVertex 的每一次执行尝试是一个 Execution（带 attemptNumber）
//           → 在 TaskManager 上落成一个 Task，由 executionAttemptID 唯一标识这次尝试。
//   ★ 所以"重试"不是把同一个 Task 重跑，而是同一个 ExecutionVertex 产生新的
//   ExecutionAttemptID，在（可能是另一台）TM 上新建一个全新的 Task 对象。
//
//   它实现 Runnable：构造阶段只做准备（网络 reader/writer、状态管理器、指标组），
//   真正的执行在自己的专用线程里——TaskExecutor.submitTask() → startTaskThread() → run()。
//   这个线程负责实例化并驱动 TaskInvokable（流式作业里就是 StreamTask），
//   用户的算子链代码最终跑在 StreamTask 的 mailbox 线程上（本线程即该 mailbox 线程）。
//
//   它不感知全局拓扑，也不知道自己是第几次尝试、是否还有别的兄弟子任务；
//   它只持有本次执行所需的 ID、配置，以及要消费/产出的中间结果描述。
public class Task
        implements Runnable, TaskSlotPayload, TaskActions, PartitionProducerStateProvider {

    /** The class logger. */
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    /** The thread group that contains all task threads. */
    private static final ThreadGroup TASK_THREADS_GROUP = new ThreadGroup("Flink Task Threads");

    /** For atomic state updates. */
    private static final AtomicReferenceFieldUpdater<Task, ExecutionState> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    Task.class, ExecutionState.class, "executionState");

    // ------------------------------------------------------------------------
    //  Constant fields that are part of the initial Task construction
    // ------------------------------------------------------------------------

    /** The job that the task belongs to. */
    private final JobID jobId;

    /** The type of this job. */
    private final JobType jobType;

    /** The vertex in the JobGraph whose code the task executes. */
    private final JobVertexID vertexId;

    /** The execution attempt of the parallel subtask. */
    private final ExecutionAttemptID executionId;

    /** ID which identifies the slot in which the task is supposed to run. */
    private final AllocationID allocationId;

    /** The meta information of current job. */
    private final JobInfo jobInfo;

    /** The meta information of current task. */
    private final TaskInfo taskInfo;

    /** The name of the task, including subtask indexes. */
    private final String taskNameWithSubtask;

    /** The job-wide configuration object. */
    private final Configuration jobConfiguration;

    /** The task-specific configuration. */
    private final Configuration taskConfiguration;

    /** The jar files used by this task. */
    private final Collection<PermanentBlobKey> requiredJarFiles;

    /** The classpaths used by this task. */
    private final Collection<URL> requiredClasspaths;

    /** The name of the class that holds the invokable code. */
    private final String nameOfInvokableClass;

    /** Access to task manager configuration and host names. */
    private final TaskManagerRuntimeInfo taskManagerConfig;

    /** The memory manager to be used by this task. */
    private final MemoryManager memoryManager;

    /** Shared memory manager provided by the task manager. */
    private final SharedResources sharedResources;

    /** The I/O manager to be used by this task. */
    private final IOManager ioManager;

    /** The BroadcastVariableManager to be used by this task. */
    private final BroadcastVariableManager broadcastVariableManager;

    private final TaskEventDispatcher taskEventDispatcher;

    /** Information provider for external resources. */
    private final ExternalResourceInfoProvider externalResourceInfoProvider;

    /** The manager for state of operators running in this task/slot. */
    private final TaskStateManager taskStateManager;

    /**
     * Serialized version of the job specific execution configuration (see {@link ExecutionConfig}).
     */
    private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

    private final ResultPartitionWriter[] partitionWriters;

    private final IndexedInputGate[] inputGates;

    /** Connection to the task manager. */
    private final TaskManagerActions taskManagerActions;

    /** Input split provider for the task. */
    private final InputSplitProvider inputSplitProvider;

    /** Checkpoint notifier used to communicate with the CheckpointCoordinator. */
    private final CheckpointResponder checkpointResponder;

    /**
     * The gateway for operators to send messages to the operator coordinators on the Job Manager.
     */
    private final TaskOperatorEventGateway operatorCoordinatorEventGateway;

    /** GlobalAggregateManager used to update aggregates on the JobMaster. */
    private final GlobalAggregateManager aggregateManager;

    /** The library cache, from which the task can request its class loader. */
    private final LibraryCacheManager.ClassLoaderHandle classLoaderHandle;

    /** The cache for user-defined files that the invokable requires. */
    private final FileCache fileCache;

    /** The service for kvState registration of this task. */
    private final KvStateService kvStateService;

    /** The registry of this task which enables live reporting of accumulators. */
    private final AccumulatorRegistry accumulatorRegistry;

    /** The thread that executes the task. */
    private final Thread executingThread;

    /** Parent group for all metrics of this task. */
    private final TaskMetricGroup metrics;

    /** Partition producer state checker to request partition states from. */
    private final PartitionProducerStateChecker partitionProducerStateChecker;

    /** Executor to run future callbacks. */
    private final Executor executor;

    /** Future that is completed once {@link #run()} exits. */
    private final CompletableFuture<ExecutionState> terminationFuture = new CompletableFuture<>();

    /** The factory of channel state write request executor. */
    private final ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory;

    // ------------------------------------------------------------------------
    //  Fields that control the task execution. All these fields are volatile
    //  (which means that they introduce memory barriers), to establish
    //  proper happens-before semantics on parallel modification
    // ------------------------------------------------------------------------

    /** atomic flag that makes sure the invokable is canceled exactly once upon error. */
    private final AtomicBoolean invokableHasBeenCanceled;
    /**
     * The invokable of this task, if initialized. All accesses must copy the reference and check
     * for null, as this field is cleared as part of the disposal logic.
     */
    @Nullable private volatile TaskInvokable invokable;

    /** The current execution state of the task. */
    private volatile ExecutionState executionState = ExecutionState.CREATED;

    /** The observed exception, in case the task execution failed. */
    private volatile Throwable failureCause;

    /** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
    private long taskCancellationInterval;

    /** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
    private long taskCancellationTimeout;

    /**
     * This class loader should be set as the context class loader for threads that may dynamically
     * load user code.
     */
    private UserCodeClassLoader userCodeClassLoader;

    // ★ 构造函数刻意是"纯准备、无副作用"的：invokable 在这里【不】创建，
    //   网络 reader/writer 只建对象不做 setup，执行线程只 new 不 start。
    //   原因是下面 javadoc 说的：一旦部署失败，TM 没有任何机会回滚构造期做过的事。
    /**
     * <b>IMPORTANT:</b> This constructor may not start any work that would need to be undone in the
     * case of a failing task deployment.
     */
    public Task(
            JobInformation jobInformation,
            TaskInformation taskInformation,
            ExecutionAttemptID executionAttemptID,
            AllocationID slotAllocationId,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
            MemoryManager memManager,
            SharedResources sharedResources,
            IOManager ioManager,
            ShuffleEnvironment<?, ?> shuffleEnvironment,
            KvStateService kvStateService,
            BroadcastVariableManager bcVarManager,
            TaskEventDispatcher taskEventDispatcher,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            TaskStateManager taskStateManager,
            TaskManagerActions taskManagerActions,
            InputSplitProvider inputSplitProvider,
            CheckpointResponder checkpointResponder,
            TaskOperatorEventGateway operatorCoordinatorEventGateway,
            GlobalAggregateManager aggregateManager,
            LibraryCacheManager.ClassLoaderHandle classLoaderHandle,
            FileCache fileCache,
            TaskManagerRuntimeInfo taskManagerConfig,
            @Nonnull TaskMetricGroup metricGroup,
            PartitionProducerStateChecker partitionProducerStateChecker,
            Executor executor,
            ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory) {

        Preconditions.checkNotNull(jobInformation);
        Preconditions.checkNotNull(taskInformation);
        this.jobInfo = new JobInfoImpl(jobInformation.getJobId(), jobInformation.getJobName());
        this.taskInfo =
                new TaskInfoImpl(
                        taskInformation.getTaskName(),
                        taskInformation.getMaxNumberOfSubtasks(),
                        executionAttemptID.getSubtaskIndex(),
                        taskInformation.getNumberOfSubtasks(),
                        executionAttemptID.getAttemptNumber(),
                        String.valueOf(slotAllocationId));

        this.jobId = jobInformation.getJobId();
        this.jobType = jobInformation.getJobType();
        this.vertexId = taskInformation.getJobVertexId();
        this.executionId = Preconditions.checkNotNull(executionAttemptID);
        this.allocationId = Preconditions.checkNotNull(slotAllocationId);
        this.taskNameWithSubtask = taskInfo.getTaskNameWithSubtasks();
        this.jobConfiguration = jobInformation.getJobConfiguration();
        this.taskConfiguration = taskInformation.getTaskConfiguration();
        this.requiredJarFiles = jobInformation.getRequiredJarFileBlobKeys();
        this.requiredClasspaths = jobInformation.getRequiredClasspathURLs();
        this.nameOfInvokableClass = taskInformation.getInvokableClassName();
        this.serializedExecutionConfig = jobInformation.getSerializedExecutionConfig();

        Configuration tmConfig = taskManagerConfig.getConfiguration();
        this.taskCancellationInterval =
                tmConfig.get(TaskManagerOptions.TASK_CANCELLATION_INTERVAL).toMillis();
        this.taskCancellationTimeout =
                tmConfig.get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT).toMillis();

        this.memoryManager = Preconditions.checkNotNull(memManager);
        this.sharedResources = Preconditions.checkNotNull(sharedResources);
        this.ioManager = Preconditions.checkNotNull(ioManager);
        this.broadcastVariableManager = Preconditions.checkNotNull(bcVarManager);
        this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
        this.taskStateManager = Preconditions.checkNotNull(taskStateManager);
        this.accumulatorRegistry = new AccumulatorRegistry(jobId, executionId);

        this.inputSplitProvider = Preconditions.checkNotNull(inputSplitProvider);
        this.checkpointResponder = Preconditions.checkNotNull(checkpointResponder);
        this.operatorCoordinatorEventGateway =
                Preconditions.checkNotNull(operatorCoordinatorEventGateway);
        this.aggregateManager = Preconditions.checkNotNull(aggregateManager);
        this.taskManagerActions = checkNotNull(taskManagerActions);
        this.externalResourceInfoProvider = checkNotNull(externalResourceInfoProvider);

        this.classLoaderHandle = Preconditions.checkNotNull(classLoaderHandle);
        this.fileCache = Preconditions.checkNotNull(fileCache);
        this.kvStateService = Preconditions.checkNotNull(kvStateService);
        this.taskManagerConfig = Preconditions.checkNotNull(taskManagerConfig);

        this.metrics = metricGroup;

        this.partitionProducerStateChecker =
                Preconditions.checkNotNull(partitionProducerStateChecker);
        this.executor = Preconditions.checkNotNull(executor);
        this.channelStateExecutorFactory = channelStateExecutorFactory;

        // create the reader and writer structures

        final String taskNameWithSubtaskAndId = taskNameWithSubtask + " (" + executionId + ')';

        final ShuffleIOOwnerContext taskShuffleContext =
                shuffleEnvironment.createShuffleIOOwnerContext(
                        taskNameWithSubtaskAndId, executionId, metrics.getIOMetricGroup());

        // produced intermediate result partitions
        final ResultPartitionWriter[] resultPartitionWriters =
                shuffleEnvironment
                        .createResultPartitionWriters(
                                taskShuffleContext, resultPartitionDeploymentDescriptors)
                        .toArray(new ResultPartitionWriter[] {});

        this.partitionWriters = resultPartitionWriters;

        // consumed intermediate result partitions
        final IndexedInputGate[] gates =
                shuffleEnvironment
                        .createInputGates(taskShuffleContext, this, inputGateDeploymentDescriptors)
                        .toArray(new IndexedInputGate[0]);

        this.inputGates = new IndexedInputGate[gates.length];
        int counter = 0;
        for (IndexedInputGate gate : gates) {
            inputGates[counter++] =
                    new InputGateWithMetrics(
                            gate, metrics.getIOMetricGroup().getNumBytesInCounter());
        }

        if (shuffleEnvironment instanceof NettyShuffleEnvironment) {
            //noinspection deprecation
            ((NettyShuffleEnvironment) shuffleEnvironment)
                    .registerLegacyNetworkMetrics(
                            metrics.getIOMetricGroup(), resultPartitionWriters, gates);
        }

        invokableHasBeenCanceled = new AtomicBoolean(false);

        // ★ 只创建线程、不启动：startTaskThread() 由 TaskExecutor.submitTask() 在
        //   taskSlotTable.addTask(task) 成功之后才调用，保证 slot 账本先登记好，
        //   线程不会跑在一个"账本上还不存在"的 slot 上。
        // finally, create the executing thread, but do not start it
        executingThread = new Thread(TASK_THREADS_GROUP, this, taskNameWithSubtask);
    }

    // ------------------------------------------------------------------------
    //  Accessors
    // ------------------------------------------------------------------------

    @Override
    public JobID getJobID() {
        return jobId;
    }

    public JobVertexID getJobVertexId() {
        return vertexId;
    }

    @Override
    public ExecutionAttemptID getExecutionId() {
        return executionId;
    }

    @Override
    public AllocationID getAllocationId() {
        return allocationId;
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public Configuration getJobConfiguration() {
        return jobConfiguration;
    }

    public Configuration getTaskConfiguration() {
        return this.taskConfiguration;
    }

    public AccumulatorRegistry getAccumulatorRegistry() {
        return accumulatorRegistry;
    }

    public TaskMetricGroup getMetricGroup() {
        return metrics;
    }

    public Thread getExecutingThread() {
        return executingThread;
    }

    @Override
    public CompletableFuture<ExecutionState> getTerminationFuture() {
        return terminationFuture;
    }

    @VisibleForTesting
    long getTaskCancellationInterval() {
        return taskCancellationInterval;
    }

    @VisibleForTesting
    long getTaskCancellationTimeout() {
        return taskCancellationTimeout;
    }

    @Nullable
    @VisibleForTesting
    TaskInvokable getInvokable() {
        return invokable;
    }

    public boolean isBackPressured() {
        if (invokable == null
                || partitionWriters.length == 0
                || (executionState != ExecutionState.INITIALIZING
                        && executionState != ExecutionState.RUNNING)) {
            return false;
        }
        for (int i = 0; i < partitionWriters.length; ++i) {
            if (!partitionWriters[i].isAvailable()) {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------------
    //  Task Execution
    // ------------------------------------------------------------------------

    /**
     * Returns the current execution state of the task.
     *
     * @return The current execution state of the task.
     */
    public ExecutionState getExecutionState() {
        return this.executionState;
    }

    /**
     * Checks whether the task has failed, is canceled, or is being canceled at the moment.
     *
     * @return True is the task in state FAILED, CANCELING, or CANCELED, false otherwise.
     */
    public boolean isCanceledOrFailed() {
        return executionState == ExecutionState.CANCELING
                || executionState == ExecutionState.CANCELED
                || executionState == ExecutionState.FAILED;
    }

    /**
     * If the task has failed, this method gets the exception that caused this task to fail.
     * Otherwise this method returns null.
     *
     * @return The exception that caused the task to fail, or null, if the task has not failed.
     */
    public Throwable getFailureCause() {
        return failureCause;
    }

    // ★ 状态机（每个 Task 只有一个 ExecutionState，靠 CAS 原子推进，见 transitionState）：
    //     CREATED → DEPLOYING → INITIALIZING → RUNNING → FINISHED
    //        └────────┴───────────┴──────────────┴─────→ FAILED（任何状态都可能直接到 FAILED）
    //     CANCELING → CANCELED（cancelExecution 走这条路径）
    //   Task 自己的视角只从 CREATED 开始（更早的 SCHEDULED 属于 JobMaster 的 Execution）；
    //   CANCELING / FAILED 可能被外部线程（TM 的 cancelTask、failExternally）抢先设置，
    //   因此 run() 一开始必须先"认领"当前状态，抢不到就立刻走终止流程（见 doRun 开头）。
    /** Starts the task's thread. */
    public void startTaskThread() {
        executingThread.start();
    }

    // ★★ 任务线程的主入口：由 TaskExecutor.submitTask() → startTaskThread() 拉起。
    //   本方法只做两件事：补上 job 的 MDC 日志上下文，并保证 terminationFuture 一定完成
    //   （TM/JM 用它作为"这次 attempt 已收尾、可以回收结果分区"的收尾信号）。
    //   真正的逻辑都在 doRun()，主线如下（对应 doRun 里的分段注释）：
    //     第 1 步 抢占初始状态 CREATED → DEPLOYING；抢不到说明已被外部取消/失败，直接结束；
    //     第 2 步 准备用户类加载器（可能要下载 job 的 jar）、注册网络分区与输入门、分发缓存文件；
    //     第 3 步 构造 RuntimeEnvironment，反射实例化 invokable（流式作业即 StreamTask）；
    //     第 4 步 restoreAndInvoke()：INITIALIZING → restore() → RUNNING → invoke() ★ 用户代码
    //     第 5 步 正常返回：finish() 所有产出分区，并把状态推进到 FINISHED；
    //     第 6 步 任何异常进 catch：CancelTaskException 转 CANCELED，其余转 FAILED 并记录 failureCause；
    //     第 7 步 finally 释放网络/内存/缓存资源，notifyFinalState() 把终态上报给 TM。
    /** The core work method that bootstraps the task and executes its code. */
    @Override
    public void run() {
        try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
            doRun();
        } finally {
            terminationFuture.complete(executionState);
        }
    }

    private void doRun() {
        // ----------------------------
        //  Initial State transition
        // ----------------------------
        while (true) {
            ExecutionState current = this.executionState;
            if (current == ExecutionState.CREATED) {
                if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
                    // success, we can start our work
                    break;
                }
            } else if (current == ExecutionState.FAILED) {
                // we were immediately failed. tell the TaskManager that we reached our final state
                notifyFinalState();
                if (metrics != null) {
                    metrics.close();
                }
                return;
            } else if (current == ExecutionState.CANCELING) {
                if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
                    // we were immediately canceled. tell the TaskManager that we reached our final
                    // state
                    notifyFinalState();
                    if (metrics != null) {
                        metrics.close();
                    }
                    return;
                }
            } else {
                if (metrics != null) {
                    metrics.close();
                }
                throw new IllegalStateException(
                        "Invalid state for beginning of operation of task " + this + '.');
            }
        }

        // all resource acquisitions and registrations from here on
        // need to be undone in the end
        Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
        TaskInvokable invokable = null;

        try {
            // ----------------------------
            //  Task Bootstrap - We periodically
            //  check for canceling as a shortcut
            // ----------------------------

            // activate safety net for task thread
            LOG.debug("Creating FileSystem stream leak safety net for task {}", this);
            FileSystemSafetyNet.initializeSafetyNetForThread();

            // first of all, get a user-code classloader
            // this may involve downloading the job's JAR files and/or classes
            LOG.info("Loading JAR files for task {}.", this);

            userCodeClassLoader = createUserCodeClassloader();
            final ExecutionConfig executionConfig =
                    serializedExecutionConfig.deserializeValue(userCodeClassLoader.asClassLoader());
            Configuration executionConfigConfiguration = executionConfig.toConfiguration();

            // override task cancellation interval from Flink config if set in ExecutionConfig
            taskCancellationInterval =
                    executionConfigConfiguration
                            .getOptional(TaskManagerOptions.TASK_CANCELLATION_INTERVAL)
                            .orElse(Duration.ofMillis(taskCancellationInterval))
                            .toMillis();

            // override task cancellation timeout from Flink config if set in ExecutionConfig
            taskCancellationTimeout =
                    executionConfigConfiguration
                            .getOptional(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT)
                            .orElse(Duration.ofMillis(taskCancellationTimeout))
                            .toMillis();

            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            // register the task with the network stack
            // this operation may fail if the system does not have enough
            // memory to run the necessary data exchanges
            // the registration must also strictly be undone
            // ----------------------------------------------------------------

            LOG.debug("Registering task at network: {}.", this);

            setupPartitionsAndGates(partitionWriters, inputGates);

            for (ResultPartitionWriter partitionWriter : partitionWriters) {
                taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
            }

            // next, kick off the background copying of files for the distributed cache
            try {
                for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                        DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
                    LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
                    Future<Path> cp =
                            fileCache.createTmpFile(
                                    entry.getKey(), entry.getValue(), jobId, executionId);
                    distributedCacheEntries.put(entry.getKey(), cp);
                }
            } catch (Exception e) {
                throw new Exception(
                        String.format(
                                "Exception while adding files to distributed cache of task %s (%s).",
                                taskNameWithSubtask, executionId),
                        e);
            }

            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            //  call the user code initialization methods
            // ----------------------------------------------------------------

            TaskKvStateRegistry kvStateRegistry =
                    kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());

            Environment env =
                    new RuntimeEnvironment(
                            jobId,
                            jobType,
                            vertexId,
                            executionId,
                            executionConfig,
                            jobInfo,
                            taskInfo,
                            jobConfiguration,
                            taskConfiguration,
                            userCodeClassLoader,
                            memoryManager,
                            sharedResources,
                            ioManager,
                            broadcastVariableManager,
                            taskStateManager,
                            aggregateManager,
                            accumulatorRegistry,
                            kvStateRegistry,
                            inputSplitProvider,
                            distributedCacheEntries,
                            partitionWriters,
                            inputGates,
                            taskEventDispatcher,
                            checkpointResponder,
                            operatorCoordinatorEventGateway,
                            taskManagerConfig,
                            metrics,
                            this,
                            externalResourceInfoProvider,
                            channelStateExecutorFactory,
                            taskManagerActions);

            // Make sure the user code classloader is accessible thread-locally.
            // We are setting the correct context class loader before instantiating the invokable
            // so that it is available to the invokable during its entire lifetime.
            executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

            // When constructing invokable, separate threads can be constructed and thus should be
            // monitored for system exit (in addition to invoking thread itself monitored below).
            FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
            try {
                // now load and instantiate the task's invokable code
                invokable =
                        loadAndInstantiateInvokable(
                                userCodeClassLoader.asClassLoader(), nameOfInvokableClass, env);
            } finally {
                FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
            }

            // ----------------------------------------------------------------
            //  actual task core work
            // ----------------------------------------------------------------

            // we must make strictly sure that the invokable is accessible to the cancel() call
            // by the time we switched to running.
            this.invokable = invokable;

            // ★ 到这一步用户代码才真正开跑：restoreAndInvoke 内部依次完成
            //   restore()（恢复状态，状态转 INITIALIZING）→ invoke()（一直跑到数据读完或作业被取消，
            //   状态转 RUNNING）。每次状态切换都会经 taskManagerActions.updateTaskExecutionState
            //   上报给 TM，再由 TM 转发给 JM——这就是"Task 状态如何回到 JobMaster"的完整路径。
            restoreAndInvoke(invokable);

            // make sure, we enter the catch block if the task leaves the invoke() method due
            // to the fact that it has been canceled
            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            //  finalization of a successful execution
            // ----------------------------------------------------------------

            // finish the produced partitions. if this fails, we consider the execution failed.
            for (ResultPartitionWriter partitionWriter : partitionWriters) {
                if (partitionWriter != null) {
                    partitionWriter.finish();
                }
            }

            // try to mark the task as finished
            // if that fails, the task was canceled/failed in the meantime
            if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
                throw new CancelTaskException();
            }
        } catch (Throwable t) {
            // ----------------------------------------------------------------
            // the execution failed. either the invokable code properly failed, or
            // an exception was thrown as a side effect of cancelling
            // ----------------------------------------------------------------

            t = preProcessException(t);

            try {
                // transition into our final state. we should be either in DEPLOYING, INITIALIZING,
                // RUNNING, CANCELING, or FAILED
                // loop for multiple retries during concurrent state changes via calls to cancel()
                // or to failExternally()
                while (true) {
                    ExecutionState current = this.executionState;

                    if (current == ExecutionState.RUNNING
                            || current == ExecutionState.INITIALIZING
                            || current == ExecutionState.DEPLOYING) {
                        if (ExceptionUtils.findThrowable(t, CancelTaskException.class)
                                .isPresent()) {
                            if (transitionState(current, ExecutionState.CANCELED, t)) {
                                cancelInvokable(invokable);
                                break;
                            }
                        } else {
                            if (transitionState(current, ExecutionState.FAILED, t)) {
                                cancelInvokable(invokable);
                                break;
                            }
                        }
                    } else if (current == ExecutionState.CANCELING) {
                        if (transitionState(current, ExecutionState.CANCELED)) {
                            break;
                        }
                    } else if (current == ExecutionState.FAILED) {
                        // in state failed already, no transition necessary any more
                        break;
                    }
                    // unexpected state, go to failed
                    else if (transitionState(current, ExecutionState.FAILED, t)) {
                        LOG.error(
                                "Unexpected state in task {} ({}) during an exception: {}.",
                                taskNameWithSubtask,
                                executionId,
                                current);
                        break;
                    }
                    // else fall through the loop and
                }
            } catch (Throwable tt) {
                String message =
                        String.format(
                                "FATAL - exception in exception handler of task %s (%s).",
                                taskNameWithSubtask, executionId);
                LOG.error(message, tt);
                notifyFatalError(message, tt);
            }
        } finally {
            try {
                LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);

                // clear the reference to the invokable. this helps guard against holding references
                // to the invokable and its structures in cases where this Task object is still
                // referenced
                this.invokable = null;

                // free the network resources
                releaseResources();

                // free memory resources
                if (invokable != null) {
                    memoryManager.releaseAll(invokable);
                }

                // remove all of the tasks resources
                fileCache.releaseJob(jobId, executionId);

                // close and de-activate safety net for task thread
                LOG.debug("Ensuring all FileSystem streams are closed for task {}", this);
                FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

                notifyFinalState();
            } catch (Throwable t) {
                // an error in the resource cleanup is fatal
                String message =
                        String.format(
                                "FATAL - exception in resource cleanup of task %s (%s).",
                                taskNameWithSubtask, executionId);
                LOG.error(message, t);
                notifyFatalError(message, t);
            }

            // un-register the metrics at the end so that the task may already be
            // counted as finished when this happens
            // errors here will only be logged
            try {
                metrics.close();
            } catch (Throwable t) {
                LOG.error(
                        "Error during metrics de-registration of task {} ({}).",
                        taskNameWithSubtask,
                        executionId,
                        t);
            }
        }
    }

    // 异常进入状态机之前先做两件"预处理"：
    //   1) 剥掉框架包装异常（WrappingRuntimeException），让用户看到的栈更干净；
    //   2) ★ JVM 级致命错误或 OOM 直接 halt JVM——此时 TM 的内存/状态已不可信，
    //      与其带着半死不活的 TM 继续被调度，不如让 YARN/K8s 直接重启整个容器。
    /** Unwrap, enrich and handle fatal errors. */
    private Throwable preProcessException(Throwable t) {
        // unwrap wrapped exceptions to make stack traces more compact
        if (t instanceof WrappingRuntimeException) {
            t = ((WrappingRuntimeException) t).unwrap();
        }

        TaskManagerExceptionUtils.tryEnrichTaskManagerError(t);

        // check if the exception is unrecoverable
        if (ExceptionUtils.isJvmFatalError(t)
                || (t instanceof OutOfMemoryError
                        && taskManagerConfig.shouldExitJvmOnOutOfMemoryError())) {

            // terminate the JVM immediately
            // don't attempt a clean shutdown, because we cannot expect the clean shutdown
            // to complete
            try {
                LOG.error(
                        "Encountered fatal error {} - terminating the JVM",
                        t.getClass().getName(),
                        t);
            } finally {
                Runtime.getRuntime().halt(-1);
            }
        }

        return t;
    }

    // ★ 状态推进与用户代码调用的绑定点，也是 Task → StreamTask 的唯一入口：
    //   DEPLOYING → INITIALIZING（上报）→ invokable.restore() → RUNNING（上报）→ invokable.invoke()。
    //   任何一步抛出异常都从这里抛回 doRun 的 catch，由它决定最终是 CANCELED 还是 FAILED；
    //   无论成功失败，cleanUp 都恰好被调用一次（失败路径会把异常一并传进去）。
    private void restoreAndInvoke(TaskInvokable finalInvokable) throws Exception {
        try {
            // switch to the INITIALIZING state, if that fails, we have been canceled/failed in the
            // meantime
            if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.INITIALIZING)) {
                throw new CancelTaskException();
            }

            taskManagerActions.updateTaskExecutionState(
                    new TaskExecutionState(executionId, ExecutionState.INITIALIZING));

            // make sure the user code classloader is accessible thread-locally
            executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

            runWithSystemExitMonitoring(finalInvokable::restore);

            if (!transitionState(ExecutionState.INITIALIZING, ExecutionState.RUNNING)) {
                throw new CancelTaskException();
            }

            // notify everyone that we switched to running
            taskManagerActions.updateTaskExecutionState(
                    new TaskExecutionState(executionId, ExecutionState.RUNNING));

            runWithSystemExitMonitoring(finalInvokable::invoke);
        } catch (Throwable throwable) {
            try {
                runWithSystemExitMonitoring(() -> finalInvokable.cleanUp(throwable));
            } catch (Throwable cleanUpThrowable) {
                throwable.addSuppressed(cleanUpThrowable);
            }
            throw throwable;
        }
        runWithSystemExitMonitoring(() -> finalInvokable.cleanUp(null));
    }

    /**
     * Monitor user codes from exiting JVM covering user function invocation. This can be done in a
     * finer-grained way like enclosing user callback functions individually, but as exit triggered
     * by framework is not performed and expected in this invoke function anyhow, we can monitor
     * exiting JVM for entire scope.
     */
    private void runWithSystemExitMonitoring(RunnableWithException action) throws Exception {
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            action.run();
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    @VisibleForTesting
    public static void setupPartitionsAndGates(
            ResultPartitionWriter[] producedPartitions, InputGate[] inputGates) throws IOException {

        for (ResultPartitionWriter partition : producedPartitions) {
            partition.setup();
        }

        // InputGates must be initialized after the partitions, since during InputGate#setup
        // we are requesting partitions
        for (InputGate gate : inputGates) {
            gate.setup();
        }
    }

    // 释放资源必须区分任务结局：
    //   ✔ 正常结束 → 只 close 分区，让下游还能把已经写好的数据读完；
    //   ✘ 取消/失败 → 先 fail 所有产出分区，主动通知下游"别等了，这批数据不完整"，
    //     否则下游可能一直阻塞在等这个分区的数据上（表现为作业卡住不结束）。
    /**
     * Releases resources before task exits. We should also fail the partition to release if the
     * task has failed, is canceled, or is being canceled at the moment.
     */
    private void releaseResources() {
        LOG.debug(
                "Release task {} network resources (state: {}).",
                taskNameWithSubtask,
                getExecutionState());

        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            taskEventDispatcher.unregisterPartition(partitionWriter.getPartitionId());
        }

        // close network resources
        if (isCanceledOrFailed()) {
            failAllResultPartitions();
        }
        closeAllResultPartitions();
        closeAllInputGates();

        try {
            taskStateManager.close();
        } catch (Exception e) {
            LOG.error("Failed to close task state manager for task {}.", taskNameWithSubtask, e);
        }
    }

    private void failAllResultPartitions() {
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            partitionWriter.fail(getFailureCause());
        }
    }

    private void closeAllResultPartitions() {
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            try {
                partitionWriter.close();
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                LOG.error(
                        "Failed to release result partition for task {}.", taskNameWithSubtask, t);
            }
        }
    }

    private void closeAllInputGates() {
        TaskInvokable invokable = this.invokable;
        if (invokable == null || !invokable.isUsingNonBlockingInput()) {
            // Cleanup resources instead of invokable if it is null, or prevent it from being
            // blocked on input, or interrupt if it is already blocked. Not needed for StreamTask
            // (which does NOT use blocking input); for which this could cause race conditions
            for (InputGate inputGate : inputGates) {
                try {
                    inputGate.close();
                } catch (Throwable t) {
                    ExceptionUtils.rethrowIfFatalError(t);
                    LOG.error("Failed to release input gate for task {}.", taskNameWithSubtask, t);
                }
            }
        }
    }

    private UserCodeClassLoader createUserCodeClassloader() throws Exception {
        long startDownloadTime = System.currentTimeMillis();

        // triggers the download of all missing jar files from the job manager
        final UserCodeClassLoader userCodeClassLoader =
                classLoaderHandle.getOrResolveClassLoader(requiredJarFiles, requiredClasspaths);

        LOG.debug(
                "Getting user code class loader for task {} at library cache manager took {} milliseconds",
                executionId,
                System.currentTimeMillis() - startDownloadTime);

        return userCodeClassLoader;
    }

    // ★ 状态上报的终点：Task 只在自己线程里"推"状态给 TM，TM 再转发给 JobMaster
    //   （方向固定：Task → TaskManagerActions → TaskExecutor → JobMaster）。
    //   ★ Task 从不主动向 JM 拉取信息，也完全不感知 JM 换了 leader——
    //     连接层面的换主由 TaskExecutor 兜住（旧连接断开时 Task 会被 failExternally）。
    private void notifyFinalState() {
        checkState(executionState.isTerminal());
        taskManagerActions.updateTaskExecutionState(
                new TaskExecutionState(executionId, executionState, failureCause));
    }

    private void notifyFatalError(String message, Throwable cause) {
        taskManagerActions.notifyFatalError(message, cause);
    }

    // ★ 用 AtomicReferenceFieldUpdater 做 CAS 状态推进：这是"外部线程取消/失败"与
    //   "任务线程自己推进"之间的唯一同步点。CAS 失败就意味着状态已被并发改掉，
    //   调用方必须据此放弃当前动作（例如放弃 finish，改走 CANCELED 分支），绝不能强行覆盖。
    /**
     * Try to transition the execution state from the current state to the new state.
     *
     * @param currentState of the execution
     * @param newState of the execution
     * @return true if the transition was successful, otherwise false
     */
    private boolean transitionState(ExecutionState currentState, ExecutionState newState) {
        return transitionState(currentState, newState, null);
    }

    /**
     * Try to transition the execution state from the current state to the new state.
     *
     * @param currentState of the execution
     * @param newState of the execution
     * @param cause of the transition change or null
     * @return true if the transition was successful, otherwise false
     */
    private boolean transitionState(
            ExecutionState currentState, ExecutionState newState, Throwable cause) {
        if (STATE_UPDATER.compareAndSet(this, currentState, newState)) {
            if (cause == null) {
                LOG.info(
                        "{} ({}) switched from {} to {}.",
                        taskNameWithSubtask,
                        executionId,
                        currentState,
                        newState);
            } else if (ExceptionUtils.findThrowable(cause, CancelTaskException.class).isPresent()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "{} ({}) switched from {} to {} due to CancelTaskException:",
                            taskNameWithSubtask,
                            executionId,
                            currentState,
                            newState,
                            cause);
                } else {
                    LOG.info(
                            "{} ({}) switched from {} to {} due to CancelTaskException.",
                            taskNameWithSubtask,
                            executionId,
                            currentState,
                            newState);
                }
            } else {
                // proper failure of the task. record the exception as the root
                // cause
                failureCause = cause;
                LOG.warn(
                        "{} ({}) switched from {} to {} with failure cause:",
                        taskNameWithSubtask,
                        executionId,
                        currentState,
                        newState,
                        cause);
            }

            return true;
        } else {
            return false;
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    //  Canceling / Failing the task from the outside
    // ----------------------------------------------------------------------------------------------------------------

    // ★ 取消（cancel）与失败（failExternally）的区别：
    //   cancelExecution() 走 CANCELING → CANCELED，语义是"这个结果我们不要了"（用户 stop、
    //   JM 主动取消、上游失败导致的连带取消），不算错误，failureCause 保持为 null；
    //   failExternally() 直接进 FAILED，语义是"这次执行出错了"，必须携带 cause。
    //   两者都只是给状态"打标记"并异步去中断用户代码，方法本身不阻塞，见下面的内部实现。
    /**
     * Cancels the task execution. If the task is already in a terminal state (such as FINISHED,
     * CANCELED, FAILED), or if the task is already canceling this does nothing. Otherwise it sets
     * the state to CANCELING, and, if the invokable code is running, starts an asynchronous thread
     * that aborts that code.
     *
     * <p>This method never blocks.
     */
    public void cancelExecution() {
        try (MdcUtils.MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
            LOG.info("Attempting to cancel task {} ({}).", taskNameWithSubtask, executionId);
            cancelOrFailAndCancelInvokable(ExecutionState.CANCELING, null);
        }
    }

    // ★ 为什么必须有"外部失败"这条路径：很多失败原因根本不在本次执行的代码里，而在 Task 之外——
    //   例如 JM 断连（TaskExecutor.disconnectJobManagerConnection 会批量调用它）、
    //   上游分区数据不可用（updatePartitions 失败）、算子事件投递失败、checkpoint 触发失败等。
    //   这些场景下 Task 线程往往正阻塞在用户代码里（读数据、等锁），自己无法感知，
    //   只能由 TM 线程替它把状态置为 FAILED 并中断它，从而让 JM 尽快重新调度。
    /**
     * Marks task execution failed for an external reason (a reason other than the task code itself
     * throwing an exception). If the task is already in a terminal state (such as FINISHED,
     * CANCELED, FAILED), or if the task is already canceling this does nothing. Otherwise it sets
     * the state to FAILED, and, if the invokable code is running, starts an asynchronous thread
     * that aborts that code.
     *
     * <p>This method never blocks.
     */
    @Override
    public void failExternally(Throwable cause) {
        try (MdcUtils.MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
            LOG.info(
                    "Attempting to fail task externally {} ({}).",
                    taskNameWithSubtask,
                    executionId);
            cancelOrFailAndCancelInvokable(ExecutionState.FAILED, cause);
        }
    }

    private void cancelOrFailAndCancelInvokable(ExecutionState targetState, Throwable cause) {
        try {
            cancelOrFailAndCancelInvokableInternal(targetState, cause);
        } catch (Throwable t) {
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(t)) {
                String message =
                        String.format(
                                "FATAL - exception in cancelling task %s (%s).",
                                taskNameWithSubtask, executionId);
                notifyFatalError(message, t);
            } else {
                throw t;
            }
        }
    }

    // 取消 / 失败的公共实现，一共三件事：
    //   1) 已经是终态或已在 CANCELING → 直接返回（幂等，重复取消是正常现象）；
    //   2) CREATED / DEPLOYING → 只改状态即可，invokable 还没被调用过，不需要取消它；
    //   3) INITIALIZING / RUNNING → 先改状态，再起【独立线程】去调 invokable.cancel()。
    // ★ 第 3 点为什么必须另起线程：用户代码的 cancel 可能阻塞（例如卡在 I/O 或锁上），
    //   若在调用方线程（常常是 TM 的 RPC 主线程）里直接调用，会把整个 TM 的 RPC 处理拖死。
    @VisibleForTesting
    void cancelOrFailAndCancelInvokableInternal(ExecutionState targetState, Throwable cause) {
        cause = preProcessException(cause);

        while (true) {
            ExecutionState current = executionState;

            // if the task is already canceled (or canceling) or finished or failed,
            // then we need not do anything
            if (current.isTerminal() || current == ExecutionState.CANCELING) {
                LOG.info("Task {} is already in state {}", taskNameWithSubtask, current);
                return;
            }

            if (current == ExecutionState.DEPLOYING || current == ExecutionState.CREATED) {
                if (transitionState(current, targetState, cause)) {
                    // if we manage this state transition, then the invokable gets never called
                    // we need not call cancel on it
                    return;
                }
            } else if (current == ExecutionState.INITIALIZING
                    || current == ExecutionState.RUNNING) {
                if (transitionState(current, targetState, cause)) {
                    // we are canceling / failing out of the running state
                    // we need to cancel the invokable

                    // copy reference to guard against concurrent null-ing out the reference
                    final TaskInvokable invokable = this.invokable;

                    if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
                        LOG.info(
                                "Triggering cancellation of task code {} ({}).",
                                taskNameWithSubtask,
                                executionId);

                        // because the canceling may block on user code, we cancel from a separate
                        // thread
                        // we do not reuse the async call handler, because that one may be blocked,
                        // in which
                        // case the canceling could not continue

                        // The canceller calls cancel and interrupts the executing thread once
                        Runnable canceler =
                                new TaskCanceler(
                                        LOG, invokable, executingThread, taskNameWithSubtask);

                        Thread cancelThread =
                                new Thread(
                                        executingThread.getThreadGroup(),
                                        canceler,
                                        String.format(
                                                "Canceler for %s (%s).",
                                                taskNameWithSubtask, executionId));
                        cancelThread.setDaemon(true);
                        cancelThread.setUncaughtExceptionHandler(
                                FatalExitExceptionHandler.INSTANCE);
                        cancelThread.start();

                        // the periodic interrupting thread - a different thread than the canceller,
                        // in case
                        // the application code does blocking stuff in its cancellation paths.
                        Runnable interrupter =
                                new TaskInterrupter(
                                        LOG,
                                        invokable,
                                        executingThread,
                                        taskNameWithSubtask,
                                        taskCancellationInterval,
                                        jobId);

                        Thread interruptingThread =
                                new Thread(
                                        executingThread.getThreadGroup(),
                                        interrupter,
                                        String.format(
                                                "Canceler/Interrupts for %s (%s).",
                                                taskNameWithSubtask, executionId));
                        interruptingThread.setDaemon(true);
                        interruptingThread.setUncaughtExceptionHandler(
                                FatalExitExceptionHandler.INSTANCE);
                        interruptingThread.start();

                        // if a cancellation timeout is set, the watchdog thread kills the process
                        // if graceful cancellation does not succeed
                        if (taskCancellationTimeout > 0) {
                            Runnable cancelWatchdog =
                                    new TaskCancelerWatchDog(
                                            taskInfo,
                                            executingThread,
                                            taskManagerActions,
                                            taskCancellationTimeout,
                                            jobId);

                            Thread watchDogThread =
                                    new Thread(
                                            executingThread.getThreadGroup(),
                                            cancelWatchdog,
                                            String.format(
                                                    "Cancellation Watchdog for %s (%s).",
                                                    taskNameWithSubtask, executionId));
                            watchDogThread.setDaemon(true);
                            watchDogThread.setUncaughtExceptionHandler(
                                    FatalExitExceptionHandler.INSTANCE);
                            watchDogThread.start();
                        }
                    }
                    return;
                }
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Unexpected state: %s of task %s (%s).",
                                current, taskNameWithSubtask, executionId));
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Partition State Listeners
    // ------------------------------------------------------------------------

    @Override
    public void requestPartitionProducerState(
            final IntermediateDataSetID intermediateDataSetId,
            final ResultPartitionID resultPartitionId,
            Consumer<? super ResponseHandle> responseConsumer) {

        final CompletableFuture<ExecutionState> futurePartitionState =
                partitionProducerStateChecker.requestPartitionProducerState(
                        jobId, intermediateDataSetId, resultPartitionId);

        FutureUtils.assertNoException(
                futurePartitionState
                        .handle(PartitionProducerStateResponseHandle::new)
                        .thenAcceptAsync(responseConsumer, executor));
    }

    // ------------------------------------------------------------------------
    //  Notifications on the invokable
    // ------------------------------------------------------------------------

    // ★ 检查点（checkpoint）的触发方向是 JM → TM → Task → StreamTask：
    //   CheckpointCoordinator → JobMaster → TaskExecutor.triggerCheckpoint()（RPC）
    //     → 本方法 → CheckpointableTask.triggerCheckpointAsync()（投递进 mailbox 异步执行）。
    //   注意这里是【异步触发】：立即返回并不代表 checkpoint 已完成；
    //   无法触发（非 RUNNING、mailbox 已关）时必须调用 declineCheckpoint 主动拒绝，
    //   否则 CheckpointCoordinator 会一直等这个 subtask，导致检查点超时。
    /**
     * Calls the invokable to trigger a checkpoint.
     *
     * @param checkpointID The ID identifying the checkpoint.
     * @param checkpointTimestamp The timestamp associated with the checkpoint.
     * @param checkpointOptions Options for performing this checkpoint.
     */
    public void triggerCheckpointBarrier(
            final long checkpointID,
            final long checkpointTimestamp,
            final CheckpointOptions checkpointOptions) {

        final TaskInvokable invokable = this.invokable;
        final CheckpointMetaData checkpointMetaData =
                new CheckpointMetaData(
                        checkpointID, checkpointTimestamp, System.currentTimeMillis());

        if (executionState == ExecutionState.RUNNING) {
            checkState(invokable instanceof CheckpointableTask, "invokable is not checkpointable");
            try {
                ((CheckpointableTask) invokable)
                        .triggerCheckpointAsync(checkpointMetaData, checkpointOptions)
                        .handle(
                                (triggerResult, exception) -> {
                                    if (exception != null || !triggerResult) {
                                        declineCheckpoint(
                                                checkpointID,
                                                CheckpointFailureReason.TASK_FAILURE,
                                                exception);
                                        return false;
                                    }
                                    return true;
                                });
            } catch (RejectedExecutionException ex) {
                // This may happen if the mailbox is closed. It means that the task is shutting
                // down, so we just ignore it.
                LOG.debug(
                        "Triggering checkpoint {} for {} ({}) was rejected by the mailbox",
                        checkpointID,
                        taskNameWithSubtask,
                        executionId);
                declineCheckpoint(
                        checkpointID, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING);
            } catch (Throwable t) {
                if (getExecutionState() == ExecutionState.RUNNING) {
                    failExternally(
                            new Exception(
                                    "Error while triggering checkpoint "
                                            + checkpointID
                                            + " for "
                                            + taskNameWithSubtask,
                                    t));
                } else {
                    LOG.debug(
                            "Encountered error while triggering checkpoint {} for "
                                    + "{} ({}) while being not in state running.",
                            checkpointID,
                            taskNameWithSubtask,
                            executionId,
                            t);
                }
            }
        } else {
            LOG.debug(
                    "Declining checkpoint request for non-running task {} ({}).",
                    taskNameWithSubtask,
                    executionId);

            // send back a message that we did not do the checkpoint
            declineCheckpoint(
                    checkpointID, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY);
        }
    }

    private void declineCheckpoint(long checkpointID, CheckpointFailureReason failureReason) {
        declineCheckpoint(checkpointID, failureReason, null);
    }

    private void declineCheckpoint(
            long checkpointID,
            CheckpointFailureReason failureReason,
            @Nullable Throwable failureCause) {
        checkpointResponder.declineCheckpoint(
                jobId,
                executionId,
                checkpointID,
                new CheckpointException(
                        "Task name with subtask : " + taskNameWithSubtask,
                        failureReason,
                        failureCause));
    }

    public void notifyCheckpointComplete(final long checkpointID) {
        notifyCheckpoint(
                checkpointID,
                CheckpointStoreUtil.INVALID_CHECKPOINT_ID,
                NotifyCheckpointOperation.COMPLETE);
    }

    public void notifyCheckpointAborted(
            final long checkpointID, final long latestCompletedCheckpointId) {
        notifyCheckpoint(
                checkpointID, latestCompletedCheckpointId, NotifyCheckpointOperation.ABORT);
    }

    public void notifyCheckpointSubsumed(long checkpointID) {
        notifyCheckpoint(
                checkpointID,
                CheckpointStoreUtil.INVALID_CHECKPOINT_ID,
                NotifyCheckpointOperation.SUBSUME);
    }

    private void notifyCheckpoint(
            long checkpointId,
            long latestCompletedCheckpointId,
            NotifyCheckpointOperation notifyCheckpointOperation) {
        TaskInvokable invokable = this.invokable;

        if (executionState == ExecutionState.RUNNING && invokable != null) {
            checkState(invokable instanceof CheckpointableTask, "invokable is not checkpointable");
            try {
                switch (notifyCheckpointOperation) {
                    case ABORT:
                        ((CheckpointableTask) invokable)
                                .notifyCheckpointAbortAsync(
                                        checkpointId, latestCompletedCheckpointId);
                        break;
                    case COMPLETE:
                        ((CheckpointableTask) invokable)
                                .notifyCheckpointCompleteAsync(checkpointId);
                        break;
                    case SUBSUME:
                        ((CheckpointableTask) invokable)
                                .notifyCheckpointSubsumedAsync(checkpointId);
                }
            } catch (RejectedExecutionException ex) {
                // This may happen if the mailbox is closed. It means that the task is shutting
                // down, so we just ignore it.
                LOG.debug(
                        "Notify checkpoint {}} {} for {} ({}) was rejected by the mailbox.",
                        notifyCheckpointOperation,
                        checkpointId,
                        taskNameWithSubtask,
                        executionId);
            } catch (Throwable t) {
                switch (notifyCheckpointOperation) {
                    case ABORT:
                    case COMPLETE:
                        if (getExecutionState() == ExecutionState.RUNNING) {
                            failExternally(
                                    new RuntimeException(
                                            String.format(
                                                    "Error while notify checkpoint %s.",
                                                    notifyCheckpointOperation),
                                            t));
                        }
                        break;
                    case SUBSUME:
                        // just rethrow the throwable out as we do not expect notification of
                        // subsume could fail the task.
                        ExceptionUtils.rethrow(t);
                }
            }
        } else {
            LOG.info(
                    "Ignoring checkpoint {} notification for non-running task {}.",
                    notifyCheckpointOperation,
                    taskNameWithSubtask);
        }
    }

    /**
     * Dispatches an operator event to the invokable task.
     *
     * <p>If the event delivery did not succeed, this method throws an exception. Callers can use
     * that exception for error reporting, but need not react with failing this task (this method
     * takes care of that).
     *
     * @throws FlinkException This method throws exceptions indicating the reason why delivery did
     *     not succeed.
     */
    public void deliverOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> evt)
            throws FlinkException {
        final TaskInvokable invokable = this.invokable;
        final ExecutionState currentState = this.executionState;

        if (invokable == null
                || (currentState != ExecutionState.RUNNING
                        && currentState != ExecutionState.INITIALIZING)) {
            throw new TaskNotRunningException("Task is not running, but in state " + currentState);
        }

        if (invokable instanceof CoordinatedTask) {
            try {
                ((CoordinatedTask) invokable).dispatchOperatorEvent(operator, evt);
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                if (getExecutionState() == ExecutionState.RUNNING
                        || getExecutionState() == ExecutionState.INITIALIZING) {
                    FlinkException e = new FlinkException("Error while handling operator event", t);
                    failExternally(e);
                    throw e;
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    // 无论是正常结束、异常退出还是取消，最后都要在用户代码上调一次 cancel()，
    // 让算子有机会关闭自己的资源；invokableHasBeenCanceled 保证它不会被重复调用。
    private void cancelInvokable(TaskInvokable invokable) {
        // in case of an exception during execution, we still call "cancel()" on the task
        if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
            try {
                invokable.cancel();
            } catch (Throwable t) {
                LOG.error("Error while canceling task {}.", taskNameWithSubtask, t);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("%s (%s) [%s]", taskNameWithSubtask, executionId, executionState);
    }

    @VisibleForTesting
    class PartitionProducerStateResponseHandle implements ResponseHandle {
        private final Either<ExecutionState, Throwable> result;

        PartitionProducerStateResponseHandle(
                @Nullable ExecutionState producerState, @Nullable Throwable t) {
            this.result = producerState != null ? Either.Left(producerState) : Either.Right(t);
        }

        @Override
        public ExecutionState getConsumerExecutionState() {
            return executionState;
        }

        @Override
        public Either<ExecutionState, Throwable> getProducerExecutionState() {
            return result;
        }

        @Override
        public void cancelConsumption() {
            cancelExecution();
        }

        @Override
        public void failConsumption(Throwable cause) {
            failExternally(cause);
        }
    }

    // ★ 反射实例化的就是作业提交时写进 TaskInformation 的 invokableClass：
    //   流式作业是 StreamTask 的子类（由 StreamGraph 生成），批处理则是 BatchTask 等。
    //   "创建 StreamTask"这一步到此完成，真正的执行交给 restoreAndInvoke()；
    //   构造函数固定接收一个 Environment——它是用户代码与运行时之间的唯一契约
    //   （读输入、写输出、拿状态、发算子事件都通过它）。
    /**
     * Instantiates the given task invokable class, passing the given environment (and possibly the
     * initial task state) to the task's constructor.
     *
     * <p>The method will first try to instantiate the task via a constructor accepting both the
     * Environment and the TaskStateSnapshot. If no such constructor exists, and there is no initial
     * state, the method will fall back to the stateless convenience constructor that accepts only
     * the Environment.
     *
     * @param classLoader The classloader to load the class through.
     * @param className The name of the class to load.
     * @param environment The task environment.
     * @return The instantiated invokable task object.
     * @throws Throwable Forwards all exceptions that happen during initialization of the task. Also
     *     throws an exception if the task class misses the necessary constructor.
     */
    private static TaskInvokable loadAndInstantiateInvokable(
            ClassLoader classLoader, String className, Environment environment) throws Throwable {

        final Class<? extends TaskInvokable> invokableClass;
        try {
            invokableClass =
                    Class.forName(className, true, classLoader).asSubclass(TaskInvokable.class);
        } catch (Throwable t) {
            throw new Exception("Could not load the task's invokable class.", t);
        }

        Constructor<? extends TaskInvokable> statelessCtor;

        try {
            statelessCtor = invokableClass.getConstructor(Environment.class);
        } catch (NoSuchMethodException ee) {
            throw new FlinkException("Task misses proper constructor", ee);
        }

        // instantiate the class
        try {
            //noinspection ConstantConditions  --> cannot happen
            return statelessCtor.newInstance(environment);
        } catch (InvocationTargetException e) {
            // directly forward exceptions from the eager initialization
            throw e.getTargetException();
        } catch (Exception e) {
            throw new FlinkException("Could not instantiate the task's invokable class.", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Task cancellation
    //
    //  The task cancellation uses in total three threads, as a safety net
    //  against various forms of user- and JVM bugs.
    //
    //    - The first thread calls 'cancel()' on the invokable and closes
    //      the input and output connections, for fast thread termination
    //    - The second thread periodically interrupts the invokable in order
    //      to pull the thread out of blocking wait and I/O operations
    //    - The third thread (watchdog thread) waits until the cancellation
    //      timeout and then performs a hard cancel (kill process, or let
    //      the TaskManager know)
    //
    //  Previously, thread two and three were in one thread, but we needed
    //  to separate this to make sure the watchdog thread does not call
    //  'interrupt()'. This is a workaround for the following JVM bug
    //   https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8138622
    // ------------------------------------------------------------------------

    /**
     * This runner calls cancel() on the invokable, closes input-/output resources, and initially
     * interrupts the task thread.
     */
    private class TaskCanceler implements Runnable {

        private final Logger logger;
        private final TaskInvokable invokable;
        private final Thread executor;
        private final String taskName;

        TaskCanceler(Logger logger, TaskInvokable invokable, Thread executor, String taskName) {
            this.logger = logger;
            this.invokable = invokable;
            this.executor = executor;
            this.taskName = taskName;
        }

        @Override
        public void run() {
            try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
                // the user-defined cancel method may throw errors.
                // we need do continue despite that
                try {
                    invokable.cancel();
                } catch (Throwable t) {
                    ExceptionUtils.rethrowIfFatalError(t);
                    logger.error("Error while canceling the task {}.", taskName, t);
                }

                // Early release of input and output buffer pools. We do this
                // in order to unblock async Threads, which produce/consume the
                // intermediate streams outside of the main Task Thread (like
                // the Kafka consumer).
                // Notes: 1) This does not mean to release all network resources,
                // the task thread itself will release them; 2) We can not close
                // ResultPartitions here because of possible race conditions with
                // Task thread so we just call the fail here.
                failAllResultPartitions();
                closeAllInputGates();

                invokable.maybeInterruptOnCancel(executor, null, null);
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                logger.error("Error in the task canceler for task {}.", taskName, t);
            }
        }
    }

    /** This thread sends the delayed, periodic interrupt calls to the executing thread. */
    private static final class TaskInterrupter implements Runnable {

        /** The logger to report on the fatal condition. */
        private final Logger log;

        /** The invokable task. */
        private final TaskInvokable task;

        /** The executing task thread that we wait for to terminate. */
        private final Thread executorThread;

        /** The name of the task, for logging purposes. */
        private final String taskName;

        /** The interval in which we interrupt. */
        private final long interruptIntervalMillis;

        private final JobID jobID;

        TaskInterrupter(
                Logger log,
                TaskInvokable task,
                Thread executorThread,
                String taskName,
                long interruptIntervalMillis,
                JobID jobID) {

            this.log = log;
            this.task = task;
            this.executorThread = executorThread;
            this.taskName = taskName;
            this.interruptIntervalMillis = interruptIntervalMillis;
            this.jobID = jobID;
        }

        @Override
        public void run() {
            try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobID))) {
                // we initially wait for one interval
                // in most cases, the threads go away immediately (by the cancellation thread)
                // and we need not actually do anything
                executorThread.join(interruptIntervalMillis);

                // log stack trace where the executing thread is stuck and
                // interrupt the running thread periodically while it is still alive
                while (executorThread.isAlive()) {
                    task.maybeInterruptOnCancel(executorThread, taskName, interruptIntervalMillis);
                    try {
                        executorThread.join(interruptIntervalMillis);
                    } catch (InterruptedException e) {
                        // we ignore this and fall through the loop
                    }
                }
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                log.error("Error in the task canceler for task {}.", taskName, t);
            }
        }
    }

    /**
     * Watchdog for the cancellation. If the task thread does not go away gracefully within a
     * certain time, we trigger a hard cancel action (notify TaskManager of fatal error, which in
     * turn kills the process).
     */
    private static class TaskCancelerWatchDog implements Runnable {

        /** The executing task thread that we wait for to terminate. */
        private final Thread executorThread;

        /** The TaskManager to notify if cancellation does not happen in time. */
        private final TaskManagerActions taskManager;

        /** The timeout for cancellation. */
        private final long timeoutMillis;

        private final TaskInfo taskInfo;

        private final JobID jobID;

        TaskCancelerWatchDog(
                TaskInfo taskInfo,
                Thread executorThread,
                TaskManagerActions taskManager,
                long timeoutMillis,
                JobID jobID) {

            checkArgument(timeoutMillis > 0);

            this.taskInfo = taskInfo;
            this.executorThread = executorThread;
            this.taskManager = taskManager;
            this.timeoutMillis = timeoutMillis;
            this.jobID = jobID;
        }

        @Override
        public void run() {
            try (MdcCloseable ign = MdcUtils.withContext(MdcUtils.asContextData(jobID))) {
                Deadline timeout = Deadline.fromNow(Duration.ofMillis(timeoutMillis));
                while (executorThread.isAlive() && timeout.hasTimeLeft()) {
                    try {
                        executorThread.join(Math.max(1, timeout.timeLeft().toMillis()));
                    } catch (InterruptedException ignored) {
                        // we don't react to interrupted exceptions, simply fall through the loop
                    }
                }

                if (executorThread.isAlive()) {
                    logTaskThreadStackTrace(
                            executorThread,
                            taskInfo.getTaskNameWithSubtasks(),
                            timeoutMillis,
                            "notifying TM");
                    String msg =
                            "Task did not exit gracefully within "
                                    + (timeoutMillis / 1000)
                                    + " + seconds.";
                    taskManager.notifyFatalError(msg, new FlinkRuntimeException(msg));
                }
            } catch (Throwable t) {
                throw new FlinkRuntimeException("Error in Task Cancellation Watch Dog", t);
            }
        }
    }

    public static void logTaskThreadStackTrace(
            Thread thread, String taskName, long timeoutMs, String action) {
        StackTraceElement[] stack = thread.getStackTrace();
        StringBuilder stackTraceStr = new StringBuilder();
        for (StackTraceElement e : stack) {
            stackTraceStr.append(e).append('\n');
        }

        LOG.warn(
                "Task '{}' did not react to cancelling signal - {}; it is stuck for {} seconds in method:\n {}",
                taskName,
                action,
                timeoutMs / 1000,
                stackTraceStr);
    }

    /** Various operation of notify checkpoint. */
    public enum NotifyCheckpointOperation {
        ABORT,
        COMPLETE,
        SUBSUME
    }
}
