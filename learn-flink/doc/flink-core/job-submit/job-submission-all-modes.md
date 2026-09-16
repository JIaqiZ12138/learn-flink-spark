<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Flink 内核源码学习笔记（一）：任务提交全流程

> 代码基线：本仓库 `1.20-SNAPSHOT`（commit `fb230abd439`）
> 说明：文中行号对应当前 checkout，版本升级后可能有 ±几十行漂移，类名/方法名相对稳定，定位时优先用 `grep` 搜方法名而不是死记行号。

---

## 0. 先建立全局图景

一次 Flink 作业提交，本质上是**一个 `JobGraph` 对象从「用户 JVM / 客户端」走到「JobManager」，然后被还原成 `ExecutionGraph` 并开始调度**的过程。

```
┌───────────────── Client JVM ─────────────────┐
│ bin/flink                                    │
│   → CliFrontend.main()                       │
│   → PackagedProgram (加载用户 jar)            │
│   → 用户 main() 里的 env.execute()            │
│       → StreamGraph (逻辑图)                  │
│       → JobGraph   (物理图, 客户端生成)        │
│   → ClusterClient.submitJob(JobGraph)        │
└──────────────────┬───────────────────────────┘
                   │  REST: POST /jobs (multipart, JobGraph 走 Java 序列化)
                   ▼
┌──────────── JobManager JVM ──────────────────┐
│ WebMonitorEndpoint → JobSubmitHandler        │
│   → DispatcherGateway.submitJob()            │
│       → Dispatcher.submitJob()               │
│           → jobGraphWriter.putJobGraph()     │  ← HA 恢复的关键落盘点
│           → JobManagerRunner.start()         │
│               → JobMasterServiceLeadershipRunner (选主)
│                   → DefaultJobMasterServiceProcess
│                       → JobMaster            │
│                           → SchedulerNG = DefaultScheduler
│                               → ExecutionGraph (执行图)
│                               → SchedulingStrategy 开始调度
└──────────────────┬───────────────────────────┘
                   │  declareRequiredResources / requestSlot
                   ▼
┌──────────── ResourceManager ─────────────────┐
│ SlotManager (FineGrainedSlotManager)          │
└──────────────────┬───────────────────────────┘
                   │  requestSlot / offerSlots
                   ▼
┌──────────── TaskManager ─────────────────────┐
│ TaskExecutor.requestSlot → submitTask         │
│   → Task (真正跑用户算子)                       │
└───────────────────────────────────────────────┘
```

**一句话概括内核关注点**：客户端只负责「翻译 + 上传」，JobManager 负责「还原 + 调度」，TaskManager 负责「执行」。你学内核，主线就是不断追问：**对象在哪一方被创建、被序列化、被还原**。

---

## 1. 五条提交入口（先认清"源头"）

Flink 不是一个入口，而是五个入口收敛到同一个 `Dispatcher.submitJob()`。学习时不要贪多，**先只跟 `bin/flink run` 这一条**，其他四条最后再看差异。

| 入口 | 用户命令 / 代码 | 源码起点 |
|---|---|---|
| CLI | `bin/flink run app.jar` | `../../flink-dist/src/main/flink-bin/bin/flink` → `org.apache.flink.client.cli.CliFrontend` |
| 程序内嵌（IDE 调试） | `env.execute()` | `StreamExecutionEnvironment.execute()` |
| Table / SQL | `tableEnv.executeSql("INSERT ...")` | `TableEnvironmentImpl.executeInternal()` |
| SQL Gateway / JDBC | HTTP/JDBC 打到 Gateway | `../../flink-table/flink-sql-gateway` |
| REST 直连 | `POST /jobs` 自行上传 JobGraph | `JobSubmitHandler` |

程序内嵌方式（`LocalExecutor` + `MiniCluster`）是**你调试内核时最该用的方式**，因为它把 client、JobManager、TaskManager 塞进同一个 JVM，可以一路打断点不跨进程。

---

## 2. 客户端第一段：`bin/flink run` → `JobGraph`

### 2.1 脚本入口

`../../flink-dist/src/main/flink-bin/bin/flink`（末尾）：

```bash
exec "${JAVA_RUN}" $JVM_ARGS ... -classpath "..." org.apache.flink.client.cli.CliFrontend "$@"
```

所以**第一个断点**就是：

```
flink-clients/src/main/java/org/apache/flink/client/cli/CliFrontend.java
```

### 2.2 CliFrontend 的调用链

| 位置 | 方法 | 干了什么 |
|---|---|---|
| `CliFrontend.java:222` | `run(String[] args)` | `run` 子命令主流程 |
| 同上 | `CustomCommandLine activeCommandLine` | 选 `-m/--target` 的 CLI 实现（`GenericCLI`/`DefaultCLI`/`YarnCLI`...） |
| `CliFrontend.java:237` | `ProgramOptions.create(commandLine)` | 解析 `-c`、`-p`、`-s`、jar 路径、程序参数 |
| `CliFrontend.java:239` | `getJobJarAndDependencies()` | jar + 依赖 jar 列表 |
| `CliFrontend.java:241` | `getEffectiveConfiguration(...)` | **多配置源合并**：`flink-conf.yaml` < 命令行 `-D`/`-yD` < `ProgramOptions` |
| `CliFrontend.java:266/1046` | `getPackagedProgram → buildProgram` | 构造 `PackagedProgram`（用户 jar 的元信息 + 独立 classloader） |
| `CliFrontend.java:1024` | `executeProgram(config, program)` | 交给 `ClientUtils.executeProgram` |

注意 `getEffectiveConfiguration(effectiveConfiguration)` 的顺序非常重要——`execution.target`、`parallelism.default`、`jobmanager.rpc.address` 都从这里最终定型，**`PipelineExecutor` 的选择完全依赖这份 Configuration**。想改集群地址/执行模式，断点打在这里看最终值最省事。

### 2.3 `PackagedProgram`：用户代码怎么被"装起来"

`../../flink-clients/src/main/java/org/apache/flink/client/program/PackagedProgram.java`

- 持有用户 jar 的 URL、main class、程序参数、`Configuration`
- `getUserCodeClassLoader()`：`FlinkUserCodeClassLoaders`（child-first 加载，这就是为什么用户 jar 里的依赖能覆盖 Flink 自带版本）
- `invokeInteractiveModeForExecution()`（`:220`）：反射调用用户 main class 的 `main()`

### 2.4 上下文环境注入：`getExecutionEnvironment()` 为什么能拿到 client

`flink-clients/src/main/java/org/apache/flink/client/ClientUtils.java:77`：

```java
public static void executeProgram(PipelineExecutorServiceLoader executorServiceLoader,
                                  Configuration configuration,
                                  PackagedProgram program, ...) {
    Thread.currentThread().setContextClassLoader(userCodeClassLoader);
    ContextEnvironment.setAsContext(executorServiceLoader, configuration, userCodeClassLoader, ...);
    StreamContextEnvironment.setAsContext(executorServiceLoader, configuration, ...);
    ExecutionContextEnvironment.setAsContext(...);   // DataStream v2
    program.invokeInteractiveModeForExecution();     // ← 进用户 main()
}
```

于是用户代码里的 `StreamExecutionEnvironment.getExecutionEnvironment()` 拿到的其实是 `StreamContextEnvironment`（ThreadLocal 注入），它携带了 `PipelineExecutorServiceLoader`。**这是「CLI 能控制用户程序怎么提交」的机制**，也是 IDE 里 `getExecutionEnvironment()` 能变成 `LocalStreamEnvironment` 的同一套机制。

### 2.5 用户代码 → `StreamGraph` → `JobGraph`

```
StreamExecutionEnvironment.execute()            :2302
  → execute(String jobName)                    :2317
    → execute(StreamGraph)                     :2350
      → executeAsync(StreamGraph)              :2467
        → getPipelineExecutor()                :2989
          → executor.execute(streamGraph, configuration, userCodeClassloader)
```

`executor.execute(...)` 是分水岭：

**（A）`Pipeline → JobGraph`**（如果还没转）

```
PipelineExecutorUtils.getJobGraph(pipeline, configuration, userClassloader)
  → FlinkPipelineTranslationUtil.getJobGraph(...)      flink-clients/.../client/FlinkPipelineTranslationUtil.java
      → getPipelineTranslator(...)   // PlanTranslator(批) 或 StreamGraphTranslator(流)
          → StreamGraphTranslator.translateToJobGraph   flink-clients/.../client/StreamGraphTranslator.java
              → StreamGraph.getJobGraph(userClassloader, null)
                  → StreamingJobGraphGenerator.createJobGraph()
```

- `StreamGraph`：**逻辑图**，节点是 `StreamNode`（算子 + 序列化器 + 并行度），边是 `StreamEdge`（含分区方式、是否 forward）
- `JobGraph`：**物理图**，节点是 `JobVertex`（已按算子链合并），边是 `JobEdge` + `IntermediateDataSet`（中间结果）

关键文件：
- `flink-streaming-java/.../streaming/api/graph/StreamGraphGenerator.java` — `Transformation` 图 → `StreamGraph`
- `flink-streaming-java/.../streaming/api/graph/StreamingJobGraphGenerator.java` — **算子链（operator chaining）在这里发生**，`isChainable()` / `createChain()` 决定哪些算子被塞进同一个 `JobVertex`
- `flink-streaming-java/.../streaming/api/graph/StreamGraphHasherV2.java` — 生成 `JobVertex` 的 hash（savepoint 状态映射靠它，改算子结构会改 hash）

**（B）再补充作业级信息**

`flink-clients/.../client/deployment/executors/PipelineExecutorUtils.java`：

```java
jobGraph.setJobID(...)                                  // PIPELINE_FIXED_JOB_ID，测试/CLI 指定 jobId 用
jobGraph.setInitialClientHeartbeatTimeout(...)          // attached + shutdown-if-attached 才有
jobGraph.addJars(executionConfigAccessor.getJars());
jobGraph.setClasspaths(executionConfigAccessor.getClasspaths());
jobGraph.setSavepointRestoreSettings(...);              // -s 从 savepoint 恢复
```

> **重点**：`userJars`、`classpaths`、`savepointRestoreSettings`、`serializedExecutionConfig` 全挂在 `JobGraph` 上。所以 `JobGraph` 不只是「图」，它是**作业提交的完整信封**。

### 2.6 `PipelineExecutor` 怎么被选中

`StreamExecutionEnvironment.getPipelineExecutor()`（`:2989`）：

```
executorServiceLoader.getExecutorFactory(configuration)  // SPI: PipelineExecutorFactory
```

- 接口：`../../flink-core/src/main/java/org/apache/flink/core/execution/PipelineExecutor.java`
- 注册文件：`../../flink-clients/src/main/resources/META-INF/services/org.apache.flink.core.execution.PipelineExecutorFactory`
  ```
  org.apache.flink.client.deployment.executors.RemoteExecutorFactory
  org.apache.flink.client.deployment.executors.LocalExecutorFactory
  ```
- 查找逻辑：`flink-core/.../core/execution/DefaultExecutorServiceLoader.java`
- 判定依据：`execution.target` 配置项（`local` / `remote` / `yarn-per-job` / `kubernetes-application` ...），每个 `PipelineExecutorFactory.isCompatibleWith(configuration)` 自己判断

三种 executor 的差别：

| executor | 什么时候用 | 行为 |
|---|---|---|
| `LocalExecutor` (`:45 NAME="local"`) | IDE / `execution.target=local` | 起 `MiniCluster`（`PerJobMiniClusterFactory`），**client 和 JM/TM 同 JVM** |
| `RemoteExecutor` (`:31 NAME="remote"`) | session 模式、`flink run -m host:port` | `AbstractSessionClusterExecutor.execute()`：连已有集群 |
| `ApplicationClusterExecutor` | application 模式（`flink run-application -t yarn-application`） | **不生成 JobGraph**，直接把用户 jar 交给集群，main() 在 JM 里跑 |

---

## 3. 客户端第二段：`ClusterClient` → REST → JobManager

### 3.1 Session 模式：`AbstractSessionClusterExecutor.execute()`

`../../flink-clients/src/main/java/org/apache/flink/client/deployment/executors/AbstractSessionClusterExecutor.java`

```java
final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration, userCodeClassloader);

try (ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration)) {
    final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
    final ClusterClientProvider<ClusterID> provider = clusterDescriptor.retrieve(clusterID);
    ClusterClient<ClusterID> clusterClient = provider.getClusterClient();

    return clusterClient.submitJob(jobGraph)
            .thenApplyAsync(jobId -> {
                ClientUtils.waitUntilJobInitializationFinished(   // attached 模式：等作业脱离 INITIALIZING
                        () -> clusterClient.getJobStatus(jobId).get(),
                        () -> clusterClient.requestJobResult(jobId).get(),
                        userCodeClassloader);
                return jobId;
            })
            .thenApplyAsync(jobID -> (JobClient) new ClusterClientJobClientAdapter<>(provider, jobID, userCodeClassloader))
            .whenCompleteAsync((i1, i2) -> clusterClient.close());
}
```

要点：
- `ClusterClientFactory` 通过 SPI 加载（`../../flink-clients/src/main/resources/META-INF/services/org.apache.flink.client.deployment.ClusterClientFactory` → `flink-yarn`、`flink-kubernetes` 各自注册）→ `DefaultClusterClientServiceLoader` 选择
- 返回给用户的是 `ClusterClientJobClientAdapter`（`JobClient` 的门面），**attached 模式下 client JVM 会一直挂着等作业结束**（这也是"为什么 yarn-per-job 提交后终端不退出"的原因）
- `ClientUtils.waitUntilJobInitializationFinished` 是 attached 模式的语义来源：先等到不再是 `INITIALIZING`，否则立刻抛 `JobInitializationException`

### 3.2 `RestClusterClient.submitJob()`：JobGraph 怎么过网

`flink-clients/src/main/java/org/apache/flink/client/program/rest/RestClusterClient.java:358`

1. `Files.createTempFile("flink-jobgraph-" + jobGraph.getJobID(), ".bin")` → **`ObjectOutputStream.writeObject(jobGraph)`**，即 **Java 序列化**
2. 收集 `jobGraph.getUserJars()`、`getUserArtifacts()`（非分布式文件系统上的才上传）
3. 组装 `JobSubmitRequestBody`（`jobGraphFileName` + `jarFileNames` + `artifactFileNames`）
4. `sendRetriableRequest(JobSubmitHeaders.getInstance(), ..., requestBody, filesToUpload, ...)` → HTTP **`POST /jobs`**，`multipart/form-data`（`RestClient.sendRequest`）
5. 成功后删临时文件

**为什么是 Java 序列化？** 历史原因，`JobGraph` 至今实现 `Serializable`。这也是 Flink 内部唯一被允许用 Java 序列化的场景（AGENTS.md 明确规定「新特性禁止 Java 序列化，内部 RPC 传输除外」）。

> ⚠️ 注意区分：`JobGraph` 走 Java 序列化上传，而**算子内部的用户数据**走 Flink 自己的 `TypeSerializer`。两者是完全不同的东西。

### 3.3 `MiniClusterClient`

`flink-clients/.../client/program/MiniClusterClient.java` — 不上 HTTP，直接调 `MiniCluster.getDispatcherGateway().submitJob(...)`。调试时走这条最快。

---

## 4. JobManager 侧：从 HTTP 到 `ExecutionGraph`

### 4.1 `WebMonitorEndpoint` → `JobSubmitHandler`

- 注册：`../../flink-runtime/src/main/java/org/apache/flink/runtime/webmonitor/WebMonitorEndpoint.java`
- HTTP 定义：`flink-runtime/.../rest/messages/job/JobSubmitHeaders.java`（`URL = "/jobs"`，`POST`）
- 请求体：`flink-runtime/.../rest/messages/job/JobSubmitRequestBody.java`
- 处理器：`flink-runtime/.../rest/handler/job/JobSubmitHandler.java`

`JobSubmitHandler.handleRequest()` 干了三件事（**这是学习时最容易漏掉的一步**）：

```java
CompletableFuture<JobGraph> jobGraphFuture = loadJobGraph(requestBody, nameToFile);      // ObjectInputStream.readObject
Collection<Path> jarFiles = getJarFilesToUpload(...);
CompletableFuture<JobGraph> finalized = uploadJobGraphFiles(gateway, jobGraphFuture, jarFiles, artifacts, configuration);
//   ↑ 把 jar 上传到 BlobServer，并把 blob key 写回 JobGraph（setUserJars / setUserArtifacts）
CompletableFuture<Acknowledge> submission = finalized.thenCompose(jg -> gateway.submitJob(jg, timeout));
```

两个关键认识：
1. **jar 文件在这里被重新上传到 BlobServer**，`JobGraph` 里存的是 blob key 而非本地路径 —— 这就是 TaskManager 后来能拿到用户 jar 的原因。
2. `JobSubmitHandler` 只做「反序列化 + 落 blob + 转发」，**真正的提交动作在 `Dispatcher`**。REST 层非常薄。

### 4.2 `Dispatcher.submitJob()`

`flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/Dispatcher.java:518`

```
submitJob(jobGraph, timeout)
  ├─ isInGloballyTerminalState(jobId)?            → DuplicateJobSubmissionException.ofGloballyTerminated
  ├─ jobManagerRunnerRegistry.isRegistered(jobId)? → DuplicateJobSubmissionException.of(jobId)
  │   （还有 submittedAndWaitingTerminationJobIDs：正在提交中的也算重复）
  ├─ isPartialResourceConfigured(jobGraph)?       → 部分 vertex 配了资源则拒绝
  └─ internalSubmitJob(jobGraph)                  :612
       └─ waitForTerminatingJob(jobId, jobGraph, this::persistAndRunJob)

persistAndRunJob(jobGraph)                        :651
  ├─ jobGraphWriter.putJobGraph(jobGraph)   ← ★ HA 恢复的落盘点
  ├─ initJobClientExpiredTime(jobGraph)     ← attached 模式客户端心跳超时
  └─ runJob(createJobMasterRunner(jobGraph), ExecutionType.SUBMISSION)

createJobMasterRunner(jobGraph)                   :659
  └─ jobManagerRunnerFactory.createJobManagerRunner(...)   → JobMasterServiceLeadershipRunner

runJob(jobManagerRunner, executionType)           :681
  ├─ jobManagerRunner.start()
  ├─ jobManagerRunnerRegistry.register(runner)
  └─ getResultFuture().handleAsync(handleJobManagerRunnerResult)  ← 作业结束后归档/清理/回调
```

**`jobGraphWriter.putJobGraph()` 是本流程第一个「持久化」点**：JobManager 挂掉后重启，`Dispatcher.startRecoveredJobs()`（`:397`）会从 `JobGraphStore` 里把作业捞回来重新跑。搞 HA/恢复必须从这里入手。

### 4.3 `JobManagerRunner` 的层次：为什么要多一层选主

```
JobManagerRunner (接口)
  └─ JobMasterServiceLeadershipRunner            flink-runtime/.../jobmaster/JobMasterServiceLeadershipRunner.java
       （implements LeaderContender，参加 JobManager 选主）
       └─ JobMasterServiceProcess (接口)
            └─ DefaultJobMasterServiceProcess     flink-runtime/.../jobmaster/DefaultJobMasterServiceProcess.java
                 └─ JobMasterServiceFactory.createJobMasterService()
                      └─ DefaultJobMasterServiceFactory  → new JobMaster(...)
```

- 工厂：`flink-runtime/.../dispatcher/JobMasterServiceLeadershipRunnerFactory.java`（在 `SessionDispatcherFactory:51`、`JobDispatcherFactory:68` 被装配）
- 目的：一个作业一个 `JobMaster`，但 JobMaster 必须由「拿到领导权的那个 JobManager 进程」持有。`LeaderContender` 机制保证 JobMaster 只在 leader 上运行，`JobMasterId`（fencing token）是防止脑裂的关键。

**调试建议**：本地不配 HA 时，链路会短很多（`DefaultJobMasterServiceProcess` 直接创建 `JobMaster`），先按非 HA 路径打断点，理解后再看 HA。

### 4.4 `JobMaster.start()`

`../../flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobMaster.java`

```
JobMaster.start()
  → schedulerNG 的创建在构造函数里完成（:382 createScheduler(slotPoolServiceSchedulerFactory, ...)）
  → startJobExecution()                    :1138
      ├─ shuffleMaster.registerJob(context)
      ├─ startJobMasterServices()          :1155
      │    ├─ createTaskManagerHeartbeatManager / createResourceManagerHeartbeatManager
      │    ├─ slotPoolService.start(fencingToken, address, mainThreadExecutor)
      │    └─ resourceManagerLeaderRetriever.start(...)   ← 异步连 RM，连上后 slot pool 才开始要 slot
      └─ startScheduling()                 :1234
           → schedulerNG.startScheduling()
```

注意 `JobMaster` 构造时就已创建 `SchedulerNG`，所以 **`ExecutionGraph` 的构建发生在 `JobMaster` 构造期间**，早于 `start()`。

### 4.5 `ExecutionGraph` 的构建

```
JobMaster.createScheduler()                      :397
  → SlotPoolServiceSchedulerFactory.createScheduler(...)
      → SchedulerNGFactory.createInstance(...)   → DefaultSchedulerFactory.createInstance
          ├─ SlotPool slotPool = slotPoolService.castInto(SlotPool.class)
          ├─ createSchedulerComponents(...)      ← 决定 pipelined/blocking 调度组件
          ├─ RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(...)
          ├─ new DefaultExecutionGraphFactory(...)
          └─ new DefaultScheduler(log, jobGraph, ..., executionGraphFactory, ...)

DefaultScheduler 构造 → SchedulerBase 构造
  → DefaultExecutionGraphFactory.createAndRestoreExecutionGraph(...)
      → DefaultExecutionGraphBuilder.buildGraph(...)   flink-runtime/.../executiongraph/DefaultExecutionGraphBuilder.java:75
```

关键文件：
- `flink-runtime/.../scheduler/DefaultSchedulerFactory.java`
- `flink-runtime/.../scheduler/DefaultScheduler.java`
- `flink-runtime/.../scheduler/SchedulerBase.java`（`:382` 处 graph 创建）
- `flink-runtime/.../scheduler/DefaultExecutionGraphFactory.java`（`:173`）
- `flink-runtime/.../executiongraph/DefaultExecutionGraphBuilder.java`
- `flink-runtime/.../executiongraph/ExecutionGraph.java`（**内核最核心的类之一**）

**`ExecutionGraph` 里生成了什么**：`ExecutionJobVertex` / `ExecutionVertex` / `Execution`(attempt) 三层展开，`IntermediateResult` / `IntermediateResultPartition`，每个 `ExecutionVertex` 的并行度份数在这里被实例化。同时这里也是**重启策略、checkpoint 协调器、状态恢复（savepoint）**的挂载点。

> 学习提示：`JobGraph`（静态、一次提交一个）vs `ExecutionGraph`（动态、带 attempt 和状态机，会因 failover 而演变）是理解 Flink 运行时的分水岭，务必分清。

### 4.6 从 `ExecutionGraph` 到实际部署

```
DefaultScheduler.startSchedulingInternal()      :234
  → schedulingStrategy.startScheduling()
      PipelinedRegionSchedulingStrategy.startScheduling()   flink-runtime/.../scheduler/strategy/PipelinedRegionSchedulingStrategy.java:182
        → SchedulerOperations.allocateSlotsAndDeploy(verticesToDeploy)
            DefaultScheduler.allocateSlotsAndDeploy()       :466
              → executionDeployer.allocateSlotsAndDeploy(executionsToDeploy, requiredVersionByVertex)
                  → ExecutionSlotAllocator (SlotSharingExecutionSlotAllocator)
                      → PhysicalSlotProviderImpl.allocatePhysicalSlots()
                          → SlotPool / DeclarativeSlotPoolBridge   flink-runtime/.../jobmaster/slotpool/
                              → DeclareResourceRequirementServiceConnectionManager
                                  → ResourceManagerGateway.declareRequiredResources(...)   flink-runtime/.../resourcemanager/ResourceManagerGateway.java:90
                                      → SlotManager = FineGrainedSlotManager    .../resourcemanager/slotmanager/FineGrainedSlotManager.java
                                          → TaskExecutorGateway.requestSlot(...)  flink-runtime/.../taskexecutor/TaskExecutor.java:1183
                                              → TaskExecutor.submitTask(...)      :660
                                                  → new Task(...) → 真正执行算子
```

**调度策略（`SchedulingStrategy`）**要看清：默认是 **pipelined region 调度**——把 `ExecutionGraph` 切成若干「流水线区域」（region 内是 all-or-nothing 一起调度），而不是一个 vertex 一个 vertex 地调度。`PipelinedRegionSchedulingStrategy` 是理解「为什么 Flink 是区域级部署」的入口。

再往后的 `Task` 执行、网络栈、反压就超出「提交」范畴了，属于后续模块。

---

## 5. Application 模式：完全不同的路径

`flink run-application` 不生成 `JobGraph`：

```
CliFrontend.runApplication(args)                    flink-clients/.../client/cli/CliFrontend.java:170
  → new ApplicationClusterDeployer(clusterClientServiceLoader)
  → deployer.run(effectiveConfiguration, new ApplicationConfiguration(programArgs, entryPointClassName))
      → ClusterDescriptor.deployApplicationCluster(...)
          → YarnClusterDescriptor / KubernetesClusterDescriptor
              → 启动 ApplicationClusterEntryPoint
                  → ApplicationDispatcherBootstrap.runApplication()
                      → ClientUtils.executeProgram(...)  ← 用户 main() 在 JobManager JVM 里跑
```

- 入口类：`flink-clients/.../client/deployment/application/ApplicationClusterEntryPoint.java:54`（`extends ClusterEntrypoint`）
- Bootstrap：`flink-clients/.../client/deployment/application/ApplicationDispatcherBootstrap.java`
- 特点：**`JobGraph` 在 JobManager 进程内生成**，client 提交完就退出。这也是 application 模式"一个作业一个集群"的资源模型来源。

Debug 建议：比较 `flink run` 与 `flink run-application` 两条链的**分叉点** —— 分叉发生在 `PipelineExecutor` 与 `ClusterDescriptor` 之间，其余（`Dispatcher` 之后）完全一致。

---

## 6. Table / SQL 路径的差异

`../../flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/internal/TableEnvironmentImpl.java`

```
executeSql("INSERT INTO ...")
  → executeInternal(Operation)                        :1099
      → executeInternal(List<ModifyOperation>)        :839
          → translate(...) 得到 List<Transformation<?>>
          → executeInternal(transformations, sinkIdentifierNames, jobStatusHookList)  :1016
              → execEnv.createPipeline(transformations, tableConfig, jobName)   :1025
              → execEnv.executeAsync(pipeline)                                  :1032   ← 从这往后与 DataStream 完全合流
```

查询（`SELECT`）走另一条：`generatePipelineFromQueryOperation()`（`:1185`）→ `execEnv.executeAsync(pipeline)`（`:1072`）。

**结论**：Table/SQL 只在「生成 `Transformation` / `Pipeline`」这一段不同（planner 的活儿），**从 `PipelineExecutor` 开始与 DataStream API 共用同一条提交链**。所以你把 `job-submission` 这条主线吃透，SQL 侧只需要补 planner。

---

## 7. 关键类速查表

### 客户端

| 类 | 路径（`../../flink-clients/src/main/java/org/apache/flink/client`） | 职责 |
|---|---|---|
| `CliFrontend` | `cli/CliFrontend.java` | CLI 总入口，`run` / `run-application` / `list` / `cancel` / `savepoint` |
| `ProgramOptions` | `cli/ProgramOptions.java` | `run` 子命令参数解析 |
| `PackagedProgram` | `program/PackagedProgram.java` | 用户 jar 的封装与 main() 反射调用 |
| `ContextEnvironment` / `StreamContextEnvironment` | `program/*.java` | ThreadLocal 注入执行环境 |
| `ClientUtils` | `ClientUtils.java:77` | `executeProgram`、`waitUntilJobInitializationFinished` |
| `PipelineExecutorUtils` | `deployment/executors/PipelineExecutorUtils.java` | `Pipeline → JobGraph` + 作业级信息挂载 |
| `FlinkPipelineTranslationUtil` | `FlinkPipelineTranslationUtil.java` | 选 translator（`PlanTranslator` / `StreamGraphTranslator`） |
| `AbstractSessionClusterExecutor` | `deployment/executors/AbstractSessionClusterExecutor.java` | session 模式提交主流程 |
| `RemoteExecutor` / `LocalExecutor` | `deployment/executors/*.java` | `PipelineExecutor` 实现 |
| `RestClusterClient` | `program/rest/RestClusterClient.java:358` | REST 提交、savepoint、cancel |
| `MiniClusterClient` | `program/MiniClusterClient.java` | 本地直连 MiniCluster |
| `ClusterClientJobClientAdapter` | `deployment/ClusterClientJobClientAdapter.java` | 返回给用户的 `JobClient` |

### 图与计划

| 类 | 模块 | 职责 |
|---|---|---|
| `StreamGraph` / `StreamNode` / `StreamEdge` | `flink-streaming-java` | 逻辑图 |
| `StreamGraphGenerator` | `flink-streaming-java` | `Transformation` → `StreamGraph` |
| `StreamingJobGraphGenerator` | `flink-streaming-java` | `StreamGraph` → `JobGraph`，**算子链在此** |
| `StreamGraphHasherV2` | `flink-streaming-java` | vertex hash（savepoint 映射） |
| `JobGraph` / `JobVertex` / `JobEdge` / `IntermediateDataSet` | `flink-runtime/.../jobgraph/` | 物理图（提交信封） |
| `ExecutionGraph` / `ExecutionJobVertex` / `ExecutionVertex` / `Execution` | `flink-runtime/.../executiongraph/` | 执行图（运行时可演变） |

### 服务端

| 类 | 路径（`../../flink-runtime/src/main/java/org/apache/flink/runtime`） | 职责 |
|---|---|---|
| `WebMonitorEndpoint` | `webmonitor/WebMonitorEndpoint.java` | REST 端点注册 |
| `JobSubmitHandler` | `rest/handler/job/JobSubmitHandler.java` | `/jobs` 处理：反序列化 JobGraph + 上传 jar |
| `Dispatcher` | `dispatcher/Dispatcher.java` | 接收提交、落 JobGraphStore、创建 runner |
| `DispatcherGateway` | `dispatcher/DispatcherGateway.java` | RPC 门面 |
| `JobMasterServiceLeadershipRunner` | `jobmaster/JobMasterServiceLeadershipRunner.java` | 作业级选主 |
| `DefaultJobMasterServiceProcess` | `jobmaster/DefaultJobMasterServiceProcess.java` | 领导权生命周期 |
| `DefaultJobMasterServiceFactory` | `jobmaster/factories/DefaultJobMasterServiceFactory.java` | `new JobMaster(...)` |
| `JobMaster` | `jobmaster/JobMaster.java` | 单作业运行期"大脑" |
| `SchedulerBase` / `DefaultScheduler` | `scheduler/*.java` | 构造 ExecutionGraph + 调度 |
| `DefaultExecutionGraphBuilder` | `executiongraph/DefaultExecutionGraphBuilder.java` | `JobGraph` → `ExecutionGraph` |
| `PipelinedRegionSchedulingStrategy` | `scheduler/strategy/` | 区域级调度 |
| `ExecutionDeployer` / `ExecutionSlotAllocator` | `scheduler/` | 分配 slot 并下发部署 |
| `SlotPool` / `DeclarativeSlotPoolBridge` | `jobmaster/slotpool/` | JobMaster 侧 slot 账本 |
| `ResourceManager` / `FineGrainedSlotManager` | `resourcemanager/` | 全局资源管理与分配 |
| `TaskExecutor` | `taskexecutor/TaskExecutor.java` | TM 侧收 slot 请求、提交 Task |

---

## 8. 建议的阅读顺序（按依赖倒序，先粗后细）

**第 1 轮 — 只跟主干，不追细节（半天）**

1. `CliFrontend.run()` → `executeProgram()`
2. `ClientUtils.executeProgram()`（理解上下文环境注入）
3. `StreamExecutionEnvironment.executeAsync()` → `getPipelineExecutor()`
4. `AbstractSessionClusterExecutor.execute()`
5. `RestClusterClient.submitJob()`（看懂"JobGraph 被序列化成文件 POST 出去"就够）
6. `JobSubmitHandler.handleRequest()` → `Dispatcher.submitJob()` → `runJob()`
7. `JobMaster.startJobExecution()` → `startScheduling()`
8. `DefaultSchedulerFactory.createInstance()` → `DefaultExecutionGraphBuilder.buildGraph()`

第 1 轮的目标只有一个：**能在脑中复述 JobGraph 从生成到落盘的完整旅程**。

**第 2 轮 — 补两个"图"的差异**

`StreamGraph` / `StreamingJobGraphGenerator`（重点 `createChain`）→ `JobGraph` → `ExecutionGraph`。

**第 3 轮 — 补调度与资源**

`PipelinedRegionSchedulingStrategy` → `ExecutionDeployer` → `ExecutionSlotAllocator` → `SlotPool` → `ResourceManager` → `TaskExecutor.submitTask`。

**第 4 轮 — 补旁支**

`ApplicationClusterEntryPoint`（application 模式）、`TableEnvironmentImpl`（SQL）、HA（`JobGraphStore` + `LeaderContender`）。

---

## 9. 实操：怎么调起来

### 9.1 最快路径：MiniCluster + 断点

在 `flink-examples` 里找一个作业，或自己写一个最小 `main`：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
env.fromElements(1, 2, 3).map(i -> i * 2).print();
env.execute("debug-job");
```

以 `LocalExecutor`（`execution.target=local`，或 IDE 默认）运行，在这几个点打断点，全部在同一个 JVM 内：

| 断点 | 观察什么 |
|---|---|
| `StreamGraphGenerator.generate()` | `Transformation` 列表怎么变成 `StreamNode` |
| `StreamingJobGraphGenerator.createChain()` | 哪几个算子被合并成一个 `JobVertex` |
| `MiniClusterClient.submitJob()` | 提交的 `JobGraph` 结构（看 `getVertices()`） |
| `Dispatcher.submitJob()` | jobGraph 落盘时机 |
| `DefaultExecutionGraphBuilder.buildGraph()` | `ExecutionVertex` 的并行度展开 |
| `PipelinedRegionSchedulingStrategy.startScheduling()` | region 划分结果 |

### 9.2 集群路径

```bash
# terminal 1
./bin/start-cluster.sh

# terminal 2：attached 提交，终端会挂着
./bin/flink run -m localhost:8081 ./examples/streaming/WordCount.jar

# detached 提交
./bin/flink run -d -m localhost:8081 ./examples/streaming/WordCount.jar
```

### 9.3 直接观察 REST 协议

`POST /jobs` 是 `multipart/form-data`，可以直接用 curl 测；但更实用的是在 `RestClient.sendRequest` 或 `JobSubmitHandler.handleRequest` 打断点，看 `JobSubmitRequestBody` 里三个文件名字段。

### 9.4 常用辅助配置

这四项是**跟着提交流程走**的机制性配置（谁读它们、在哪读），实战参数请直接看 §10。

| 配置 | 作用 |
|---|---|
| `execution.target` | 选 `PipelineExecutor`（`local` / `remote` / ...） |
| `parallelism.default` | 默认并行度，`PipelineExecutorUtils` 会用到 |
| `pipeline.jars` / `pipeline.classpaths` | 编程方式指定 jar 与 classpath |
| `execution.attached` | attached / detached 语义 |
| `$internal.pipeline.job-id`（`PIPELINE_FIXED_JOB_ID`） | 固定 jobId，便于测试（内部选项，勿在生产环境用） |

---

## 10. 通用提交参数设置：`flink run` 的参数从哪来、怎么生效

前面 §2.2 只说了「配置在这里合并」，这一节把它讲透。理解参数的关键不是背选项表，而是**搞清三份配置的叠加顺序**——90% 的"我明明配了却不生效"都源于此。

### 10.1 先看这个：一组最常用的提交参数（简明版）

日常 90% 的需求就是这几项——JM 内存、TM 内存、slot 数、JM/TM 核数、checkpoint 间隔：

```bash
FLINK_HOME/bin/flink run \
  -t yarn-per-job \
  -p 8 \
  -D jobmanager.memory.process.size=2048m \
  -D taskmanager.memory.process.size=4096m \
  -D taskmanager.numberOfTaskSlots=4 \
  -D yarn.appmaster.vcores=2 \
  -D yarn.containers.vcores=4 \
  -D execution.checkpointing.interval=60s \
  -D execution.checkpointing.dir=hdfs:///flink/checkpoints \
  -D state.backend.type=rocksdb \
  ./app.jar
```

逐项对照：

| 想调什么 | 参数 | 自带 `flink-conf.yaml` 的值 |
|---|---|---|
| JM 内存 | `-D jobmanager.memory.process.size=2048m` | `1600m` |
| TM 内存 | `-D taskmanager.memory.process.size=4096m` | `1728m` |
| 每个 TM 的 slot 数 | `-D taskmanager.numberOfTaskSlots=4` | `1` |
| JM 核数（YARN） | `-D yarn.appmaster.vcores=2` | 默认 1 |
| TM 核数（YARN） | `-D yarn.containers.vcores=4` | `-1`，即等于 slot 数 |
| JM 核数（K8s） | `-D kubernetes.jobmanager.cpu.amount=2` | `1.0` |
| TM 核数（K8s） | `-D kubernetes.taskmanager.cpu.amount=4` | `-1.0`，即等于 slot 数 |
| checkpoint 间隔 | `-D execution.checkpointing.interval=60s` | 无默认值——**不配就不做周期 checkpoint** |
| checkpoint 目录 | `-D execution.checkpointing.dir=hdfs:///flink/checkpoints` | 无默认值 |
| 状态后端 | `-D state.backend.type=rocksdb` | `hashmap` |
| 作业并行度 | `-p 8`（不是 `-D`） | `parallelism.default: 1` |

> ⚠️ **这一节最值钱的一条**：上面这些**内存 / slot / 核数**参数，只在「**集群随作业一起创建**」的部署模式下生效：
>
> | 模式 | 生效？ | 原因 |
> |---|---|---|
> | `-t yarn-per-job`、`-t yarn-application`、`-t kubernetes-application` | ✅ | JM/TM 容器为此作业新起，启动时读的正是这份配置（`YarnClusterDescriptor` / `KubernetesClusterDescriptor` 从配置里算容器规格） |
> | `-t remote`、`-t yarn-session`、`-t kubernetes-session` | ❌ | JM/TM 早就跑起来了，提交参数**改不了已运行的进程** |
>
> 所以往 session 集群提交时，`-D taskmanager.memory.process.size=...` 是**静默无效**的（不报错，只是没人理）。这类参数必须在**集群启动前**写进 `conf/flink-conf.yaml`，或作为 `-D` 传给 `start-cluster.sh` / `yarn-session.sh`。
>
> 而 `-p`、checkpoint 间隔、状态后端这些**作业级**配置，任何模式下都生效——因为它们是跟着 `JobGraph` 走的，不依赖进程重启。

### 10.2 三份配置与优先级

`CliFrontend.getEffectiveConfiguration(activeCommandLine, commandLine, programOptions, jobJars)`（`CliFrontend.java:294`）把三份配置依次叠加，**后叠加的覆盖先叠加的**：

```java
// ① + ②
final Configuration effectiveConfiguration =
        getEffectiveConfiguration(activeCustomCommandLine, commandLine);
// ③
final ExecutionConfigAccessor executionParameters =
        ExecutionConfigAccessor.fromProgramOptions(programOptions, jobJars);
executionParameters.applyToConfiguration(effectiveConfiguration);
```

| 顺序 | 来源 | 代码位置 | 覆盖范围 |
|---|---|---|---|
| ① | `conf/flink-conf.yaml`（`FLINK_CONF_DIR`） | `CliFrontend` 构造函数 | 集群级默认值 |
| ② | Custom command line：`-t`/`-e`、`-D`、`-m`、`-z` | `GenericCLI` / `DefaultCLI` / `FlinkYarnSessionCli` 的 `toConfiguration()` | 提交目标 + 任意配置项 |
| ③ | Program options：`-p`、`-d`、`-sae`、`-C`、`-s`/`-n`/`-cm`，以及 jar → `pipeline.jars` | `ProgramOptions.applyToConfiguration()`（`ProgramOptions.java:182`）、`ExecutionConfigAccessor.fromProgramOptions()` | 并行度、运行模式、savepoint |

由此得到一个**反直觉但很重要**的结论：③ 在 ② 之后应用，所以

```bash
flink run -p 4 -D parallelism.default=2 app.jar     # 实际并行度是 4，不是 2
```

`-p` 覆盖 `-D parallelism.default`；而 `-D` 覆盖 `flink-conf.yaml`。完整优先级：

```
flink-conf.yaml   <   -D / -t / -m / -z   <   -p / -d / -s / -C / -sae
```

> **排查"参数不生效"的最快手段**：`CliFrontend.java:310` 有一行
> `LOG.debug("Effective configuration after Flink conf, custom commandline, and program options: {}", ...)`。
> 把 `root` 日志级别开到 DEBUG，或在 `:308` 打断点，就能看到**最终生效的完整配置**。

### 10.3 Custom command line 的选择（决定哪些参数会被解析）

`CliFrontend.loadCustomCommandLines()`（`:1422`）按固定顺序注册，`validateAndGetActiveCommandLine()`（`:1466`）返回**第一个 `isActive()` 为 true 的**：

| 顺序 | 实现 | `isActive()` 条件 | 备注 |
|---|---|---|---|
| 1 | `GenericCLI` | 传了 `-t`/`-e`，**或** flink-conf.yaml 里已有 `execution.target` | 提供 `-t`/`-e`/`-D` |
| 2 | `FlinkYarnSessionCli`（前缀 `y`/`yarn`） | 传了 `-y*` 参数或 `-m yarn-cluster` | 提供 `-yD`/`-yqu`/`-yjm`/`-ytm`/`-ys`/`-ynm`/`-yat` 等 |
| 3 | `DefaultCLI` | **永远 true** | 兜底，所以必须放最后（源码注释明确写了这一点） |

两个容易踩的细节：

1. `DefaultCLI.toConfiguration()` 会**无条件设置 `execution.target=remote`**，并把 `-m host:port` 转成 `jobmanager.rpc.address` + `rest.path` + `rest.ssl.enabled`。也就是说**不写 `-t` 时默认走 session 集群**。
2. `-m` **只在 HA 配置为 NONE 时才被尊重**（这是选项描述里的原话）。开了 HA 还得靠 `-z` + `high-availability` 配置去发现 JobManager。

### 10.4 通用参数速查表

以下参数与具体部署方式无关，都能用：

| 参数 | 长选项 | 等价的配置项 | 说明 |
|---|---|---|---|
| `-t` | `--target` | `execution.target` | 部署目标，见下表；**决定选哪个 `PipelineExecutor`** |
| `-e` | `--executor` | `execution.target` | 已废弃，用 `-t` |
| `-D` | — | 任意 key | 通用配置覆盖，可重复出现 |
| `-p` | `--parallelism` | `parallelism.default` | 并行度，**优先级高于 `-D`** |
| `-d` | `--detached` | `execution.attached=false` | 分离模式；不写则 attached |
| `-sae` | `--shutdownOnAttachedExit` | `execution.shutdown-on-attached-exit` | attached 下 Ctrl+C 时顺带关集群 |
| `-c` | `--class` | — | 入口类；jar 的 manifest 没写 `Main-Class` 时必填 |
| `-C` | `--classpath` | `pipeline.classpaths` | 所有节点可见的 URL，**必须带协议**（`file://`、`hdfs://`），可重复 |
| `-s` | `--fromSavepoint` | `execution.state-recovery.path` | 从 savepoint 恢复 |
| `-n` | `--allowNonRestoredState` | `execution.state-recovery.ignore-unclaimed-state` | 允许跳过无法恢复的状态 |
| `-cm` | `--claimMode` | `execution.state-recovery.claim-mode` | `claim` / `no_claim`(默认) / `legacy` |
| `-m` | `--jobmanager` | `jobmanager.rpc.address` | 仅 HA=NONE 生效 |
| `-z` | `--zookeeperNamespace` | `high-availability.cluster-id` | HA 命名空间 |
| `-a` | `--arguments` | — | 程序参数；也可直接跟在 jar 后面 |
| `-j` | `--jarfile` | — | 显式指定 jar；也可作为第一个位置参数 |

`-s`/`-n`/`-cm` 三者的落点可以在 `SavepointRestoreSettings.toConfiguration()`（`flink-runtime/.../jobgraph/SavepointRestoreSettings.java:171`）里一眼看全。

#### `-t` 可选值

| 值 | 模式 | 是否可用于 `run-application` |
|---|---|---|
| `local` | MiniCluster，client 与 JM/TM 同 JVM | ✗ |
| `remote` | session 集群（**默认**） | ✗ |
| `yarn-session` | YARN session | ✗ |
| `yarn-per-job` | YARN per-job（已废弃） | ✗ |
| `kubernetes-session` | K8s session | ✗ |
| `yarn-application` | YARN application | ✓ |
| `kubernetes-application` | K8s application | ✓ |

值的定义位置：`YarnDeploymentTarget`（`flink-yarn/.../configuration/`）、`KubernetesDeploymentTarget`（`flink-kubernetes/.../configuration/`）。
`run-application` 只用 `-t` 指定 application 类目标，**`-m` 在 application 模式下没有意义**。

### 10.5 `-D` 常用配置项（按用途分组）

`-D` 是唯一"万金油"参数：`DynamicPropertiesUtil.encodeDynamicProperties()` 把 `key=value` 直接 `setString` 到有效配置上；**只写 key 不写值等于设成 `true`**。

#### 运行时语义

| key | 取值 | 默认 |
|---|---|---|
| `execution.runtime-mode` | `STREAMING` / `BATCH` / `AUTOMATIC` | `STREAMING` |
| `pipeline.name` | 作业名 | jar/类名推导 |
| `pipeline.operator-chaining.enabled` | `true` / `false` | `true` |
| `pipeline.object-reuse` | `true` / `false` | `false` |
| `pipeline.auto-watermark-interval` | 如 `200ms` | 无 |
| `pipeline.auto-generate-uids` | `true` / `false` | `true` |

#### 并行度与资源

| key | 说明 |
|---|---|
| `parallelism.default` | 默认并行度（会被 `-p` 覆盖） |
| `pipeline.max-parallelism` | 最大并行度，影响 key group 数量，**改它等于改 savepoint 兼容性，慎用** |
| `pipeline.jobvertex-parallelism-overrides` | 按 vertex 覆盖并行度 |
| `taskmanager.numberOfTaskSlots` | TM 的 slot 数（**集群侧配置**，作业级配置改不了已启动的 TM） |

#### Checkpoint 与状态

| key | 取值 / 说明 | 默认 |
|---|---|---|
| `execution.checkpointing.mode` | `EXACTLY_ONCE` / `AT_LEAST_ONCE` | `EXACTLY_ONCE` |
| `execution.checkpointing.interval` | 触发间隔，如 `30s`；**不设就不做周期 checkpoint** | 无 |
| `execution.checkpointing.timeout` | 单次超时 | 10min |
| `execution.checkpointing.min-pause` | 两次之间最小间隔 | 0 |
| `execution.checkpointing.max-concurrent-checkpoints` | 并发数 | 1 |
| `execution.checkpointing.externalized-checkpoint-retention` | `NO_EXTERNALIZED_CHECKPOINTS` / `DELETE_ON_CANCELLATION` / `RETAIN_ON_CANCELLATION` | `NO_EXTERNALIZED_CHECKPOINTS` |
| `execution.checkpointing.dir` | checkpoint 存储目录 | 无 |
| `execution.checkpointing.savepoint-dir` | savepoint 默认目录 | 无 |
| `execution.checkpointing.incremental` | 增量 checkpoint | `false` |
| `execution.checkpointing.unaligned.enabled` | 非对齐 checkpoint | `false` |
| `execution.checkpointing.tolerable-failed-checkpoints` | 容忍失败次数 | 0 |
| `state.backend.type` | `hashmap` / `rocksdb`（或 `StateBackendFactory` 全类名） | `hashmap` |

#### 故障恢复

| key | 取值 |
|---|---|
| `restart-strategy.type` | `disable`（别名 `none`/`off`）/ `fixed-delay` / `failure-rate` / `exponential-delay` |
| `restart-strategy.fixed-delay.attempts` | 次数 |
| `restart-strategy.fixed-delay.delay` | 如 `10s` |
| `restart-strategy.failure-rate.max-failures-per-interval` | 次数 |
| `restart-strategy.failure-rate.failure-rate-interval` | 时间窗 |
| `restart-strategy.failure-rate.delay` | 如 `10s` |

#### 客户端行为

| key | 默认 | 说明 |
|---|---|---|
| `client.timeout` | 60s | 与 JobManager 交互超时 |
| `client.retry-period` | 2s | 重试间隔 |
| `client.heartbeat.timeout` | 180s | attached 模式下 client 心跳超时，超时后 JM 会取消作业 |
| `client.heartbeat.interval` | 30s | 心跳间隔 |

> ⚠️ **1.20 有一批配置项改名（旧名仍可用但不推荐）**，看老博客/老教程时特别容易对不上：
>
> | 旧名 | 新名（1.20） |
> |---|---|
> | `state.backend` | `state.backend.type` |
> | `state.backend.incremental` | `execution.checkpointing.incremental` |
> | `state.checkpoints.dir` | `execution.checkpointing.dir` |
> | `state.savepoints.dir` | `execution.checkpointing.savepoint-dir` |
> | `restart-strategy` | `restart-strategy.type` |
> | `pipeline.operator-chaining` | `pipeline.operator-chaining.enabled` |
>
> 都可从 `ConfigOption.withDeprecatedKeys(...)` 里查到对应关系，例如 `StateBackendOptions.STATE_BACKEND`、`CheckpointingOptions`。

### 10.6 参数写在哪：三种方式的边界

| 方式 | 生效范围 | 适用场景 |
|---|---|---|
| `conf/flink-conf.yaml` | 整个集群 | 集群级默认（HA、内存、TM slot、metrics） |
| CLI `-D` / `-p` / `-t` … | 本次提交 | 单作业差异化配置，**最常用** |
| 代码内 `env.configure(...)` / `env.enableCheckpointing(...)` | 该作业 | 跟着 jar 走的配置，避免运维漏配 |

关于第三种方式，有一个值得记住的链路：`StreamExecutionEnvironment.configuration`（`configure()` 在 `:1121` 做 `addAll`）会经

```
StreamGraphGenerator.generate()  →  new StreamGraph(configuration, ...)   :276
StreamingJobGraphGenerator       →  jobGraph.setJobConfiguration(streamGraph.getJobConfiguration())   :313
```

**搭上 `JobGraph` 的作业级配置**，到集群侧作为作业级覆盖生效。所以 `env.configure()` 里写和 `-D` 同名的 key 是有效的。

但要注意边界：作业级配置**改不了 TaskManager 的进程启动参数**（进程内存、`taskmanager.numberOfTaskSlots` 等），那些必须在集群侧配置。

### 10.7 三个可直接抄的模板

最精简的一组参数见 §10.1；下面是三个完整场景。

**A. 本地调试（IDE 之外的最快方式）**

```bash
FLINK_HOME/bin/flink run \
  -t local \
  -p 2 \
  -c com.example.MyJob \
  ./target/my-job.jar arg1 arg2
```

**B. Session 集群：attached + 从 savepoint 恢复 + 打开 checkpoint**

```bash
FLINK_HOME/bin/flink run \
  -t remote -m jobmanager-host:8081 \
  -p 4 \
  -s hdfs:///flink/savepoints/savepoint-3f2a1b-9c8d \
  -cm claim \
  -D execution.checkpointing.interval=30s \
  -D execution.checkpointing.timeout=10min \
  -D execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION \
  -D execution.checkpointing.dir=hdfs:///flink/checkpoints \
  -D state.backend.type=rocksdb \
  -D execution.checkpointing.incremental=true \
  -D restart-strategy.type=fixed-delay \
  -D restart-strategy.fixed-delay.attempts=10 \
  -D restart-strategy.fixed-delay.delay=10s \
  ./target/my-job.jar
```

**C. Application 模式（作业自己带配置，client 提交完即退出）**

```bash
FLINK_HOME/bin/flink run-application \
  -t yarn-application \
  -D yarn.application.name=my-job \
  -D parallelism.default=8 \
  -D execution.checkpointing.interval=60s \
  -D execution.checkpointing.dir=hdfs:///flink/checkpoints \
  hdfs:///jars/my-job.jar
```

### 10.8 关于 `FLINK_HOME/bin/flink run app.jar` 这个最朴素的形式

```bash
FLINK_HOME/bin/flink run app.jar
```

它等价于：

```
① flink-conf.yaml 全部生效
② 没有 -t、flink-conf.yaml 里也没有 execution.target → GenericCLI 不激活
   → DefaultCLI 激活 → 强制 execution.target=remote
③ 没有 -p/-d/-s → attached 模式，并行度用 parallelism.default
④ jar 路径 = app.jar，入口类从 jar 的 manifest 的 Main-Class 读取
```

所以这条命令的**隐含语义是「attached 提交到配置文件里那个 session 集群」**。它跑不起来一般就三个原因：

1. 没有正在运行的 session 集群（`execution.target=remote` 但连不上 JobManager）——先用 `FLINK_HOME/bin/start-cluster.sh` 或在 `flink-conf.yaml` 里配好地址；
2. jar 的 manifest 里没有 `Main-Class`——补 `-c com.example.Main`，或改用 `flink run -c`；
3. 想跑本地却在连集群——加 `-t local`。

---

## 11. 易错点与记忆锚点

1. **`JobGraph` ≠ `ExecutionGraph`**。前者客户端生成、可 Java 序列化、静态；后者 JobManager 生成、含状态机与 attempt、随 failover 演变。
2. **算子链在客户端就完成了**（`StreamingJobGraphGenerator`），不是 JobManager 侧。所以 `JobGraph` 的 vertex 数量 ≠ 算子数量。
3. **`JobGraph` 里没有 jar 内容，只有 blob key**。jar 在 `JobSubmitHandler.uploadJobGraphFiles()` 时才真正进 BlobServer。
4. **`Dispatcher` 不做调度**，只做「登记 + 落盘 + 起 runner」。调度是 `JobMaster` / `SchedulerNG` 的活。
5. **`JobMaster` 构造时就建好了 `ExecutionGraph`**，`start()` 只是启动服务与调度。
6. **attached/detached 的差异体现在两处**：`AbstractSessionClusterExecutor` 里是否 `waitUntilJobInitializationFinished`，以及 client 进程是否 `close()` 后退出。
7. **`execution.target` 决定一切分支**。看代码卡住时，先把最终 `Configuration` 打出来。
8. **application 模式没有客户端侧的 `JobGraph` 生成**，`JobGraph` 在 JM 进程内产生。
9. **配置优先级是「三段叠加、后者胜」**：`flink-conf.yaml` < `-D`/`-t` < `-p`/`-d`/`-s`。所以 `-p 4 -D parallelism.default=2` 的结果是 4，不是 2。
10. **不写 `-t` 时 `DefaultCLI` 会强制 `execution.target=remote`**，即"默认提交到 session 集群"，这是 `flink run app.jar` 的隐含语义。
11. **1.20 有一批配置项改名**（`state.backend`→`state.backend.type`、`state.checkpoints.dir`→`execution.checkpointing.dir`、`restart-strategy`→`restart-strategy.type` 等），旧名靠 `withDeprecatedKeys` 兼容，但看老教程时容易对不上。
12. **内存 / slot / 核数是「集群侧」参数，只在集群随作业创建时（per-job、application 模式）才由 `-D` 生效**；提交到已运行的 session 集群时静默无效，不报错。作业级参数（`-p`、checkpoint 间隔、状态后端）则任何模式都生效。

---

## 12. 后续模块学习清单（本文档系列的下一步）

- [ ] **ExecutionGraph 与状态机**：`ExecutionVertex` 状态迁移、failover、region 重调度
- [ ] **调度与资源**：`SlotSharingGroup`、`SlotPool`、`ResourceManager`、slot 分配策略
- [ ] **Checkpoint 机制**：`CheckpointCoordinator`、barrier 对齐、`CheckpointBarrierHandler`
- [ ] **网络栈**：`ResultPartition` / `InputGate` / `NetworkBufferPool`、反压
- [ ] **State Backend**：`KeyedStateBackend`、RocksDB、状态恢复
- [ ] **故障恢复与 HA**：`JobGraphStore`、`LeaderContender`、`JobResultStore`
- [ ] **Table Planner**：`Transformation` 生成、codegen

---

## 附录 A：`bin/flink run` 主干调用栈（速查）

```
CliFrontend.main(String[])
 └─ CliFrontend.run(String[])                                  CliFrontend.java:222
     ├─ ProgramOptions.create(CommandLine)                     :237
     ├─ getJobJarAndDependencies(programOptions)               :239
     ├─ getEffectiveConfiguration(...)                         :241
     ├─ getPackagedProgram(programOptions, effectiveConfiguration)  :266
     │   └─ buildProgram(...)                                  :1046
     └─ executeProgram(effectiveConfiguration, program)        :1024
         └─ ClientUtils.executeProgram(DefaultExecutorServiceLoader, ...)   ClientUtils.java:77
             ├─ ContextEnvironment.setAsContext(...)
             ├─ StreamContextEnvironment.setAsContext(...)
             └─ PackagedProgram.invokeInteractiveModeForExecution()        PackagedProgram.java:220
                 └─ [用户 main()]
                     └─ StreamExecutionEnvironment.execute()                :2302
                         └─ execute(StreamGraph)                            :2350
                             └─ executeAsync(StreamGraph)                   :2467
                                 ├─ getPipelineExecutor()                   :2989
                                 │   └─ DefaultExecutorServiceLoader.getExecutorFactory(config)
                                 └─ PipelineExecutor.execute(pipeline, config, classLoader)
                                     ├─ PipelineExecutorUtils.getJobGraph(...)                  [Pipeline → JobGraph]
                                     │   └─ FlinkPipelineTranslationUtil.getJobGraph(...)
                                     │       └─ StreamGraphTranslator.translateToJobGraph(...)
                                     │           └─ StreamingJobGraphGenerator.createJobGraph()
                                     ├─ ClusterDescriptor.retrieve(clusterId)                   [连集群]
                                     └─ ClusterClient.submitJob(jobGraph)
                                         └─ RestClusterClient.submitJob(...)                    RestClusterClient.java:358
                                             └─ POST /jobs (multipart: jobgraph.bin + jars)
```

## 附录 B：JobManager 侧主干调用栈（速查）

```
WebMonitorEndpoint (REST 路由)
 └─ JobSubmitHandler.handleRequest(...)                        JobSubmitHandler.java
     ├─ loadJobGraph(...)                                     ObjectInputStream → JobGraph
     ├─ uploadJobGraphFiles(gateway, ..., jarFiles, ...)       jar → BlobServer，回写 blob key
     └─ gateway.submitJob(jobGraph, timeout)
         └─ Dispatcher.submitJob(jobGraph, timeout)            Dispatcher.java:518
             └─ internalSubmitJob(jobGraph)                    :612
                 └─ persistAndRunJob(jobGraph)                 :651
                     ├─ jobGraphWriter.putJobGraph(jobGraph)
                     ├─ initJobClientExpiredTime(jobGraph)
                     └─ runJob(createJobMasterRunner(jobGraph), SUBMISSION)   :681
                         ├─ jobManagerRunner.start()           JobMasterServiceLeadershipRunner
                         │   └─ DefaultJobMasterServiceProcess
                         │       └─ DefaultJobMasterServiceFactory.createJobMasterService(...)
                         │           └─ new JobMaster(...)
                         │               ├─ createScheduler(...)           JobMaster.java:397
                         │               │   └─ DefaultSchedulerFactory.createInstance(...)
                         │               │       └─ new DefaultScheduler(...)
                         │               │           └─ SchedulerBase
                         │               │               └─ DefaultExecutionGraphFactory
                         │               │                   .createAndRestoreExecutionGraph(...)
                         │               │                   └─ DefaultExecutionGraphBuilder.buildGraph(...)
                         │               └─ startJobExecution()             :1138
                         │                   ├─ startJobMasterServices()    :1155
                         │                   │   ├─ slotPoolService.start(...)
                         │                   │   └─ resourceManagerLeaderRetriever.start(...)
                         │                   └─ startScheduling()           :1234
                         │                       └─ schedulerNG.startScheduling()
                         │                           └─ DefaultScheduler.startSchedulingInternal()  :234
                         │                               └─ PipelinedRegionSchedulingStrategy.startScheduling()  :182
                         │                                   └─ SchedulerOperations.allocateSlotsAndDeploy(...)
                         │                                       └─ DefaultScheduler.allocateSlotsAndDeploy()   :466
                         │                                           └─ ExecutionDeployer
                         │                                               └─ ExecutionSlotAllocator
                         │                                                   └─ PhysicalSlotProviderImpl
                         │                                                       └─ SlotPool(DeclarativeSlotPoolBridge)
                         │                                                           └─ ResourceManager.declareRequiredResources(...)
                         │                                                               └─ FineGrainedSlotManager
                         │                                                                   └─ TaskExecutor.requestSlot(...)  → submitTask(...)
                         └─ jobManagerRunnerRegistry.register(runner)
```
