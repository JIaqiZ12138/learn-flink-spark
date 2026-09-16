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

# Flink 任务提交解析：YARN per-job 模式

> 代码基线：本仓库 `1.20-SNAPSHOT`，当前分支 `release-1.20`（commit `fb230abd439`）
> 主线命令：`FLINK_HOME/bin/flink run -t yarn-per-job app.jar`
> 行号对应当前 checkout，版本升级后可能有漂移；定位时优先 `grep` 方法名，不要死记行号。

---

## 0. 全局图景：一次 per-job 提交经过哪几个 JVM

per-job 的本质是「**一作业一集群**」，所以一次提交会跨越 **3 类进程**、经历 **2 次 JobGraph 序列化**：

```
┌────────────── 客户端 JVM ──────────────┐
│ bin/flink                              │
│  → CliFrontend.run()                   │
│  → PackagedProgram（加载用户 jar）      │
│  → 用户 main()   ★ 在这里执行！         │
│      → env.execute()                   │
│        → StreamGraph（逻辑图）          │
│        → JobGraph  ★ 在这里生成！       │
│  → YarnClusterDescriptor.deployJobCluster()
│      → JobGraph ① Java 序列化到临时文件  │
│      → 注册为 YARN local resource       │
└───────────────┬────────────────────────┘
                │ YARN 提交应用（ApplicationId）
                ▼
┌────────── ApplicationMaster 容器 ──────┐
│ YarnJobClusterEntrypoint               │
│  → FileJobGraphRetriever               │
│      ② ObjectInputStream 反序列化 ★     │
│  → JobDispatcherFactory → MiniDispatcher
│  → Dispatcher.startRecoveredJobs()     │
│  → JobManagerRunner → JobMaster        │
│      → DefaultScheduler                │
│      → ExecutionGraph（执行图）         │
└───────────────┬────────────────────────┘
                │ declareRequiredResources / 请求容器
                ▼
┌────────── TaskManager 容器（N 个）──────┐
│ YarnTaskExecutorRunner                 │
│  → TaskExecutor.submitTask()           │
│  → Task（真正跑用户算子）                │
└────────────────────────────────────────┘
        作业结束后整个 Application 被销毁
```

**记住两个"最"**：

1. **`main()` 和 `JobGraph` 都在客户端**——这是 per-job 与 application 模式最根本的分野；
2. **JobGraph 不走 HTTP**——它是 Java 序列化后塞进 YARN 的 local resource 随容器分发的，JobManager 侧压根没有 `JobSubmitHandler` 参与。

---

## 1. per-job 模式的定位

### 1.1 它是什么

`YarnDeploymentTarget.PER_JOB`：

```java
// flink-yarn/.../configuration/YarnDeploymentTarget.java:36
@Deprecated
PER_JOB("yarn-per-job");
```

官方文档的一句话定义：

> The Per-job Cluster mode will launch a Flink cluster on YARN, then **run the provided application jar locally** and finally submit the JobGraph to the JobManager on YARN. The YARN cluster will stop once the job has stopped.

「run locally」指的就是 **main() 在客户端执行**——这是理解后面所有差异的钥匙。

### 1.2 生命周期

```
客户端提交 ──► YARN 创建 Application（AM 容器 + N 个 TM 容器）
                    │
                    ├─ 作业运行
                    │
               作业结束（成功/失败/取消）──► 整个 Application 销毁，资源全部回收
```

隔离性来自"资源只给这一个作业"，代价是每个作业都要付一次集群启动开销（容器申请 + JVM 启动 + 分发 flink dist）。

### 1.3 废弃时间线（重要，别踩坑）

| 时间 | 版本 | 事件 | 提交 |
|---|---|---|---|
| 2022-02-11 | **1.15** | FLINK-25999 **废弃** | `47c5aae7456` |
| 2024-12-30 | **2.0-SNAPSHOT** | FLINK-36310 **移除**（-2738 行） | `004226ad1fe` |
| — | **release-1.20**（当前分支） | **仍可用**，只标 `@Deprecated` | — |

**1.20 上 per-job 完全能用**，没有任何硬阻断。唯一的信号是运行时一行告警：

```java
// YarnJobClusterEntrypoint.java:75
LOG.warn("Job Clusters are deprecated since Flink 1.15. Please use an Application Cluster/Application Mode instead.");
```

以及 `flink run -h` 里显示 `"yarn-per-job" (deprecated)`。

### 1.4 为什么被废弃（读代码时会不断撞见这个问题的答案）

| 理由 | 依据 |
|---|---|
| **application mode 完全覆盖其价值** | 文档：application mode "provides **the same resource isolation and load balancing guarantees as the Per-Job mode**" |
| **客户端太重** | 文档：main() 在客户端跑，要下载依赖、做 DAG 翻译与代码生成、上传二进制，"**This makes the Client a heavy resource consumer**"，"more pronounced when the Client is shared across users" |
| **只在 YARN 上可用** | 文档原文：**"Per-job mode is only supported by YARN"**；K8s 从来没有 per-job |
| **代码维护面** | FLINK-36310 删掉 54 个文件 / 2738 行，含 per-job 专用的 `MiniDispatcher`、`JobDispatcherFactory`、`JobClusterEntrypoint`、`YarnJobClusterEntrypoint`、`AbstractJobClusterExecutor` 与一批 ITCase |

### 1.5 三种模式的决定性差异（一张表记住）

| | **per-job** | session | application |
|---|---|---|---|
| `main()` 在哪跑 | **客户端** | 客户端 | **JobManager** |
| `JobGraph` 在哪生成 | **客户端** | 客户端 | **JobManager** |
| JobGraph 如何到集群 | **Java 序列化 → `job.graph` 文件 → YARN local resource** | Java 序列化 → 临时文件 → **REST `POST /jobs`** | **不传**，进程内 `dispatcherGateway.submitJob()` |
| 集群何时创建 | 每作业新建 | 提前建好、长期存在 | 每应用新建 |
| 集群何时销毁 | 作业结束 | 手动 | 应用结束 |
| 一个集群跑几个作业 | 1 | N | 1 个 `main()` 里的 N 个 |
| PipelineExecutor（客户端） | `YarnJobClusterExecutor` | `YarnSessionClusterExecutor` | **无** |
| PipelineExecutor（JM 内） | 无 | 无 | `EmbeddedExecutor` |

> 有意思的是：`PipelineExecutorUtils.getJobGraph(...)` 这个"翻译入口"在三种模式里**都会被调用**，唯一变的是它运行在哪个 JVM。读代码时看到这个调用，先问自己「我现在在客户端还是 JobManager 里」。

---

## 2. 阶段一：客户端 —— 从命令行到 `JobGraph`

### 2.1 脚本入口

`flink-dist/src/main/flink-bin/bin/flink` 末尾：

```bash
exec "${JAVA_RUN}" $JVM_ARGS ... -classpath "..." org.apache.flink.client.cli.CliFrontend "$@"
```

**第一个断点**：`flink-clients/src/main/java/org/apache/flink/client/cli/CliFrontend.java`

### 2.2 CLI 解析：为什么 `-t` 决定了后面一切

`CliFrontend.loadCustomCommandLines()`（`:1422`）按固定顺序注册三个 CustomCommandLine，`validateAndGetActiveCommandLine()`（`:1466`）返回**第一个 `isActive()` 为 true 的**：

| 顺序 | 实现 | `isActive()` 条件 |
|---|---|---|
| 1 | **`GenericCLI`** | 传了 `-t`/`-e`，**或** flink-conf.yaml 里已有 `execution.target` |
| 2 | `FlinkYarnSessionCli`（前缀 `y`/`yarn`） | 传了 `-y*` 参数或 `-m yarn-cluster` |
| 3 | `DefaultCLI` | 永远 true（兜底，所以必须放最后） |

因为我们传了 `-t yarn-per-job`，**`GenericCLI` 被激活**，它做两件事：

```java
// GenericCLI.toConfiguration()
resultConfiguration.set(DeploymentOptions.TARGET, "yarn-per-job");                 // execution.target
DynamicPropertiesUtil.encodeDynamicProperties(commandLine, resultConfiguration);   // 所有 -D
```

⚠️ **如果忘记写 `-t`**，兜底的是 `DefaultCLI`，它会**无条件设置 `execution.target=remote`**——于是会莫名其妙地"提交到 session 集群"。这是 per-job 提交最常见的翻车点。

配置优先级（三段叠加、后者胜，`CliFrontend.java:294`）：

```
flink-conf.yaml  <  -D / -t / -m / -z  <  -p / -d / -s / -C / -sae
```

### 2.3 装载用户程序

```
CliFrontend.run()                        :222
  ├─ ProgramOptions.create(commandLine)  :237
  ├─ getEffectiveConfiguration(...)      :241   ← 上面 2.2 的合并发生在这里
  ├─ getPackagedProgram(...)             :266 → buildProgram() :1046
  └─ executeProgram(effectiveConfiguration, program)  :1024
```

`PackagedProgram`（`flink-clients/.../client/program/PackagedProgram.java`）持有：
- 用户 jar 的 URL
- main class 名（`-c` 或 jar manifest 的 `Main-Class`）
- 程序参数
- **独立的 child-first user code classloader**（`FlinkUserCodeClassLoaders`）

### 2.4 ★ 用户 `main()` 在客户端执行

```
CliFrontend.executeProgram()            :1024
  → ClientUtils.executeProgram(new DefaultExecutorServiceLoader(), configuration, program, false, false)
      // flink-clients/.../client/ClientUtils.java:77
      ├─ Thread.currentThread().setContextClassLoader(userCodeClassLoader)
      ├─ ContextEnvironment.setAsContext(...)          ← ThreadLocal 注入
      ├─ StreamContextEnvironment.setAsContext(...)    ← 关键
      ├─ ExecutionContextEnvironment.setAsContext(...) ← DataStream v2
      └─ PackagedProgram.invokeInteractiveModeForExecution()   :220
            └─ ★★★ 反射调用用户 main() ★★★
```

因为注入了 `StreamContextEnvironment`，用户代码里这句：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```

拿到的是 `StreamContextEnvironment` 实例（而不是 `LocalStreamEnvironment`），它携带了 `PipelineExecutorServiceLoader`。**这就是「CLI 能决定用户程序怎么提交」的机制。**

### 2.5 `PipelineExecutor` 的选择：SPI 命中 `YarnJobClusterExecutor`

```
env.execute()                                  StreamExecutionEnvironment.java:2302
  → execute(String jobName)                    :2317
    → getStreamGraph()                         :2499
    → execute(StreamGraph)                     :2350
      → executeAsync(StreamGraph)              :2467
        → getPipelineExecutor()                :2989
            ├─ executorServiceLoader.getExecutorFactory(configuration)
            │    → DefaultExecutorServiceLoader.getExecutorFactory()
            │        → ServiceLoader.load(PipelineExecutorFactory.class)
            └─ executorFactory.getExecutor(configuration)
```

SPI 注册文件 `flink-yarn/src/main/resources/META-INF/services/org.apache.flink.core.execution.PipelineExecutorFactory`：

```
org.apache.flink.yarn.executors.YarnJobClusterExecutorFactory
org.apache.flink.yarn.executors.YarnSessionClusterExecutorFactory
```

两个工厂都注册了，靠 `isCompatibleWith` 里的 target 字符串二选一：

```java
// YarnJobClusterExecutorFactory.isCompatibleWith()
return "yarn-per-job".equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));   // ✅ 命中
```

于是得到：

```java
// YarnJobClusterExecutor（@Deprecated）
public class YarnJobClusterExecutor
        extends AbstractJobClusterExecutor<ApplicationId, YarnClusterClientFactory> {
    public static final String NAME = YarnDeploymentTarget.PER_JOB.getName();  // "yarn-per-job"
    public YarnJobClusterExecutor() { super(new YarnClusterClientFactory()); }
}
```

**继承关系是关键分水岭**：

```
AbstractSessionClusterExecutor（"集群已存在，我去连它"）
├─ RemoteExecutor                     remote
├─ YarnSessionClusterExecutor         yarn-session
└─ KubernetesSessionClusterExecutor   kubernetes-session

AbstractJobClusterExecutor（"为我新建一个集群"）—— 只有这一个子类
└─ YarnJobClusterExecutor             yarn-per-job   ← 我们在的位置
```

### 2.6 `StreamGraph` → `JobGraph`（客户端侧最重的一块）

`StreamGraphTranslator` 只是薄薄一层转发：

```
PipelineExecutorUtils.getJobGraph(pipeline, configuration, userClassLoader)
  → FlinkPipelineTranslationUtil.getJobGraph(...)
      → getPipelineTranslator(...)          // 流作业 → StreamGraphTranslator
          → StreamGraphTranslator.translateToJobGraph(...)
              → StreamGraph.getJobGraph(userCodeLoader, null)
                  → StreamingJobGraphGenerator.createJobGraph()   ★ 真正的转换
```

`StreamingJobGraphGenerator.createJobGraph()` 是核心，按职责分四段：

| 段 | 做什么 |
|---|---|
| **初始化** | `preValidate()` 校验；设置 JobType / dynamic / 近似本地恢复 |
| **翻译 DAG** | ③ 生成确定性 hash；④ **`setChaining(hashes, legacyHashes)` ← 算子链在这里产生** |
| **图定型后修正** | non-chained outputs 配置 → `setPhysicalEdges()` → `setSlotSharingAndCoLocation()` → 托管内存 → checkpoint |
| **收尾** | ExecutionConfig（必须最后设置）→ JobConfiguration → 等待异步序列化 → 返回 |

#### 算子链（operator chaining）要点

**原理**：满足条件的相邻算子被合并进**同一个 `JobVertex`**，运行时表现为**同一个线程里串联执行的算子串**（`OperatorChain`），算子间直接传对象，不过序列化和网络。所以 `JobVertex` 数量**少于** `StreamNode` 数量。

**算法**（一次 DFS 同时完成分链和建边）：

```
setChaining()
  ├─ buildChainedInputsAndGetHeadInputs()   // 分出"链头 source" 与 "作 chained input 的 source"
  └─ 对每个链头（按 id 排序，保证确定性）调用
       createChain(startNodeId, 1, chainInfo, chainEntryPoints)
            ↑ chainIndex 从 1 开始，0 留给 chained source input

createChain(currentNodeId, chainIndex, chainInfo, ...)   // 递归
  └─ 遍历出边，分两组：
       chainable    → 递归时沿用同一个 chainInfo，chainIndex+1    ⇒ 同一个 JobVertex
       nonChainable → chainInfo.newChain(targetId)，index 重置为 1 ⇒ 新 JobVertex
                      并把这条边加入 transitiveOutEdges
```

两个细节：

- **`transitiveOutEdges` 必须一路向上冒泡到链头**——因为 `JobEdge` 的起点只能是最外层 `JobVertex`。这就是 `createChain` 返回 `List<StreamEdge>` 而非 void 的原因。
- **链头 vs 链内节点的 `StreamConfig` 构造方式不同**：链头走 `createJobVertex()` 真建 `JobVertex`；链内只 `new StreamConfig(...)` 然后塞进 `chainedConfigs`，最后由链头通过 `setTransitiveChainedTaskConfigs(...)` 携带全链配置。这就是"一个 `JobVertex` 怎么装下多个算子"的答案。

**断链规则全在 `isChainable(edge, streamGraph)`**：

| # | 规则 | 为什么 |
|---|---|---|
| 1 | 下游只能有 1 条入边 | join/co-process 需要多输入通道 |
| 2 | `pipeline.operator-chaining.enabled` 全局开关 | 可整体关闭 |
| 3 | 上下游同一 `SlotSharingGroup` | 不同组的算子会落到不同 slot |
| 4 | `ChainingStrategy` 组合允许 | `NEVER`/`HEAD` 独占 vertex |
| 5 | **并行度必须相同** | 链内一一对应传记录，个数必须相等 |
| 6 | maxParallelism 相同（可开关放宽） | 链内 key group 划分 |
| — | **非 `ForwardPartitioner` 一律断链** | 重分区必须走网络栈 |
| — | union 特例：同输入位置多条入边不可链 | 源码注释：*"unions currently only work through the network/byte-channel stack"* |

`ChainingStrategy` 四值：`ALWAYS`（默认）/ `NEVER` / `HEAD` / `HEAD_WITH_SOURCES`。

#### hash 与 savepoint 的关系

`createJobGraph` ③ 用 `defaultStreamGraphHasher` 给每个 `StreamNode` 算**确定性 hash**，两个用途：

1. `createChain` → `createJobVertex()` 用 `chainInfo.getHash(streamNodeId)` 构造 **`JobVertexID`**；
2. savepoint 里状态与算子的映射。

所以「改算子结构 → hash 变 → `JobVertexID` 变 → savepoint 恢复失败」这条运维规则的根源就在这里。同时还会生成 legacy hashes 兼容老 savepoint（新版 hasher 上线后不能改算法，只能新增）。

### 2.7 `PipelineExecutorUtils` 补充的作业级信息

`flink-clients/.../deployment/executors/PipelineExecutorUtils.java`：

```java
jobGraph.setJobID(...)                                          // 来自 $internal.pipeline.job-id，可固定 jobId
jobGraph.setInitialClientHeartbeatTimeout(...)                  // attached + shutdown-if-attached 才有
jobGraph.addJars(executionConfigAccessor.getJars());            // pipeline.jars
jobGraph.setClasspaths(executionConfigAccessor.getClasspaths()); // pipeline.classpaths
jobGraph.setSavepointRestoreSettings(...);                      // -s / -n / -cm
```

**`JobGraph` 不只是"图"，它是作业提交的完整信封**——用户 jar、classpath、savepoint 恢复设置、序列化的 ExecutionConfig 全挂在上面。

---

## 3. 阶段二：客户端 → YARN

### 3.1 `AbstractJobClusterExecutor.execute()` 全解

`flink-clients/src/main/java/org/apache/flink/client/deployment/executors/AbstractJobClusterExecutor.java:66`

```java
public CompletableFuture<JobClient> execute(
        @Nonnull final Pipeline pipeline,
        @Nonnull final Configuration configuration,
        @Nonnull final ClassLoader userCodeClassloader) throws Exception {

    // ① ★ JobGraph 在客户端生成（完整的算子链计算也在这里）
    final JobGraph jobGraph =
            PipelineExecutorUtils.getJobGraph(pipeline, configuration, userCodeClassloader);

    try (final ClusterDescriptor<ClusterID> clusterDescriptor =
            clusterClientFactory.createClusterDescriptor(configuration)) {   // ② YarnClusterDescriptor

        final ExecutionConfigAccessor configAccessor =
                ExecutionConfigAccessor.fromConfiguration(configuration);    // ③ 取 attached/detached

        final ClusterSpecification clusterSpecification =
                clusterClientFactory.getClusterSpecification(configuration); // ④ JM/TM 容器规格

        // ⑤ ★ 新建集群 + 把 JobGraph 一起交出去
        final ClusterClientProvider<ClusterID> clusterClientProvider =
                clusterDescriptor.deployJobCluster(
                        clusterSpecification, jobGraph, configAccessor.getDetachedMode());
        LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());

        // ⑥ 立刻返回，不等作业结束
        return CompletableFuture.completedFuture(
                new ClusterClientJobClientAdapter<>(
                        clusterClientProvider, jobGraph.getJobID(), userCodeClassloader));
    }
}
```

**与 session 模式的结构差异**（对照 `AbstractSessionClusterExecutor`）：

| | per-job | session |
|---|---|---|
| 集群获取 | `deployJobCluster(...)` **新建** | `retrieve(clusterId)` **连已有** |
| 提交方式 | 集群创建时把 `JobGraph` 一起交出去 | `clusterClient.submitJob(jobGraph)` 走 REST |
| 等初始化 | **不等**，直接 `completedFuture(...)` | attached 时 `waitUntilJobInitializationFinished(...)` |
| 关闭 client | 不关（集群归作业所有） | `whenCompleteAsync((i1,i2) -> clusterClient.close())` |

返回的 `ClusterClientJobClientAdapter` 是 `JobClient` 的适配器：它只存 `ClusterClientProvider` + `jobID`，每次操作（`cancel()`/`getJobStatus()`）临时取一个 `ClusterClient`、用完即 `close()`——**短连接式**。

### 3.2 `deployJobCluster` → `deployInternal`

`flink-yarn/src/main/java/org/apache/flink/yarn/YarnClusterDescriptor.java:553`

```java
public ClusterClientProvider<ApplicationId> deployJobCluster(
        ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached)
        throws ClusterDeploymentException {

    LOG.warn("Job Clusters are deprecated since Flink 1.15. Please use an Application Cluster/Application Mode instead.");
    try {
        return deployInternal(
                clusterSpecification,
                "Flink per-job cluster",
                getYarnJobClusterEntrypoint(),      // ← AM 容器的启动类
                jobGraph,
                detached);
    } catch (Exception e) {
        throw new ClusterDeploymentException("Could not deploy Yarn job cluster.", e);
    }
}
```

```java
// YarnClusterDescriptor.java:320
protected String getYarnJobClusterEntrypoint() {
    return YarnJobClusterEntrypoint.class.getName();
}
```

真正的重活在 `deployInternal(...)`（`:600`）。

### 3.3 ★ JobGraph 怎么过去：Java 序列化 → `job.graph` → YARN local resource

`YarnClusterDescriptor.java:1057` —— **这是 per-job 模式最独特的一段代码**：

```java
// write job graph to tmp file and add it to local resource
// TODO: server use user main method to generate job graph     ← ★★ 整个演进史的伏笔，见 §3.5
if (jobGraph != null) {
    File tmpJobGraphFile = null;
    try {
        tmpJobGraphFile = File.createTempFile(appId.toString(), null);
        try (FileOutputStream output = new FileOutputStream(tmpJobGraphFile);
                ObjectOutputStream obOutput = new ObjectOutputStream(output)) {
            obOutput.writeObject(jobGraph);                       // ① Java 序列化
        }

        final String jobGraphFilename = "job.graph";
        configuration.set(JOB_GRAPH_FILE_PATH, jobGraphFilename);  // ② internal.jobgraph-path

        fileUploader.registerSingleLocalResource(                  // ③ 注册为 YARN local resource
                jobGraphFilename,
                new Path(tmpJobGraphFile.toURI()),
                "",                        // relativeDstPath：落到容器工作目录根
                LocalResourceType.FILE,
                true,                      // whetherToAddToRemotePaths
                false);                    // whetherToAddToEnvShipResourceList
        classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
    } catch (Exception e) {
        LOG.warn("Add job graph to local resource fail.");
        throw e;
    } finally {
        if (tmpJobGraphFile != null && !tmpJobGraphFile.delete()) {
            LOG.warn("Fail to delete temporary file {}.", tmpJobGraphFile.toPath());
        }
    }
}
```

要点：

1. **用的是 Java 序列化**（`ObjectOutputStream.writeObject`）——这也是 Flink 内部唯一被允许使用 Java 序列化的场景；
2. **走 YARN 的 local resource 机制**，不用 HTTP、不用 BlobServer 上传；
3. **靠配置项传递路径**：`internal.jobgraph-path`（默认 `job.graph`），AM 侧据此找到文件；
4. 最后那个 `false` 是 `whetherToAddToEnvShipResourceList`——**不把 `job.graph` 放进 `_FLINK_SHIP_*` 环境变量**，因为它只需要给 AM 用，不需要再分发给 TM 容器。

### 3.4 AM 容器还带上了什么

`deployInternal` 里在 JobGraph 之外还会：

| 动作 | 代码 |
|---|---|
| 上传 Flink 发行版 jar | `fileUploader.uploadFlinkDist(flinkJarPath)`（`:1051`） |
| 上传 `flink-conf.yaml` | `tmpConfigurationFile` + 注册 local resource（`:1089` 起） |
| 上传用户 jar 与 ship files | `fileUploader` 相关逻辑 |
| 构造 AM 的 `ContainerLaunchContext` | `setupApplicationMasterContainer(yarnClusterEntrypoint, hasKrb5, processSpec)`（`:1206` / `:1853`） |
| 绑定 local resources 到容器 | `amContainer.setLocalResources(fileUploader.getRegisteredLocalResources())`（`:1217`） |
| 提交应用 | `yarnClient.submitApplication(...)` → 得到 `ApplicationId` |

`setupApplicationMasterContainer` 的入参 `String yarnClusterEntrypoint` 就是前面 `getYarnJobClusterEntrypoint()` 返回的类名——**AM 容器启动时执行的就是 `YarnJobClusterEntrypoint.main()`**。

### 3.5 那句决定历史的 TODO

```java
// TODO: server use user main method to generate job graph
```

**这就是 application mode 的由来。** 当年写下这个 TODO 的人想的是"让服务端用用户 `main()` 生成 JobGraph"，而这个 TODO 后来被 FLIP-85（Flink 1.11 引入 application mode）真正实现了——今天 `ApplicationDispatcherBootstrap` 就在 JobManager 里跑用户 `main()`。

对比 application 模式下生成 JobGraph 的代码（**同一个方法，不同的 JVM**）：

```java
// EmbeddedExecutor.java:122   ← 这个方法运行在 JobManager 进程里
final JobGraph jobGraph =
        PipelineExecutorUtils.getJobGraph(pipeline, configuration, userCodeClassloader);
```

---

## 4. 阶段三：ApplicationMaster 启动

### 4.1 `YarnJobClusterEntrypoint`

`flink-yarn/src/main/java/org/apache/flink/yarn/entrypoint/YarnJobClusterEntrypoint.java`

```java
/**
 * @deprecated Per-mode has been deprecated in Flink 1.15 and will be removed in the future.
 *     Please use application mode instead.
 */
@Deprecated
public class YarnJobClusterEntrypoint extends JobClusterEntrypoint {

    @Override
    protected DispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(Configuration configuration) throws IOException {
        return DefaultDispatcherResourceManagerComponentFactory.createJobComponentFactory(
                YarnResourceManagerFactory.getInstance(),
                FileJobGraphRetriever.createFrom(                 // ★ 关键：从文件读 JobGraph
                        configuration,
                        YarnEntrypointUtils.getUsrLibDir(configuration).orElse(null)));
    }

    public static void main(String[] args) {
        LOG.warn("Job Clusters are deprecated since Flink 1.15. Please use an Application Cluster/Application Mode instead.");
        ...
        runClusterEntrypoint(...);
    }
}
```

### 4.2 `FileJobGraphRetriever`：反序列化

`flink-runtime/src/main/java/org/apache/flink/runtime/entrypoint/component/FileJobGraphRetriever.java`

```java
@Internal
public static final ConfigOption<String> JOB_GRAPH_FILE_PATH =
        ConfigOptions.key("internal.jobgraph-path").stringType().defaultValue("job.graph");

@Override
public JobGraph retrieveJobGraph(Configuration configuration) throws FlinkException {
    final File fp = new File(jobGraphFile);
    try (FileInputStream input = new FileInputStream(fp);
            ObjectInputStream obInput = new ObjectInputStream(input)) {
        final JobGraph jobGraph = (JobGraph) obInput.readObject();      // ★ 反序列化
        addUserClassPathsToJobGraph(jobGraph);                          // 补上 usrlib 里的 classpath
        return jobGraph;
    } catch (FileNotFoundException e) {
        throw new FlinkException("Could not find the JobGraph file.", e);
    } catch (ClassNotFoundException | IOException e) {
        throw new FlinkException("Could not load the JobGraph from file.", e);
    }
}
```

注意 `addUserClassPathsToJobGraph`：把 AM 容器里 `usrlib` 目录的路径合并进 `JobGraph.classpaths`——因为 `JobGraph` 是**在客户端生成**的，那时还不知道集群侧的 `usrlib` 在哪。

### 4.3 ★★ 关键发现：per-job 的 JobGraph 不走 `submitJob` RPC

这是最容易误解的一点。per-job 模式下 **JobManager 侧完全没有 `JobSubmitHandler` / REST 提交**参与。

`createJobComponentFactory` 把 retriever 交给 `JobDispatcherFactory`：

```java
// DefaultDispatcherResourceManagerComponentFactory.java:305
public static DefaultDispatcherResourceManagerComponentFactory createJobComponentFactory(
        ResourceManagerFactory<?> resourceManagerFactory, JobGraphRetriever jobGraphRetriever) {
    return new DefaultDispatcherResourceManagerComponentFactory(
            DefaultDispatcherRunnerFactory.createJobRunner(jobGraphRetriever),
            resourceManagerFactory,
            JobRestEndpointFactory.INSTANCE);
}
```

```java
// JobDispatcherFactory.createDispatcher(...)
final JobGraph recoveredJobGraph = Iterables.getOnlyElement(recoveredJobs, null);
...
return new MiniDispatcher(
        rpcService, fencingToken,
        DispatcherServices.from(..., JobMasterServiceLeadershipRunnerFactory.INSTANCE, ...),
        recoveredJobGraph,          // ★ JobGraph 作为"已恢复作业"传入！
        recoveredDirtyJob,
        dispatcherBootstrapFactory,
        executionMode);
```

`MiniDispatcher` 的 javadoc 说得很清楚：

> The mini dispatcher is **initialized with a single `JobGraph`** which it runs.

而构造参数 `jobGraph` 最终被 `Dispatcher` 当成 `recoveredJobs`：

```java
// MiniDispatcher 构造函数
super(..., CollectionUtil.ofNullable(jobGraph), dispatcherBootstrapFactory, ...);
//                                    ↑ 作为 recoveredJobs
```

所以**整条路径是**：

```
FileJobGraphRetriever.retrieveJobGraph()
  → JobDispatcherFactory.createDispatcher(recoveredJobs = [jobGraph])
      → new MiniDispatcher(..., recoveredJobGraph, ...)
          → Dispatcher.startRecoveredJobs()      :397     ← 不是 submitJob()！
```

**对比三种模式的"JobGraph 入口"**：

| 模式 | JobGraph 进入 Dispatcher 的路径 |
|---|---|
| **per-job** | `FileJobGraphRetriever` → `MiniDispatcher` 构造参数 → `startRecoveredJobs()` |
| session / application | `JobSubmitHandler`（REST `POST /jobs`）或 `EmbeddedExecutor` → `Dispatcher.submitJob()` RPC |

> `MiniDispatcher` 确实也有 `submitJob()` 重写（`:103`），但那是给 HA 恢复/RPC 用的次要路径；**per-job 的正常路径是 `recoveredJobs`**。
>
> 顺带一提：在 2.x 里 `MiniDispatcher` 和 `JobDispatcherFactory` 都被 FLINK-36310 删掉了——**per-job 独有的这段"绕过 submitJob"逻辑，正是随 per-job 一起消失的东西**。

---

## 5. 阶段四：从 `JobGraph` 到 `ExecutionGraph`

### 5.1 `Dispatcher.startRecoveredJobs()`

```java
// Dispatcher.java:397
private void startRecoveredJobs() {
    for (JobGraph recoveredJob : recoveredJobs) {
        runRecoveredJob(recoveredJob);
    }
    recoveredJobs.clear();
}

// Dispatcher.java:404
private void runRecoveredJob(final JobGraph recoveredJob) {
    checkNotNull(recoveredJob);
    initJobClientExpiredTime(recoveredJob);
    try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(recoveredJob.getJobID()))) {
        runJob(createJobMasterRunner(recoveredJob), ExecutionType.RECOVERY);   // ★ RECOVERY
    } catch (Throwable throwable) {
        onFatalError(new DispatcherException(
                String.format("Could not start recovered job %s.", recoveredJob.getJobID()), throwable));
    }
}
```

⚠️ 注意 `ExecutionType.RECOVERY` —— per-job 的首次提交在语义上就是"恢复"，因为没有经过 `submitJob` 那条路，也就**没有 session 模式里 `persistAndRunJob` → `jobGraphWriter.putJobGraph()` 那个落盘点**。

### 5.2 `JobManagerRunner` → `JobMaster`

```
createJobMasterRunner(jobGraph)                    Dispatcher.java:659
  → JobMasterServiceLeadershipRunnerFactory.INSTANCE.createJobManagerRunner(...)
      → JobMasterServiceLeadershipRunner            （implements LeaderContender，参加选主）
          → DefaultJobMasterServiceProcess
              → DefaultJobMasterServiceFactory.createJobMasterService(...)
                  → new JobMaster(...)
```

为什么要多一层选主？因为 `JobMaster` 必须由"拿到领导权的那个 JobManager 进程"持有，`JobMasterId`（fencing token）用于防脑裂。非 HA 场景下这条链会短很多。

### 5.3 `ExecutionGraph` 的构建

```
JobMaster 构造函数                JobMaster.java:382
  → createScheduler(slotPoolServiceSchedulerFactory, ...)
      → DefaultSlotPoolServiceSchedulerFactory.createScheduler(...)
          → DefaultSchedulerFactory.createInstance(...)
              ├─ SlotPool slotPool = slotPoolService.castInto(SlotPool.class)
              ├─ createSchedulerComponents(...)        // pipelined / blocking 调度组件
              ├─ RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(...)
              ├─ new DefaultExecutionGraphFactory(...)
              └─ new DefaultScheduler(...)             → SchedulerBase
                  → DefaultExecutionGraphFactory.createAndRestoreExecutionGraph(...)
                      → DefaultExecutionGraphBuilder.buildGraph(...)
```

**注意时序**：`ExecutionGraph` 是在 `JobMaster` **构造期间**建好的，早于 `JobMaster.start()`。

`ExecutionGraph` 里展开成三层：`ExecutionJobVertex` / `ExecutionVertex` / `Execution`(attempt)，外加 `IntermediateResult` / `IntermediateResultPartition`。**重启策略、checkpoint 协调器、savepoint 状态恢复**都挂在这里。

### 5.4 开始调度

```
JobMaster.start()                  JobMaster.java
  → startJobExecution()            :1138
      ├─ shuffleMaster.registerJob(context)
      ├─ startJobMasterServices()  :1155
      │    ├─ createTaskManagerHeartbeatManager / createResourceManagerHeartbeatManager
      │    ├─ slotPoolService.start(fencingToken, address, mainThreadExecutor)
      │    └─ resourceManagerLeaderRetriever.start(...)   ← 异步连 RM，连上后才开始要 slot
      └─ startScheduling()         :1234
          → schedulerNG.startScheduling()
              → DefaultScheduler.startSchedulingInternal()              :234
                  → PipelinedRegionSchedulingStrategy.startScheduling() :182
                      → SchedulerOperations.allocateSlotsAndDeploy(...)
                          → DefaultScheduler.allocateSlotsAndDeploy()    :466
                              → ExecutionDeployer.allocateSlotsAndDeploy(...)
```

**默认调度策略是 pipelined region 调度**：把 `ExecutionGraph` 切成若干"流水线区域"（region 内是 all-or-nothing 一起调度），而不是一个 vertex 一个 vertex 地调度。

---

## 6. 阶段五：TaskManager 容器拉起

```
ExecutionSlotAllocator（如 SlotSharingExecutionSlotAllocator）
  → PhysicalSlotProviderImpl.allocatePhysicalSlots()
      → SlotPool / DeclarativeSlotPoolBridge            flink-runtime/.../jobmaster/slotpool/
          → DeclareResourceRequirementServiceConnectionManager
              → ResourceManager 声明资源需求
                  → FineGrainedSlotManager（1.20 唯一的 SlotManager 实现）
                      → YarnResourceManagerDriver.requestNewWorker(...)   ★ 向 YARN 要容器
                          → startTaskExecutorInContainerAsync(...)        :481
                              → 构造 ContainerLaunchContext
                                  启动类 = YarnTaskExecutorRunner             :563
                          → TaskExecutorGateway.requestSlot(...)          TaskExecutor.java:1183
                              → TaskExecutor.submitTask(...)              :660
                                  → new Task(...)  → 真正执行用户算子
```

至此，**从 `flink run` 到用户算子在 YARN 容器里跑起来**的完整链路闭环。

---

## 7. 完整调用栈速查

### 7.1 客户端侧

```
CliFrontend.main(String[])
 └─ CliFrontend.run(String[])                                    :222
     ├─ ProgramOptions.create(commandLine)                       :237
     ├─ getEffectiveConfiguration(...)                           :241   [GenericCLI: execution.target=yarn-per-job]
     ├─ getPackagedProgram(...)                                  :266 → buildProgram() :1046
     └─ executeProgram(...)                                      :1024
         └─ ClientUtils.executeProgram(...)                      ClientUtils.java:77
             ├─ StreamContextEnvironment.setAsContext(...)
             └─ PackagedProgram.invokeInteractiveModeForExecution()  PackagedProgram.java:220
                 └─ [用户 main()]
                     └─ StreamExecutionEnvironment.execute()     :2302
                         └─ executeAsync(StreamGraph)            :2467
                             ├─ getPipelineExecutor()            :2989
                             │   └─ DefaultExecutorServiceLoader → SPI
                             │       → YarnJobClusterExecutorFactory.isCompatibleWith ✅
                             └─ YarnJobClusterExecutor.execute(...)      [= AbstractJobClusterExecutor:66]
                                 ├─ PipelineExecutorUtils.getJobGraph(...)           ★ JobGraph 生成
                                 │   └─ FlinkPipelineTranslationUtil → StreamGraphTranslator
                                 │       → StreamingJobGraphGenerator.createJobGraph()
                                 │           ├─ preValidate()
                                 │           ├─ StreamGraphHasher.traverseStreamGraphAndGenerateHashes()
                                 │           └─ setChaining() → createChain() → isChainable()   ★ 算子链
                                 ├─ YarnClusterClientFactory.createClusterDescriptor(...)
                                 ├─ YarnClusterClientFactory.getClusterSpecification(...)
                                 └─ YarnClusterDescriptor.deployJobCluster(spec, jobGraph, detached)   :553
                                     └─ deployInternal(..., YarnJobClusterEntrypoint.class.getName(), jobGraph, ...)  :600
                                         ├─ fileUploader.uploadFlinkDist(...)                :1051
                                         ├─ ★ ObjectOutputStream.writeObject(jobGraph) → "job.graph"   :1057
                                         │    + configuration.set("internal.jobgraph-path", "job.graph")
                                         │    + fileUploader.registerSingleLocalResource(...)
                                         ├─ 上传 flink-conf.yaml
                                         ├─ setupApplicationMasterContainer(entrypointClass, ...)   :1206/1853
                                         └─ yarnClient.submitApplication(...) → ApplicationId
```

### 7.2 ApplicationMaster 侧

```
YarnJobClusterEntrypoint.main()                          [AM 容器]
 └─ runClusterEntrypoint(...)
     └─ createDispatcherResourceManagerComponentFactory(configuration)
         └─ DefaultDispatcherResourceManagerComponentFactory.createJobComponentFactory(...)   :305
             ├─ FileJobGraphRetriever.createFrom(configuration, usrLibDir)
             │    JOB_GRAPH_FILE_PATH = "internal.jobgraph-path"（默认 "job.graph"）
             └─ DefaultDispatcherRunnerFactory.createJobRunner(jobGraphRetriever)
                 → JobDispatcherFactory.createDispatcher(recoveredJobs = [jobGraph])
                     └─ new MiniDispatcher(..., recoveredJobGraph, ...)
                         └─ FileJobGraphRetriever.retrieveJobGraph()   ★ ObjectInputStream.readObject()
                             └─ Dispatcher.start() → startRecoveredJobs()        :397
                                 └─ runRecoveredJob(...)                        :404
                                     └─ runJob(createJobMasterRunner(jobGraph), ExecutionType.RECOVERY)   :411
                                         ├─ JobMasterServiceLeadershipRunner
                                         │   → DefaultJobMasterServiceProcess
                                         │       → DefaultJobMasterServiceFactory → new JobMaster(...)
                                         │           ├─ createScheduler(...)                    :382
                                         │           │   → DefaultSchedulerFactory.createInstance(...)
                                         │           │       → new DefaultScheduler(...) → SchedulerBase
                                         │           │           → DefaultExecutionGraphFactory
                                         │           │               .createAndRestoreExecutionGraph(...)
                                         │           │               → DefaultExecutionGraphBuilder.buildGraph(...)
                                         │           └─ startJobExecution()                     :1138
                                         │               ├─ startJobMasterServices()            :1155
                                         │               └─ startScheduling()                   :1234
                                         │                   → DefaultScheduler.startSchedulingInternal()  :234
                                         │                       → PipelinedRegionSchedulingStrategy.startScheduling()  :182
                                         │                           → allocateSlotsAndDeploy(...)    :466
                                         │                               → ExecutionDeployer
                                         │                                   → ExecutionSlotAllocator
                                         │                                       → PhysicalSlotProviderImpl
                                         │                                           → SlotPool / DeclarativeSlotPoolBridge
                                         │                                               → ResourceManager
                                         │                                                   → FineGrainedSlotManager
                                         │                                                       → YarnResourceManagerDriver.requestNewWorker()
                                         │                                                           → YarnTaskExecutorRunner 启动
                                         │                                                               → TaskExecutor.submitTask()
                                         │                                                                   → Task
                                         └─ jobManagerRunnerRegistry.register(runner)
```

---

## 8. yarn-per-job 的提交参数

### 8.1 常用组合

```bash
FLINK_HOME/bin/flink run \
  -t yarn-per-job \
  -p 8 \
  -Dyarn.application.name=my-job \
  -Djobmanager.memory.process.size=2048m \
  -Dtaskmanager.memory.process.size=4096m \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dyarn.appmaster.vcores=2 \
  -Dyarn.containers.vcores=4 \
  -Dexecution.checkpointing.interval=60s \
  -Dexecution.checkpointing.dir=hdfs:///flink/checkpoints \
  -Dstate.backend.type=rocksdb \
  ./app.jar
```

逐项对照（含自带 `flink-conf.yaml` 的默认值）：

| 想调什么 | 参数 | 默认 |
|---|---|---|
| JM 内存 | `-Djobmanager.memory.process.size=2048m` | `1600m` |
| TM 内存 | `-Dtaskmanager.memory.process.size=4096m` | `1728m` |
| 每个 TM 的 slot 数 | `-Dtaskmanager.numberOfTaskSlots=4` | `1` |
| JM 核数 | `-Dyarn.appmaster.vcores=2` | 1 |
| TM 核数 | `-Dyarn.containers.vcores=4` | `-1`（= slot 数） |
| checkpoint 间隔 | `-Dexecution.checkpointing.interval=60s` | 无默认值（不配就不做周期 checkpoint） |
| checkpoint 目录 | `-Dexecution.checkpointing.dir=...` | 无默认值 |
| 状态后端 | `-Dstate.backend.type=rocksdb` | `hashmap` |
| 并行度 | `-p 8` | `parallelism.default: 1` |

### 8.2 ★ 为什么 per-job 下这些"集群侧参数"能生效

内存 / slot / 核数属于**集群侧**参数，只在"集群随作业创建"时才由 `-D` 生效。**per-job 正好属于这一类**：

| 模式 | `-D jobmanager.memory...` 等是否生效 | 原因 |
|---|---|---|
| **per-job** | ✅ **生效** | AM/TM 容器为此作业新起，`YarnClusterDescriptor` 从配置里算容器规格 |
| `yarn-application` | ✅ 生效 | 同上 |
| `yarn-session` / `remote` | ❌ **静默无效** | 进程早跑起来了，提交参数改不了已运行的进程 |

而 `-p`、checkpoint 间隔、状态后端是**作业级**配置，跟着 `JobGraph` 走，任何模式都生效。

### 8.3 YARN 专属参数（前缀 `y`/`yarn`）

由 `FlinkYarnSessionCli` 提供（`loadCustomCommandLines()` 里以 `"y"`, `"yarn"` 前缀注册）：

| 参数 | 长选项 | 作用 |
|---|---|---|
| `-yD` | — | 等价 `-D`（YARN 专用形式） |
| `-yqu` | `--yarnqueue` | YARN 队列 |
| `-yjm` | `--yarnjobManagerMemory` | JM 容器内存 |
| `-ytm` | `--yarntaskManagerMemory` | TM 容器内存 |
| `-ys` | `--yarnslots` | 每个 TM 的 slot 数 |
| `-ynm` | `--yarnname` | YARN 应用名 |
| `-yat` | `--yarnapplicationType` | YARN 应用类型 |
| `-ynl` | `--yarnnodeLabel` | YARN node label |

推荐统一用 `-D`（通用、无歧义）。

### 8.4 attached vs detached

| | attached（默认） | detached（`-d`） |
|---|---|---|
| `execution.attached` | `true` | `false` |
| 客户端行为 | 等作业跑完才退出 | 提交完立即退出 |
| 客户端心跳 | 有（`client.heartbeat.timeout` 默认 180s，超时 JM 会取消作业） | 无 |
| `-sae` | 配合 `--shutdownOnAttachedExit`，Ctrl+C 时顺带关集群 | — |

### 8.5 配置优先级（三段叠加、后者胜）

`CliFrontend.getEffectiveConfiguration()`（`:294`）依次叠加三份配置：

| 顺序 | 来源 | 覆盖范围 |
|---|---|---|
| ① | `conf/flink-conf.yaml` | 集群级默认值 |
| ② | `-t` / `-e` / `-D` / `-m` / `-z` | 提交目标 + 任意配置项 |
| ③ | `-p` / `-d` / `-sae` / `-C` / `-s` / `-n` / `-cm` + jar → `pipeline.jars` | 并行度、模式、savepoint |

所以：

```bash
flink run -t yarn-per-job -p 4 -Dparallelism.default=2 app.jar    # 实际并行度是 4，不是 2
```

> 排查"参数不生效"最快的手段：`:310` 有 `LOG.debug("Effective configuration after Flink conf, custom commandline, and program options: {}", ...)`，把 root 日志开到 DEBUG 即可看到最终生效的完整配置。

---

## 9. 易错点与记忆锚点

1. **忘记 `-t yarn-per-job` 会静默变成 session 提交**。兜底的 `DefaultCLI` 无条件设 `execution.target=remote`。
2. **`JobGraph` ≠ `ExecutionGraph`**。前者客户端生成、可 Java 序列化、静态；后者 AM 里生成、含状态机与 attempt、随 failover 演变。
3. **算子链在客户端就完成了**，不是集群侧。所以 `JobGraph` 的 `JobVertex` 数量 ≠ 算子数量。
4. **per-job 的 JobGraph 不走 REST**。它是 Java 序列化到 `job.graph`，作为 YARN local resource 分发，AM 侧用 `FileJobGraphRetriever` 读回，再作为 `recoveredJobs` 喂给 `MiniDispatcher`——**全程没有 `JobSubmitHandler`**。
5. **per-job 首次提交的 `ExecutionType` 是 `RECOVERY`**，因此没有 session 模式那条 `jobGraphWriter.putJobGraph()` 落盘路径。
6. **集群侧参数（内存/slot/核数）在 per-job 下才由 `-D` 生效**；session 模式下静默无效。
7. **`JobVertexID` 来自算子 hash**。改算子结构或调影响 hash 的配置 → ID 变 → savepoint 恢复失败。
8. **`child-first` 类加载器**让用户 jar 里的依赖能覆盖 Flink 自带版本，也是"jar 冲突"类问题的根源。
9. **1.15 废弃、2.0 移除**。存量作业留在 1.20 可用，新作业直接上 `yarn-application`。
10. **`registerSingleLocalResource` 的最后两个布尔参数**容易看错：分别是 `whetherToAddToRemotePaths` 和 `whetherToAddToEnvShipResourceList`，不是"是否可执行"。

---

## 10. 调试实操

### 10.1 断点清单（按 yarn-per-job 主线顺序）

| # | 断点 | 观察什么 |
|---|---|---|
| 1 | `CliFrontend.validateAndGetActiveCommandLine()` | 是不是 `GenericCLI` 被选中 |
| 2 | `CliFrontend.getEffectiveConfiguration()` 返回处（`:308`） | **最终生效的完整配置**，`execution.target` 是否为 `yarn-per-job` |
| 3 | `ClientUtils.executeProgram()` | 上下文环境注入 |
| 4 | `YarnJobClusterExecutorFactory.isCompatibleWith()` | SPI 是否命中 |
| 5 | `StreamGraphGenerator.generate()` | `Transformation` → `StreamNode`/`StreamEdge` |
| 6 | `StreamingJobGraphGenerator.createChain()` | **哪几个算子被合并成同一个 `JobVertex`** |
| 7 | `AbstractJobClusterExecutor.execute()` 第一行 | 提交出去的 `JobGraph` 结构（`getVertices()`） |
| 8 | `YarnClusterDescriptor` 写 `job.graph` 那段（`:1057`） | JobGraph 序列化 + local resource 注册 |
| 9 | `FileJobGraphRetriever.retrieveJobGraph()` | AM 侧反序列化（**跨 JVM！需远程调试**） |
| 10 | `JobDispatcherFactory.createDispatcher()` | `recoveredJobs` 里是不是那个 JobGraph |
| 11 | `Dispatcher.startRecoveredJobs()` | `ExecutionType.RECOVERY` |
| 12 | `DefaultExecutionGraphBuilder.buildGraph()` | `ExecutionVertex` 的并行度展开 |
| 13 | `PipelinedRegionSchedulingStrategy.startScheduling()` | region 划分结果 |
| 14 | `YarnResourceManagerDriver.requestNewWorker()` | 向 YARN 要 TM 容器 |
| 15 | `TaskExecutor.submitTask()` / `Task` 构造 | Task 落地 |

### 10.2 本地调试替代方案（有覆盖盲区）

```bash
# 方案一：本地 session 集群（走 remote 模式，跳过 YARN 的 AM/TM 容器管理）
./bin/start-cluster.sh
./bin/flink run -m localhost:8081 ./app.jar

# 方案二：MiniCluster（client 与 JM/TM 同 JVM，全部断点可命中）
#   IDE 里用 LocalExecutor（execution.target=local）
```

⚠️ **这两个方案覆盖不到断点 9–11**：`FileJobGraphRetriever` → `MiniDispatcher` 这条链是 per-job **独有**的，session/local 模式都不会走。要验证它必须跑真正的 YARN per-job，或读 `YarnJobClusterEntrypoint` 相关的 ITCase。

### 10.3 观察 YARN 侧

```bash
# 查看应用日志（AM 容器日志）
yarn logs -applicationId application_XXXX_YY

# 查看应用状态
yarn application -status application_XXXX_YY

# 与已部署的 per-job 集群交互（注意要带 yarn.application.id）
./bin/flink list    -t yarn-per-job -Dyarn.application.id=application_XXXX_YY
./bin/flink cancel  -t yarn-per-job -Dyarn.application.id=application_XXXX_YY <jobId>
./bin/flink savepoint <jobId> hdfs:///savepoints -t yarn-per-job -Dyarn.application.id=application_XXXX_YY
```

⚠️ **取消 per-job 集群上的作业会连带销毁整个集群。**

---

## 11. 后续学习清单

- [ ] **`ExecutionGraph` 与状态机**：`ExecutionVertex` 状态迁移、failover、region 重调度
- [ ] **调度与资源**：`SlotSharingGroup`、`SlotPool`、`FineGrainedSlotManager`、YARN 容器分配策略
- [ ] **Checkpoint 机制**：`CheckpointCoordinator`、barrier 对齐、`CheckpointBarrierHandler`
- [ ] **网络栈**：`ResultPartition` / `InputGate` / `NetworkBufferPool`、反压
- [ ] **State Backend**：`KeyedStateBackend`、RocksDB、状态恢复
- [ ] **故障恢复与 HA**：`JobGraphStore`、`LeaderContender`、`JobResultStore`
- [ ] **回归对比**：per-job 已废弃，**建议用 `yarn-application` 重走一遍本文主线**，重点对比 §1.5 表格里的三处差异（main() 位置 / JobGraph 位置 / 传输方式）

---

## 附录：与其他模式对照

本文是 yarn-per-job 主线。session / application / Table-SQL 的完整通用解析保留在 `job-submission-all-modes.md`。
