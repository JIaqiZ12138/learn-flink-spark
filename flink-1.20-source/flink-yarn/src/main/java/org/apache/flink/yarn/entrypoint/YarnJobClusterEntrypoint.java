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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.DynamicParametersConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.IOException;
import java.util.Map;

/**
 * Entry point for Yarn per-job clusters.
 *
 * @deprecated Per-mode has been deprecated in Flink 1.15 and will be removed in the future. Please
 *     use application mode instead.
 */
@Deprecated
public class YarnJobClusterEntrypoint extends JobClusterEntrypoint {

    public YarnJobClusterEntrypoint(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected String getRPCPortRange(Configuration configuration) {
        return configuration.get(YarnConfigOptions.APPLICATION_MASTER_PORT);
    }


    // 创建 Dispatcher 转发器 和 flink内部的 resourceManager
    @Override
    protected DefaultDispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(Configuration configuration)
                    throws IOException {
        return DefaultDispatcherResourceManagerComponentFactory.createJobComponentFactory(
                YarnResourceManagerFactory.getInstance(),

                // 创建一个 JobGraphRetriever
                FileJobGraphRetriever.createFrom(
                        configuration,
                        YarnEntrypointUtils.getUsrLibDir(configuration).orElse(null)));
    }

    // ------------------------------------------------------------------------
    //  The executable entry point for the Yarn Application Master Process
    //  for a single Flink job.
    // ------------------------------------------------------------------------

    // YarnJobClusterEntrypoint 内部的执行逻辑起点
    /**
     * ★ AM 容器的入口方法。
     *
     * <p>这个方法本身很短，做的事只有三件：<b>准备配置 → 造出 entrypoint 对象 → 交给基类启动</b>。
     * 真正的重活全在 {@link ClusterEntrypoint#runClusterEntrypoint} 里面。
     *
     * <p>注意本方法运行在 <b>YARN 的 AM 容器</b>里（不是客户端）。容器启动命令由客户端在
     * {@code YarnClusterDescriptor.setupApplicationMasterContainer()} 里拼好，形如：
     * {@code java ... YarnJobClusterEntrypoint -Dxxx=yyy}。
     */
    public static void main(String[] args) {

        LOG.warn(
                "Job Clusters are deprecated since Flink 1.15. Please use an Application Cluster/Application Mode instead.");

        // ==================== 第 1 步：进程级初始化（与 Flink 无关的通用准备）====================
        // startup checks and logging
        // 打印 JVM/OS/版本等环境信息，出问题时第一眼就能看到运行环境
        EnvironmentInformation.logEnvironmentInfo(
                LOG, YarnJobClusterEntrypoint.class.getSimpleName(), args);
        // 注册信号处理器，让 SIGTERM/SIGINT 能被优雅处理而不是直接杀进程
        SignalHandler.register(LOG);
        // JVM 被强制终止时给一点时间做清理，避免状态写坏
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        // ==================== 第 2 步：从 YARN 环境变量拿容器工作目录 ====================
        Map<String, String> env = System.getenv();

        // PWD 是 YARN 注入的环境变量，指向容器的工作目录——所有 local resource
        // （flink dist、job.graph、flink-conf.yaml）都被解压到这里。
        // 拿不到它说明不是在 YARN 容器里跑，直接失败。
        final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
        Preconditions.checkArgument(
                workingDirectory != null,
                "Working directory variable (%s) not set",
                ApplicationConstants.Environment.PWD.key());

        // 把 YARN 相关的环境信息打进日志，方便排查容器侧问题
        try {
            YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
        } catch (IOException e) {
            LOG.warn("Could not log YARN environment information.", e);
        }

        // ==================== 第 3 步：解析配置 ====================
        // 配置来自两个地方，这里做合并：
        //   ① 命令行 -D 参数（由客户端在 AM 启动命令里生成，即 JobManagerProcessUtils
        //      产出的 dynamicParameterListStr）
        //   ② flink-conf.yaml（客户端上传到容器工作目录的那个文件）
        final Configuration dynamicParameters =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new DynamicParametersConfigurationParserFactory(),
                        YarnJobClusterEntrypoint.class);
        final Configuration configuration =
                YarnEntrypointUtils.loadConfiguration(workingDirectory, dynamicParameters, env);

        // ==================== 第 4 步：交给基类启动 ====================
        YarnJobClusterEntrypoint yarnJobClusterEntrypoint =
                new YarnJobClusterEntrypoint(configuration);

        // ★★★ 本方法到此为止，剩下的全在基类里。
        // runClusterEntrypoint 内部会依次完成：
        //   startCluster()  → 安全上下文 + 文件系统 + runCluster()
        //     runCluster()  → initializeServices()（RPC/HA/BlobServer/指标/GraphStore）
        //                   → createDispatcherResourceManagerComponentFactory()  ← 子类多态点
        //                   → factory.create() 启动 WebMonitor + ResourceManager + Dispatcher
        //   getTerminationFuture().get()  → 阻塞等待集群终止，然后 System.exit(returnCode)
        //
        // 也就是说：这一行之后的代码永远不会执行——本方法以 JVM 退出收场。
        ClusterEntrypoint.runClusterEntrypoint(yarnJobClusterEntrypoint);
    }
}
