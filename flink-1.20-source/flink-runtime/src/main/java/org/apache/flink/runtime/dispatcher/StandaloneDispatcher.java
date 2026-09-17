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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.Collection;

// session（会话）模式的 Dispatcher 实现，是 StandaloneSessionClusterEntrypoint 等会话集群的默认选择。
// ★ 与 MiniDispatcher 的关键差别：它不会因"某一个作业结束"而关集群 —— 作业跑完只回收对应的
//   JobManagerRunner，Dispatcher 与集群继续接收新作业。所以本类既没有 executionMode 字段，也没有
//   shutDownFuture 收尾逻辑，几乎是 Dispatcher 的"空壳"子类，全部行为差异都落在父类里。
/**
 * Dispatcher implementation which spawns a {@link JobMaster} for each submitted {@link JobGraph}
 * within in the same process. This dispatcher can be used as the default for all different session
 * clusters.
 */
public class StandaloneDispatcher extends Dispatcher {
    // 参数原样转交父类；recoveredJobs 来自 HA 存储（session 集群重启时恢复），而非 YARN 上的 job.graph。
    public StandaloneDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobResults,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices)
            throws Exception {
        super(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobResults,
                dispatcherBootstrapFactory,
                dispatcherServices);
    }
}
