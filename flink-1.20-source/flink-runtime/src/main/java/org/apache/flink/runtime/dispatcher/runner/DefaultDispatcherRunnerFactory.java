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

import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.entrypoint.component.JobGraphRetriever;
import org.apache.flink.runtime.jobmanager.JobPersistenceComponentFactory;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

/**
 * {@link DispatcherRunnerFactory} implementation which creates {@link DefaultDispatcherRunner}
 * instances.
 */
public class DefaultDispatcherRunnerFactory implements DispatcherRunnerFactory {
    private final DispatcherLeaderProcessFactoryFactory dispatcherLeaderProcessFactoryFactory;

    public DefaultDispatcherRunnerFactory(
            DispatcherLeaderProcessFactoryFactory dispatcherLeaderProcessFactoryFactory) {
        this.dispatcherLeaderProcessFactoryFactory = dispatcherLeaderProcessFactoryFactory;
    }

    /**
     * ★ 创建并"启动" DispatcherRunner —— 但注意，这里是<b>异步</b>的。
     *
     * <p>本方法做两件事：
     *
     * <ol>
     *   <li>造出 {@link DispatcherLeaderProcessFactory}。它由工厂的工厂
     *       （{@code dispatcherLeaderProcessFactoryFactory}）产出，而"工厂的工厂"是
     *       {@link #createSessionRunner}/{@link #createJobRunner} 在构造本对象时就塞进来的
     *       —— 这就是 session 与 per-job 在此处的差异来源。
     *   <li>调 {@code DefaultDispatcherRunner.create(...)}，其内部会调 {@code start()}，
     *       而 {@code start()} 只做一件事：{@code leaderElection.startLeaderElection(this)}。
     * </ol>
     *
     * <p>⚠️ <b>关键</b>：{@code startLeaderElection} 只是"登记参选"并立即返回，
     * 此时 <b>Dispatcher 实例还没被创建</b>。真正的创建发生在选主成功的回调里：
     *
     * <pre>
     *   grantLeadership(leaderSessionId)                       DefaultDispatcherRunner:111
     *     → startNewDispatcherLeaderProcess(leaderSessionId)   :123
     *       → JobDispatcherLeaderProcess.start()
     *         → onStart()                                      JobDispatcherLeaderProcess
     *           → dispatcherGatewayServiceFactory.create(..., jobGraph, ...)  ★ JobGraph 在此传入
     *             → JobDispatcherFactory.createDispatcher(recoveredJobs=[jobGraph], ...)
     *               → new MiniDispatcher(...)
     *             → dispatcher.start()
     *               → Dispatcher.onStart() → startRecoveredJobs()   ★★ 作业真正开始提交
     * </pre>
     *
     * <p>所以"作业什么时候开始跑"这个问题的答案不是"这一行"，而是<b>选主成功之后</b>。
     */
    @Override
    public DispatcherRunner createDispatcherRunner(
            LeaderElection leaderElection,
            FatalErrorHandler fatalErrorHandler,
            JobPersistenceComponentFactory jobPersistenceComponentFactory,
            Executor ioExecutor,
            RpcService rpcService,
            PartialDispatcherServices partialDispatcherServices)
            throws Exception {

        // 产出"领导进程工厂"。注意 createJobRunner / createSessionRunner 的区别就固化在
        // 这个 factory 里：per-job 的工厂持有 jobGraphRetriever，session 的持有 DispatcherFactory。
        final DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory =
                dispatcherLeaderProcessFactoryFactory.createFactory(
                        jobPersistenceComponentFactory,
                        ioExecutor,
                        rpcService,
                        partialDispatcherServices,
                        fatalErrorHandler);

        // create() 内部会 start() → leaderElection.startLeaderElection(this)，异步参选后立即返回
        return DefaultDispatcherRunner.create(
                leaderElection, fatalErrorHandler, dispatcherLeaderProcessFactory);
    }

    /** session 模式：领导进程最终创建 StandaloneDispatcher（可接受随时提交的新作业）。 */
    public static DefaultDispatcherRunnerFactory createSessionRunner(
            DispatcherFactory dispatcherFactory) {
        return new DefaultDispatcherRunnerFactory(
                SessionDispatcherLeaderProcessFactoryFactory.create(dispatcherFactory));
    }

    /**
     * per-job 模式：领导进程最终创建 {@code MiniDispatcher}，并把 {@code jobGraphRetriever}
     * 一起带下去——AM 启动时读出的那个 JobGraph 就是这样一路传到 Dispatcher 的。
     */
    public static DefaultDispatcherRunnerFactory createJobRunner(
            JobGraphRetriever jobGraphRetriever) {
        return new DefaultDispatcherRunnerFactory(
                JobDispatcherLeaderProcessFactoryFactory.create(jobGraphRetriever));
    }
}
