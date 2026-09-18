package com.jiaqiz.flink.source;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 旧版 Source API：{@code RichParallelSourceFunction} + {@code env.addSource(...)}。
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.source.LegacySourceFunctionDemo
 * </pre>
 *
 * <h2>新老 Source API 的差别</h2>
 *
 * <table border="1">
 *   <caption>对比</caption>
 *   <tr><th></th><th>旧：SourceFunction（1.20 起 @Deprecated）</th><th>新：FLIP-27 Source</th></tr>
 *   <tr><td>代码形态</td><td>一个类 + run() 里 while 循环</td>
 *       <td>Source + SplitEnumerator + SourceReader + SourceSplit 四件套</td></tr>
 *   <tr><td>线程模型</td><td>SourceFunction 跑在 StreamTask 另起的
 *       {@code Legacy Source Thread} 上，和 mailbox 线程是两个线程</td>
 *       <td>SourceReader 跑在 StreamTask 自己的 mailbox 线程上（pollNext 被反复调用）</td></tr>
 *   <tr><td>分片/并行</td><td>没有 split 概念，并行度只是多起几个实例，
 *       各实例之间无法协同（做多分区消费要靠连接器自己硬凑）</td>
 *       <td>split 是一等公民，由 JobManager 上的 SplitEnumerator 统一分配，
 *       天然支持「分片数 ≠ 并行度」、分片重分配、按分片粒度做 checkpoint</td></tr>
 *   <tr><td>状态/恢复</td><td>恢复粒度是整个 source task；多分区消费容易重复读</td>
 *       <td>reader 用 {@code snapshotState} 上报「读到哪里」，可以精确续读</td></tr>
 *   <tr><td>有界/无界</td><td>靠「run() 是否返回」隐式表达</td>
 *       <td>显式 {@code getBoundedness()}，批流一体的前提</td></tr>
 *   <tr><td>背压/水位线</td><td>collect 走 SourceContext，基本能用但扩展性差</td>
 *       <td>SourceOutput 明确区分 record / watermark / idle</td></tr>
 * </table>
 *
 * <p><b>为什么新代码必须用 FLIP-27：</b>FLIP-27 的源码级契约（分片分配、reader 状态、
 * 有界性声明）是 Flink 做「流批一体」「自适应并行度」「精确一次分片续读」的基础设施；
 * 旧 API 在 1.20 已经整体标记 {@code @Deprecated}，官方 AGENTS/贡献指南里也明确要求
 * 新连接器只用 {@code Source}（FLIP-27）与 {@code Sink}（sink2 包）。
 * 本 demo 保留旧写法只为读源码时对照。
 *
 * <h2>运行语义</h2>
 *
 * 并行度 2，两个 subtask 各发 3 条记录（共 6 条）后 {@code run()} 正常返回 ——
 * 旧 API 里「run() 返回」就等于「这个 source 读完了」：
 * {@code SourceStreamTask.LegacySourceFunctionThread.completeProcessing()} 会
 * {@code endData(...)}，进而结束整个有界作业。
 */
public class LegacySourceFunctionDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // addSource(SourceFunction) 在 1.20 已标记 @Deprecated，会有编译期告警 —— 这正是重点。
        env.addSource(new CountingSourceFunction(3))
                .name("legacy-source-function")
                .print()
                .name("print-sink");

        env.execute("learn-flink legacy SourceFunction demo");
    }

    /** 每个并行子任务各发 {@code count} 条记录，然后 run() 返回。 */
    public static class CountingSourceFunction extends RichParallelSourceFunction<String> {

        private static final long serialVersionUID = 1L;

        private final int count;

        /**
         * volatile：cancel() 会被另一个线程（StreamTask 的 mailbox 线程）调用，
         * 而 run() 跑在 Legacy Source Thread 上，所以这个标志必须是 volatile 的。
         */
        private volatile boolean running = true;

        public CountingSourceFunction(int count) {
            this.count = count;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            System.out.println(
                    "[legacy-source] open on subtask#" + getRuntimeContext().getIndexOfThisSubtask());
        }

        /** 1.20 里 {@code open(Configuration)} 已废弃，只为满足抽象方法而保留空实现。 */
        @Override
        public void open(Configuration parameters) throws Exception {
            // no-op：真正的初始化放在 open(OpenContext) 里
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            final int subtask = getRuntimeContext().getIndexOfThisSubtask();
            for (int i = 0; i < count && running; i++) {
                // 旧 API 的规矩：collect 要和 checkpoint 对齐，必须拿 checkpointLock。
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect("legacy-record-" + subtask + "-" + i);
                }
                Thread.sleep(50L);
            }
            System.out.println("[legacy-source] subtask#" + subtask + " run() returns -> task ends");
        }

        /**
         * 作业被取消（或失败恢复）时由另一个线程调用。这里只置标志位，
         * 让 run() 里的循环自己退出；千万不要在这里 join 自己所在的线程。
         */
        @Override
        public void cancel() {
            System.out.println("[legacy-source] cancel() called, stopping loop");
            running = false;
        }
    }
}
