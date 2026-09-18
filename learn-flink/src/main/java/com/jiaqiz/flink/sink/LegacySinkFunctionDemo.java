package com.jiaqiz.flink.sink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 旧版 Sink API：{@code RichSinkFunction<String>} + {@code .addSink(...)}。
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.sink.LegacySinkFunctionDemo
 * </pre>
 *
 * <h2>{@code SinkFunction} vs {@code Sink}（sink2）</h2>
 *
 * <table border="1">
 *   <caption>对比</caption>
 *   <tr><th></th><th>旧：{@code SinkFunction} / {@code RichSinkFunction}</th>
 *       <th>新：{@code org.apache.flink.api.connector.sink2.Sink}</th></tr>
 *   <tr><td>接入方式</td><td>{@code stream.addSink(fn)}（1.20 起 @Deprecated）</td>
 *       <td>{@code stream.sinkTo(sink)}</td></tr>
 *   <tr><td>调用粒度</td><td>框架逐条调 {@code invoke(value, context)}，攒批要自己搞
 *       （或在 invoke 里做，但 flush 时机不可控）</td>
 *       <td>显式区分 {@code write}（逐条）/ {@code flush(endOfInput)}（刷批时机）/
 *       {@code close}（释放资源）</td></tr>
 *   <tr><td>状态与容错</td><td>只有 {@code CheckpointedFunction} 能存 state，
 *       且没有 committable 的概念，难以做两阶段提交</td>
 *       <td>{@code StatefulSink} + {@code Committer} 组成完整的
 *       write → pre-commit → commit 三段式，是 exactly-once sink 的标准形态</td></tr>
 *   <tr><td>并行度/算子链</td><td>每条记录一次 JNI 风格的调用，无法参与
 *       {@code SupportsPreCommitTopology} 之类的高级优化</td>
 *       <td>可参与算子链优化（例如 FileSink 的 compaction 拓扑改写）</td></tr>
 * </table>
 *
 * <p>结论：新代码请用 {@code sinkTo(new YourSink())}。本 demo 保留旧写法，
 * 一是为了在源码里对照 {@code StreamSink}（旧）与 {@code SinkWriterOperator}（新）
 * 两条执行路径，二是很多历史工程里还有大量 {@code RichSinkFunction}。
 *
 * <p>注意 {@code RichSinkFunction} 的 {@code open(Configuration)} 在 1.20 已废弃，
 * 推荐改写成 {@code open(OpenContext)}；这里两个都保留，真正的初始化写在
 * {@code open(OpenContext)} 里。
 */
public class LegacySinkFunctionDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        final DataStream<String> stream =
                env.fromData("one", "two", "three", "four", "five", "six");

        // addSink(SinkFunction) 在 1.20 已标记 @Deprecated —— 会有编译期告警。
        stream.addSink(new CollectingSinkFunction()).name("legacy-rich-sink-function");

        env.execute("learn-flink legacy SinkFunction demo");
    }

    /**
     * 每个并行子任务持有一个「连接」（这里用一个 List 模拟），
     * 在 open 里建立、在 invoke 里使用、在 close 里释放。
     */
    public static class CollectingSinkFunction extends RichSinkFunction<String> {

        private static final long serialVersionUID = 1L;

        /** transient：真正的运行时资源不参与序列化，只在 TaskManager 上创建。 */
        private transient List<String> buffer;

        private transient int subtaskId;

        private int totalInvoked;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            this.subtaskId = getRuntimeContext().getIndexOfThisSubtask();
            this.buffer = new ArrayList<>();
            System.out.println("[legacy-sink subtask#" + subtaskId + "] open(): 建立连接/资源");
        }

        /** 1.20 里 {@code open(Configuration)} 已废弃，留空实现只为满足抽象方法。 */
        @Override
        public void open(Configuration parameters) throws Exception {
            // no-op：真正的初始化在 open(OpenContext) 里
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            totalInvoked++;
            buffer.add(value);
            System.out.println(
                    "[legacy-sink subtask#"
                            + subtaskId
                            + "] invoke('"
                            + value
                            + "') 本条时间戳="
                            + context.timestamp()
                            + " buffer="
                            + buffer.size());
            // 旧 API 没有 flush 钩子，攒到一定量只能自己判断着发
            if (buffer.size() >= 3) {
                System.out.println(
                        "[legacy-sink subtask#" + subtaskId + "] 手动刷批 -> " + buffer);
                buffer.clear();
            }
        }

        @Override
        public void close() throws Exception {
            System.out.println(
                    "[legacy-sink subtask#"
                            + subtaskId
                            + "] close(): 累计 invoke "
                            + totalInvoked
                            + " 条，残留未刷 "
                            + (buffer == null ? 0 : buffer.size())
                            + " 条 -> "
                            + buffer);
            buffer = null;
            super.close();
        }
    }
}
