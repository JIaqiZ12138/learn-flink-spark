package com.jiaqiz.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * 新版 <b>Sink V2（FLIP-143 / {@code org.apache.flink.api.connector.sink2}）</b>演示：
 * 手写一个 {@link Sink} + {@link SinkWriter}，再用 {@code .sinkTo(...)} 接入；
 * 同时对比内置 {@link FileSink} 和 {@code .print()}。
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.sink.CustomSinkDemo
 * </pre>
 *
 * <h2>Sink V2 的三个生命周期方法（重要）</h2>
 *
 * <ul>
 *   <li>{@code write(element, context)} —— 每条记录一次。这里应当<b>攒批</b>，不要一条一次
 *       RPC。context 里能拿到 watermark/记录时间戳。
 *   <li>{@code flush(endOfInput)} —— 「把攒的批发出去」的时机。两个触发点（见
 *       {@code SinkWriterOperator}）：
 *       <ul>
 *         <li>{@code prepareSnapshotPreBarrier(checkpointId)} → {@code flush(false)}：
 *             checkpoint 对齐前刷一次，保证 at-least-once；
 *         <li>{@code endInput()}（有界作业的输入结束）→ {@code flush(true)}：
 *             这时 {@code endOfInput == true}，之后不会再有任何记录。
 *       </ul>
 *   <li>{@code close()} —— 由 {@code SinkWriterOperator.close()} 通过
 *       {@code IOUtils.closeAll(sinkWriter, super::close)} 调用，用于释放连接/文件句柄。
 *       <b>注意</b>：close() 不是「刷数据」的可靠时机（作业被 kill 时可能来不及走），
 *       该发的东西必须在 flush() 里发。
 * </ul>
 *
 * <p>另外注意 {@link Sink#createWriter(Sink.InitContext)} 在 1.20 已被 {@code @Deprecated}：
 * 因为要保持 Sink 是函数式接口，Flink 没有给它写默认实现，所以新实现仍然「必须」实现它，
 * 但运行时真正被调用的是 {@link Sink#createWriter(WriterInitContext)}。本 demo 两个都实现，
 * 把真正的逻辑放在 {@code WriterInitContext} 那个重载里。
 *
 * <h2>本 demo 的三条支路</h2>
 *
 * <ol>
 *   <li>自定义 Sink V2：打印 write/flush/close 的调用时机与顺序（并行度 2，两个 writer）；
 *   <li>内置 {@link FileSink}：写到 {@code /tmp/flink-demo-sink/}，跑完打印文件名与行数；
 *   <li>{@code .print()}：Flink 自带的调试用标准输出 sink。
 * </ol>
 *
 * <p>每次运行前会先递归删掉 {@code /tmp/flink-demo-sink/}，保证可重复运行。
 */
public class CustomSinkDemo {

    private static final String FILE_SINK_DIR = "/tmp/flink-demo-sink/";

    public static void main(String[] args) throws Exception {

        // 0. 清理上次运行留下的文件，保证结果可重复
        deleteRecursively(Paths.get(FILE_SINK_DIR));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        final DataStream<String> stream =
                env.fromData("alpha", "beta", "gamma", "delta", "epsilon", "zeta");

        // 1) 手写 Sink V2：sinkTo 直接吃 org.apache.flink.api.connector.sink2.Sink
        stream.sinkTo(new PrintingStringSink()).name("custom-sink-v2");

        // 2) 内置 FileSink：forRowFormat(path, Encoder) → with* → build()
        //    SimpleStringEncoder 会把每条记录写成「内容 + '\n'」，所以行数 == 记录数。
        final FileSink<String> fileSink =
                FileSink.forRowFormat(
                                new Path(FILE_SINK_DIR), new SimpleStringEncoder<String>("UTF-8"))
                        // 让文件直接落在 base 目录，而不是默认的 yyyy-MM-dd--HH 分桶子目录
                        .withBucketAssigner(new BasePathBucketAssigner<String>())
                        // 滚动策略：超过 128MB / 5 分钟 / 1 分钟不活跃就换新文件
                        .withRollingPolicy(
                                DefaultRollingPolicy.builder()
                                        .withMaxPartSize(128L * 1024 * 1024)
                                        .withRolloverInterval(Duration.ofMinutes(5))
                                        .withInactivityInterval(Duration.ofMinutes(1))
                                        .<String, String>build())
                        .withOutputFileConfig(
                                new OutputFileConfig("demo-part", ".txt"))
                        .build();

        // FileSink 的并行度压到 1，跑完只有一个 part 文件，便于核对行数
        stream.sinkTo(fileSink).setParallelism(1).name("file-sink");

        // 3) 内置 .print()（内部也是 Sink V2：PrintSinkFunction 被适配成 SinkWriter）
        stream.print().name("print-sink");

        env.execute("learn-flink Sink V2 demo");

        // 4) 作业结束后读取 FileSink 产物
        System.out.println("========== FileSink 结果 ==========");
        final List<java.nio.file.Path> files = new ArrayList<>();
        try (Stream<java.nio.file.Path> walk = Files.walk(Paths.get(FILE_SINK_DIR))) {
            walk.filter(Files::isRegularFile).forEach(files::add);
        }
        files.sort(Comparator.comparing(java.nio.file.Path::toString));

        int totalLines = 0;
        for (java.nio.file.Path file : files) {
            final List<String> lines = Files.readAllLines(file);
            totalLines += lines.size();
            System.out.println("文件: " + file);
            System.out.println("  行数: " + lines.size() + ", 内容: " + lines);
        }
        System.out.println("文件数: " + files.size() + ", 总行数: " + totalLines);
    }

    // ==================================================================================
    //  自定义 Sink V2
    // ==================================================================================

    /**
     * 一个「打印型」Sink V2。
     *
     * <p>Sink 本体在客户端被构建、被序列化后分发到各 subtask；真正的 {@link SinkWriter}
     * 只能在 TaskManager 上被 {@code createWriter} 创建出来，所以可变的运行时状态
     * （连接、缓冲）全部放在 writer 里，Sink 只保留不可变配置。
     */
    public static class PrintingStringSink implements Sink<String> {

        private static final long serialVersionUID = 1L;

        /** 运行时真正被调用的入口。 */
        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
            return new PrintingStringWriter(context.getSubtaskId());
        }

        /**
         * 1.20 里仍是抽象方法（Flink 为保持 Sink 的函数式接口特性没给默认实现）。
         * 保留实现只是为了能编译通过；运行时不会被调用。
         */
        @Override
        public SinkWriter<String> createWriter(InitContext context) throws IOException {
            return new PrintingStringWriter(context.getSubtaskId());
        }
    }

    /** 把 write/flush/close 的调用时机、顺序和状态全部打出来。 */
    private static final class PrintingStringWriter implements SinkWriter<String> {

        private final int subtaskId;

        /** 模拟「攒批」：真实 sink 在这里攒 JDBC batch / Kafka producer record。 */
        private final List<String> buffer = new ArrayList<>();

        private int totalWritten;

        PrintingStringWriter(int subtaskId) {
            this.subtaskId = subtaskId;
            System.out.println("[custom-sink subtask#" + subtaskId + "] writer created");
        }

        @Override
        public void write(String element, Context context) throws IOException {
            buffer.add(element);
            totalWritten++;
            System.out.println(
                    "[custom-sink subtask#"
                            + subtaskId
                            + "] write('"
                            + element
                            + "') buffer="
                            + buffer.size()
                            + " currentWatermark="
                            + context.currentWatermark()
                            + " timestamp="
                            + context.timestamp());
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // 真实实现里这里才是「把 batch 提交出去」的地方。
            System.out.println(
                    "[custom-sink subtask#"
                            + subtaskId
                            + "] flush(endOfInput="
                            + endOfInput
                            + ") 发送 "
                            + buffer.size()
                            + " 条 -> "
                            + buffer);
            buffer.clear();
        }

        @Override
        public void close() throws IOException {
            System.out.println(
                    "[custom-sink subtask#"
                            + subtaskId
                            + "] close() 累计写入 "
                            + totalWritten
                            + " 条，剩余未发送 "
                            + buffer.size()
                            + " 条");
        }
    }

    // ==================================================================================
    //  工具
    // ==================================================================================

    /** 递归删除目录（不存在就忽略），保证 demo 可重复运行。 */
    private static void deleteRecursively(java.nio.file.Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<java.nio.file.Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    Files.deleteIfExists(p);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
        }
    }
}
