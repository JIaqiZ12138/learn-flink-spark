package com.learn.flink.sample;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 最小可运行的 Flink DataStream 作业：流式 WordCount。
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn -pl learn-flink exec:java
 * </pre>
 *
 * <p>本类刻意保持最小，方便在 IDE 里打断点跟提交链路（详见 {@code doc/job-submit/}）：
 *
 * <ul>
 *   <li>{@code StreamExecutionEnvironment.getExecutionEnvironment()} —— 拿到执行环境。CLI 提交时返回的是
 *       {@code StreamContextEnvironment}，IDE 直接 run 时是本地环境；
 *   <li>{@code env.fromData(...)} —— 每个算子都会被登记进
 *       {@code StreamExecutionEnvironment.transformations}；
 *   <li>{@code env.execute()} —— 触发 {@code StreamGraph → JobGraph} 的翻译与提交。
 * </ul>
 */
public class WordCountJob {

    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境。本地运行时会自动起一个 MiniCluster（client 与 JM/TM 同 JVM），
        //    这也是调试内核时最方便的方式。
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Source：从内存数据建流（fromElements 已废弃，用 fromData）
        final DataStream<String> lines =
                env.fromData(
                        "flink spark flink",
                        "spark flink flink",
                        "hello big data",
                        "flink and spark");

        // 3. Transformation：切词 → 转 (word, 1) → 按 word 分组 → 累加
        final DataStream<Tuple2<String, Integer>> counts =
                lines.flatMap(
                                // 泛型被擦除，所以下面必须显式 returns(...)
                                (String line, Collector<Tuple2<String, Integer>> out) -> {
                                    for (String word : line.split("\\s+")) {
                                        if (!word.isEmpty()) {
                                            out.collect(Tuple2.of(word, 1));
                                        }
                                    }
                                })
                        .returns(Types.TUPLE(Types.STRING, Types.INT))
                        // keyBy 会产生 PartitionTransformation（哈希重分区）。
                        // 这条边不是 ForwardPartitioner，所以在算子链判定时会被断开，
                        // 上下游会落到不同的 JobVertex。
                        .keyBy(tuple -> tuple.f0)
                        .sum(1)
                        .name("word-count");

        // 4. Sink：打印到标准输出
        counts.print().name("print-sink");

        // 5. 提交执行。这一步内部会依次完成：
        //      StreamGraphGenerator.generate()          → StreamGraph
        //      StreamingJobGraphGenerator.createJobGraph() → JobGraph（算子链在此生成）
        //      PipelineExecutor.execute(...)            → 提交到本地 MiniCluster
        env.execute("learn-flink WordCount");
    }
}
