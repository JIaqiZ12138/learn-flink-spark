package com.jiaqiz.flink.operators;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 打印 DataStream API 的「对象结构」：从 Source 到 Sink，每一步链式调用返回的都是另一个包装对象，
 * 用 {@code getClass().getSimpleName()} 把它们逐个打印出来，顺便演示 name/uid/setParallelism/
 * slotSharingGroup/disableChaining 这些「结构类」API。
 *
 * <p>运行命令：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.operators.DataStreamStructureDemo
 * </pre>
 *
 * <p>核心认知：<b>DataStream 家族的对象只是"算子图"的构建器（builder），不是数据本身。</b>
 * 每调用一次转换方法，就往 {@code StreamExecutionEnvironment.transformations} 里追加一个
 * Transformation，并返回一个新的包装对象给你继续链式调用；直到 {@code execute()} 才把这张图
 * 翻译成 StreamGraph → JobGraph 并提交。所以这里可以在 execute 之前安全地打印类型。
 *
 * <p>包装链：
 *
 * <pre>
 *   DataStreamSource          SourceTransformation        「源」
 *     → SingleOutputStreamOperator  OneInputTransformation   「单输出算子」map/filter/flatMap
 *     → KeyedStream                 PartitionTransformation  「已分组」keyBy
 *     → WindowedStream              还是上面那个 PartitionTransformation，「开窗」只是逻辑包装
 *     → DataStreamSink              SinkTransformation       「汇」print/addSink
 * </pre>
 */
public class DataStreamStructureDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // ---------- (1) Source ----------
        final DataStreamSource<Integer> source = env.fromData(1, 2, 3, 4, 5, 6);
        System.out.println("[1] env.fromData(...)                -> " + clazz(source));

        // ---------- (2) map：SingleOutputStreamOperator ----------
        // 这里一串「结构类」API 依次链式调用，它们都不会产生新的数据转换，只是往
        // SingleOutputStreamOperator 这个包装对象/底层 Transformation 上打标记：
        //   name(...)             影响 Web UI 与日志里算子显示的名字，便于排障（纯展示）
        //   uid(...)              算子的稳定唯一 id：从 savepoint 恢复时按它对齐，一旦改动就恢复失败
        //   setParallelism(...)   只覆盖「这一个算子」的并行度，不改变 env 的全局并行度
        //   slotSharingGroup(...) slot 共享组：只有同组的算子才能被调度到同一个 Slot；
        //                         给它一个新名字就等价于"强制这个算子独占 slot"，用于隔离资源
        //   disableChaining()     禁止与上下游合并成算子链：会多出一个 JobVertex，
        //                         ⽤于排查某算子耗时/线程问题，代价是多了网络/线程切换开销
        final SingleOutputStreamOperator<Tuple2<String, Integer>> mapped =
                source.map((Integer n) -> Tuple2.of(n % 2 == 0 ? "even" : "odd", n))
                        .returns(Types.TUPLE(Types.STRING, Types.INT))
                        .name("classify-parity")
                        .uid("classify-parity-uid")
                        .setParallelism(2)
                        .slotSharingGroup("default")
                        .disableChaining();
        System.out.println("[2] .map(...).returns(...)           -> " + clazz(mapped));

        // ---------- (3) keyBy：KeyedStream ----------
        // keyBy 是"分界线"：返回值从 DataStream 变成 KeyedStream，类型上就把「有 key 的流」
        // 与「无 key 的流」区分开，从而编译期禁止你在没有 key 的流上调用 keyed state。
        final KeyedStream<Tuple2<String, Integer>, String> keyed = mapped.keyBy(tuple -> tuple.f0);
        System.out.println("[3] .keyBy(...)                      -> " + clazz(keyed));

        // ---------- (4) window：WindowedStream ----------
        // 用 countWindow 而不是时间窗口：countWindow 按"条数"触发，有界输入一定能触发完，
        // 时间窗口在本地跑小数据时可能因为 watermark 不前进而一条都不输出。
        final WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> windowed = keyed.countWindow(2);
        System.out.println("[4] .countWindow(2)                  -> " + clazz(windowed));

        // WindowedStream 是个"半成品"：它没有 print()，必须再套一层聚合才回到 DataStream。
        final SingleOutputStreamOperator<Tuple2<String, Integer>> windowSum = windowed.sum(1);
        System.out.println("[4b] .sum(1)（回到 DataStream）      -> " + clazz(windowSum));

        // ---------- (5) print：DataStreamSink ----------
        final DataStreamSink<Tuple2<String, Integer>> sink = windowSum.print("structure");
        System.out.println("[5] .print(\"structure\")              -> " + clazz(sink));

        System.out.println();
        System.out.println("=== 每个算子的 uid / name / 并行度 ===");
        // getTransformation() 能看到底层对象：每个算子都有自己的 id / name / parallelism
        System.out.println(
                "source      : "
                        + source.getTransformation().getName()
                        + ", parallelism="
                        + source.getTransformation().getParallelism());
        System.out.println(
                "classify    : "
                        + mapped.getTransformation().getName()
                        // getUid() = 用户显式指定的稳定 id（savepoint 恢复靠它对齐算子）
                        + ", uid="
                        + mapped.getTransformation().getUid()
                        + ", parallelism="
                        + mapped.getTransformation().getParallelism()
                        + ", parallelismConfigured="
                        + mapped.getTransformation().isParallelismConfigured()
                        // getMaxParallelism() 返回 -1 表示 AUTO（提交时再算），非 -1 就是 key group 数
                        + ", maxParallelism="
                        + mapped.getTransformation().getMaxParallelism()
                        + ", slotSharingGroup="
                        + mapped.getTransformation().getSlotSharingGroup().map(g -> g.getName())
                                .orElse("<null>"));
        System.out.println(
                "window-sum  : "
                        + windowSum.getTransformation().getName()
                        + ", parallelism="
                        + windowSum.getTransformation().getParallelism());
        System.out.println("sink        : " + sink.getTransformation().getName() + ", parallelism="
                + sink.getTransformation().getParallelism());

        System.out.println();
        System.out.println("=== 打印完结构后真正提交作业（有界，会自己退出） ===");
        env.execute("DataStreamStructureDemo");
    }

    /** 打一行「当前对象的运行时类名」，把包装链可视化。 */
    private static String clazz(Object o) {
        return o.getClass().getSimpleName();
    }
}
