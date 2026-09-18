package com.jiaqiz.flink.rich;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 演示「富函数」（Rich Functions）：相比普通 MapFunction/FlatMapFunction，富函数多了
 * {@code open()} / {@code close()} 生命周期与 {@code getRuntimeContext()}，从而能访问
 * subtask 下标、keyed state、累加器与 metrics。
 *
 * <p>运行命令：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.rich.RichFunctionDemo
 * </pre>
 *
 * <p>演示三件事：
 *
 * <ol>
 *   <li>{@link RichMapFunction}：在 {@code open(Configuration)} 里初始化资源、{@code close()} 里释放，
 *       并打印 {@code getIndexOfThisSubtask()} / {@code getNumberOfParallelSubtasks()}；
 *   <li>{@link RichFlatMapFunction}：在 {@code keyBy} 之后用 {@code getRuntimeContext().getState(...)}
 *       做 keyed state 计数（ValueState）；
 *   <li>累加器 {@code addAccumulator("myCounter", new IntCounter())} 与
 *       {@code getMetricGroup().counter("records")}，作业结束后用
 *       {@code JobExecutionResult.getAccumulatorResult(...)} 取回汇总值。
 * </ol>
 */
public class RichFunctionDemo {

    /**
     * 富 MapFunction：模拟「每个 subtask 打开一次昂贵资源（连接池 / 模型 / 文件句柄）」。
     *
     * <p>关键点：{@code open()} / {@code close()}
     * <b>每个 subtask 各调用一次</b>，而不是每条记录一次 —— 所以重初始化必须写在这里，
     * 写在 {@code map()} 里会导致每条记录都重建一次资源。
     *
     * <p>小提示：Flink 1.20 里 {@code RuntimeContext.getIndexOfThisSubtask()} 等方法已被标记
     * {@code @Deprecated}（新写法是 {@code getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()}），
     * 但旧方法仍可正常使用，这里按最常见的学习写法保留，只会产生 deprecation 编译警告。
     */
    public static class ResourcefulRichMapFunction extends RichMapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        /** 用 transient 修饰：资源本身不可序列化，它在 open() 里"活过来"即可。 */
        private transient List<String> openedResource;

        /** 累加器：跨 subtask 汇总，作业结束后由客户端一次性取回（适合做数据质量统计）。 */
        private transient Accumulator<Integer, Integer> myCounter;

        /** Metric：上报到 Flink MetricSystem，可在 Web UI / 监控里实时看（适合做运行时观测）。 */
        private transient Counter recordsCounter;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 1) 初始化"资源"
            openedResource = new ArrayList<>();
            openedResource.add("resource-of-subtask-" + getRuntimeContext().getIndexOfThisSubtask());

            // 2) 注册累加器。名字是全局的，多 subtask 的局部值最终会被合并（IntCounter 求和）。
            getRuntimeContext().addAccumulator("myCounter", new IntCounter());
            myCounter = getRuntimeContext().getAccumulator("myCounter");

            // 3) 注册 metric counter
            recordsCounter = getRuntimeContext().getMetricGroup().counter("records");

            System.out.printf(
                    "[open ] subtask %d/%d task=%s 初始化资源 %s%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getNumberOfParallelSubtasks(),
                    getRuntimeContext().getTaskNameWithSubtasks(),
                    openedResource);
        }

        @Override
        public String map(String value) throws Exception {
            myCounter.add(1); // 累加器：每处理一条 +1
            recordsCounter.inc(); // metric：每处理一条 +1
            return value.toUpperCase(Locale.ROOT);
        }

        @Override
        public void close() throws Exception {
            // close() 也是每个 subtask 一次：用来关连接、刷缓冲、释放句柄
            System.out.printf(
                    "[close] subtask %d/%d 释放资源 %s（本 subtask 处理了 %d 条）%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getNumberOfParallelSubtasks(),
                    openedResource,
                    recordsCounter == null ? 0 : recordsCounter.getCount());
            openedResource = null;
        }
    }

    /** 普通具名 flatMap：切词成 (word, 1)。用类声明写死泛型，避免 lambda 的类型擦除问题。 */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String word : value.split("\\s+")) {
                if (!word.isEmpty()) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }
    }

    /**
     * keyed state 计数：必须作用在 {@code KeyedStream} 上。
     *
     * <p>为什么 keyed state 比「自己用 HashMap 存」好：状态由 Flink 托管，
     * 能跟着 key group 一起做 checkpoint / rescale，扩缩容时自动重分布；
     * 手写 HashMap 在并行度变化时会丢状态。
     */
    public static class KeyedCountRichFlatMap
            extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<Integer> countState;
        private transient Accumulator<Integer, Integer> keyedCounter;

        @Override
        public void open(Configuration parameters) throws Exception {
            // ValueStateDescriptor 必须在 open() 里构造：getRuntimeContext() 只有 Task 真正运行时才可用，
            // 放在字段初始化（构造期）会拿到 null。
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("word-count", Types.INT);
            countState = getRuntimeContext().getState(descriptor);
            getRuntimeContext().addAccumulator("keyedFlatMapCounter", new IntCounter());
            keyedCounter = getRuntimeContext().getAccumulator("keyedFlatMapCounter");
        }

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            keyedCounter.add(1);            // state 是「当前 key 私有的」：同一个 subtask 上不同 key 的状态互不可见，
            // 这正是 keyed state 与 operator state 的根本区别。
            Integer current = countState.value();
            int next = (current == null ? 0 : current) + value.f1;
            countState.update(next);
            // 每来一条就发一次，方便观察"同一个 key 的计数在递增"
            out.collect(Tuple2.of(value.f0, next));
        }
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度 2：能同时看到两个 subtask 各自 open/close 一次，以及累加器的合并效果
        env.setParallelism(2);

        final DataStream<String> lines =
                env.fromData(
                        "flink spark flink",
                        "spark flink flink",
                        "flink and spark",
                        "big data flink");

        // ---------- 1) RichMapFunction：open/close + subtask 信息 + 累加器/指标 ----------
        final DataStream<String> upperCased = lines.map(new ResourcefulRichMapFunction());
        upperCased.print("rich-map");

        // ---------- 2) keyBy 之后用 keyed state 计数 ----------
        final DataStream<Tuple2<String, Integer>> words =
                lines.flatMap(new Tokenizer()).returns(Types.TUPLE(Types.STRING, Types.INT));

        final DataStream<Tuple2<String, Integer>> runningCounts =
                words
                        // 注意 keyBy(t -> t.f0)：key 类型从 Tuple2<String,Integer> 的 f0 推断为 String，
                        // 所以这里不需要额外传 TypeInformation。
                        .keyBy(tuple -> tuple.f0)
                        .flatMap(new KeyedCountRichFlatMap());
        runningCounts.print("keyed-state");

        System.out.println("=== 提交作业 ===");
        final JobExecutionResult result = env.execute("RichFunctionDemo");

        // ---------- 3) 作业结束后取回累加器 ----------
        // 注意：只有通过 RuntimeContext.addAccumulator(...) 注册过的累加器才会被上报回来。
        System.out.println("=== 作业结束，累加器汇总 ===");
        // getAccumulatorResult 的返回类型是泛型 T，用 Object 接住最直观
        final Object myCounterResult = result.getAccumulatorResult("myCounter");
        final Object keyedCounterResult = result.getAccumulatorResult("keyedFlatMapCounter");
        System.out.println("myCounter(RichMapFunction 处理总条数)        = " + myCounterResult
                + "（期望 4 条输入 × 1 = 4）");
        System.out.println("keyedFlatMapCounter(切出的总词数)            = " + keyedCounterResult);
        System.out.println("全部累加器 = " + result.getAllAccumulatorResults());
        System.out.println("作业耗时 = " + result.getNetRuntime() + " ms");
    }
}
