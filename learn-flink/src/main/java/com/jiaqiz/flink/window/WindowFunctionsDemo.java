package com.jiaqiz.flink.window;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 窗口「函数接口」四条路径的对照演示：{@link WindowFunction} / {@link AllWindowFunction} /
 * {@link ProcessWindowFunction}，以及它们与增量聚合（reduce / aggregate）组合后的形态。
 *
 * <p>本 demo 覆盖：
 *
 * <ol>
 *   <li>keyed 窗口 {@code .apply(WindowFunction)} —— 窗口函数拿到的是<b>全量</b> {@code Iterable<IN>}
 *   <li>{@code .windowAll(...).apply(AllWindowFunction)} —— 非 keyed 的对应写法
 *   <li>{@code .process(ProcessWindowFunction)} —— 用 {@code Context} 拿到窗口边界
 *       {@code window().getStart()/getEnd()}、{@code currentProcessingTime()}、{@code currentWatermark()}
 *   <li>{@code .reduce(ReduceFunction, ProcessWindowFunction)} 与
 *       {@code .aggregate(AggregateFunction, ProcessWindowFunction)} —— <b>增量聚合 + 窗口函数</b>，
 *       此时 {@code Iterable} 里只有 <b>1 个元素</b>（聚合结果），这是官方推荐的性能写法
 *   <li>迟到数据的两条出口：{@code ctx.output(OutputTag, element)} 与
 *       {@code sideOutputLateData(OutputTag)} + {@code getSideOutput(OutputTag)}
 * </ol>
 *
 * <p>运行命令（有界源 + 事件时间，跑完自动退出）：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.window.WindowFunctionsDemo
 * </pre>
 *
 * <p>涉及的运行时算子：<b>全部是 {@code WindowOperator}</b>
 * （{@code flink-1.20-source/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/windowing/WindowOperator.java#L101}）。
 * 1.20 里没有 {@code ProcessWindowOperator} 这个类，窗口 + Process 只是换了一个
 * {@code InternalWindowFunction} 实现：全量路径是 {@code InternalIterableProcessWindowFunction}，
 * 增量聚合路径是 {@code InternalSingleValueProcessWindowFunction}。
 *
 * <p><b>窗口函数四件套的关系</b>：
 *
 * <pre>
 *   WindowFunction&lt;IN, OUT, KEY, W&gt;        apply(KEY, W, Iterable&lt;IN&gt;, Collector&lt;OUT&gt;)   旧接口(1.20 未废弃)
 *   AllWindowFunction&lt;IN, OUT, W&gt;          apply(W, Iterable&lt;IN&gt;, Collector&lt;OUT&gt;)         旧接口的非 keyed 版
 *   ProcessWindowFunction&lt;IN, OUT, KEY, W&gt; process(KEY, Context, Iterable&lt;IN&gt;, Collector)  新接口：多了 Context
 *   ProcessAllWindowFunction&lt;IN, OUT, W&gt;   process(Context, Iterable&lt;IN&gt;, Collector)      非 keyed 版
 * </pre>
 */
public class WindowFunctionsDemo {

    /** 固定基准时间（能被 5000 整除，窗口边界对齐到整秒）。 */
    private static final long BASE = 1700000000000L;

    /** 侧输出 1：窗口内「乱序到达」的元素（窗口已经触发过，这次是迟到补算）。 */
    public static final OutputTag<Event> LATE_IN_WINDOW = new OutputTag<Event>("late-data") {};

    /** 侧输出 2：超过 allowedLateness 的「真迟到」元素，窗口状态已经清理，无法补算，只能分流丢弃。 */
    public static final OutputTag<Event> DROPPED_LATE = new OutputTag<Event>("dropped-late") {};

    /** 演示用事件。 */
    public static class Event {
        public String key;
        public long ts;
        public int value;

        public Event() {}

        public Event(String key, long ts, int value) {
            this.key = key;
            this.ts = ts;
            this.value = value;
        }

        @Override
        public String toString() {
            return key + "@" + rel(ts) + "(v=" + value + ")";
        }
    }

    /** 简单求和聚合函数：reduce(ReduceFunction, ProcessWindowFunction) 用不上，但和 aggregate 路径对照着看。 */
    public static class SumAggregate implements AggregateFunction<Event, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            // 累加器就是「一个 int」，每个 key + 每个窗口只存这一个值
            return 0;
        }

        @Override
        public Integer add(Event value, Integer accumulator) {
            return accumulator + value.value;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            // 只有「合并窗口」（会话窗口）才会走到这里
            return a + b;
        }
    }

    private static String rel(long ts) {
        return "+" + (ts - BASE) + "ms";
    }

    private static String win(TimeWindow w) {
        return "[" + rel(w.getStart()) + ", " + rel(w.getEnd()) + ")";
    }

    /**
     * 每条记录都发一条 watermark 的事件时间生成器。
     *
     * <p>为什么不直接用 {@code WatermarkStrategy.forMonotonousTimestamps()}？
     * 它的 {@code onEvent} 只更新「见过的最大时间戳」，发水位线的动作在 {@code onPeriodicEmit} 里，
     * 由 {@code ExecutionConfig#getAutoWatermarkInterval()}（默认 <b>200ms</b>）周期驱动。
     * 本 demo 的源是有界源（{@code env.fromData(...)}），瞬间就跑完了，中间一条 watermark 都发不出来，
     * 所有窗口只能等到输入结束的 {@code MAX_WATERMARK} 才触发 —— 那样第 5 小节的
     * 「迟到 / allowedLateness」语义根本观察不到（窗口在被清理之前永远不会触发）。
     *
     * <p>所以这里显式实现「每条记录都发」。安全性来自发射顺序：
     * {@code TimestampsAndWatermarksOperator#processElement} 先 {@code output.collect(element)}、
     * 后 {@code watermarkGenerator.onEvent(...)}（{@code TimestampsAndWatermarksOperator.java#L138/L139}），
     * 水位线一定排在它自己那条元素之后，并行度 1 时顺序完全确定。
     * 生产环境用周期性 watermark 就够了。
     */
    public static class PerRecordWatermarks implements WatermarkGenerator<Event> {

        private long maxTs = Long.MIN_VALUE;

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(maxTs, eventTimestamp);
            // 单调递增语义与 forMonotonousTimestamps() 一致：watermark = 见过的最大时间戳 - 1
            output.emitWatermark(new Watermark(maxTs - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 每条都发过了，周期发射这里什么都不做
        }
    }

    private static WatermarkStrategy<Event> perRecordWatermarks() {
        return WatermarkStrategy.<Event>forGenerator(ctx -> new PerRecordWatermarks())
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.ts);
    }

    private static StreamExecutionEnvironment newEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return env;
    }

    /** 把 Iterable 拷进 List：一边打印一边迭代时，列表比 Iterable 好处理得多。 */
    private static List<Event> materialize(Iterable<Event> values) {
        List<Event> list = new ArrayList<>();
        for (Event e : values) {
            list.add(e);
        }
        return list;
    }

    // ==================================================================================
    // 各小节
    // ==================================================================================

    /** 1) keyed 窗口 + apply(WindowFunction)：窗口函数能看到窗口内全部元素。 */
    private static void demoWindowFunction() throws Exception {
        System.out.println();
        System.out.println("=== 1) keyed .apply(WindowFunction)：窗口函数拿到全量 Iterable<Event> ===");
        System.out.println("    状态：ListState<StreamRecord<Event>>，状态名 \"window-contents\"（WindowOperatorBuilder.java#L71）");
        System.out.println("    也就是说：窗口里的每一条原始元素都被存下来了，元素越多状态越大");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Event> events = env.fromData(
                        new Event("u1", BASE + 0L, 1),
                        new Event("u1", BASE + 1000L, 2),
                        new Event("u2", BASE + 2000L, 10),
                        new Event("u1", BASE + 3000L, 3),
                        new Event("u2", BASE + 4000L, 20))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        KeyedStream<Event, String> keyed = events.keyBy(e -> e.key);

        WindowedStream<Event, String, TimeWindow> windowed =
                keyed.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)));

        SingleOutputStreamOperator<String> result =
                windowed.apply(
                        new WindowFunction<Event, String, String, TimeWindow>() {
                            @Override
                            public void apply(
                                    String key,
                                    TimeWindow window,
                                    Iterable<Event> input,
                                    Collector<String> out) {
                                List<Event> list = materialize(input);
                                System.out.println("    [WindowFunction] key=" + key + " 窗口 " + win(window)
                                        + " 元素数=" + list.size() + " -> " + list);
                                out.collect("[WindowFunction] key=" + key + " " + win(window)
                                        + " 共 " + list.size() + " 条");
                            }
                        });

        result.print("1-WindowFunction");
        env.execute("WindowFunctionsDemo-1-windowfunction");
    }

    /** 2) windowAll + apply(AllWindowFunction)：非 keyed 的对应写法。 */
    private static void demoAllWindowFunction() throws Exception {
        System.out.println();
        System.out.println("=== 2) .windowAll(...).apply(AllWindowFunction)：非 keyed 版本 ===");
        System.out.println("    windowAll 没有 key，所有数据进同一个算子实例（并行度被强制为 1），是全局统计的写法");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Event> events = env.fromData(
                        new Event("u1", BASE + 0L, 1),
                        new Event("u2", BASE + 1000L, 2),
                        new Event("u1", BASE + 3000L, 3),
                        new Event("u2", BASE + 6000L, 4))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        AllWindowedStream<Event, TimeWindow> windowed =
                events.windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(5)));

        SingleOutputStreamOperator<String> result =
                windowed.apply(
                        new AllWindowFunction<Event, String, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<Event> values, Collector<String> out) {
                                List<Event> list = materialize(values);
                                System.out.println("    [AllWindowFunction] 窗口 " + win(window)
                                        + " 元素数=" + list.size() + " -> " + list);
                                out.collect("[AllWindowFunction] " + win(window) + " 共 " + list.size() + " 条");
                            }
                        });

        result.print("2-AllWindowFunction");
        env.execute("WindowFunctionsDemo-2-allwindowfunction");
    }

    /** 3) process(ProcessWindowFunction)：Context 给出窗口边界与时间信息。 */
    private static void demoProcessWindowFunction() throws Exception {
        System.out.println();
        System.out.println("=== 3) .process(ProcessWindowFunction)：Context 能拿到窗口边界与时间 ===");
        System.out.println("    currentProcessingTime() 是机器墙钟（每次运行都不同，和时间语义无关），");
        System.out.println("    currentWatermark() 是算子当前的水位线 —— 窗口就是在水位线越过 maxTimestamp 时触发的");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Event> events = env.fromData(
                        new Event("u1", BASE + 0L, 1),
                        new Event("u1", BASE + 2000L, 2),
                        new Event("u1", BASE + 4000L, 3))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        KeyedStream<Event, String> keyed = events.keyBy(e -> e.key);

        SingleOutputStreamOperator<String> result =
                keyed.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                        .process(
                                new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                                    @Override
                                    public void process(
                                            String key,
                                            Context context,
                                            Iterable<Event> elements,
                                            Collector<String> out) {
                                        List<Event> list = materialize(elements);
                                        // 1) 窗口边界：getStart() 含、getEnd() 不含（右开区间）
                                        long start = context.window().getStart();
                                        long end = context.window().getEnd();
                                        // 2) 时间信息：处理时间 + 当前水位线
                                        long processingTime = context.currentProcessingTime();
                                        long watermark = context.currentWatermark();
                                        System.out.println("    [ProcessWindowFunction] key=" + key
                                                + " window=[" + rel(start) + ", " + rel(end) + ")"
                                                + " maxTimestamp=" + rel(context.window().maxTimestamp())
                                                + " 元素数=" + list.size());
                                        System.out.println("        currentProcessingTime=" + processingTime
                                                + "（墙钟毫秒，截断显示 " + (processingTime % 100000) + "）"
                                                + " currentWatermark=" + rel(watermark));
                                        System.out.println("        内容 -> " + list);
                                        out.collect("[ProcessWindowFunction] key=" + key + " [" + rel(start)
                                                + ", " + rel(end) + ") 元素数=" + list.size()
                                                + " watermark=" + rel(watermark));
                                    }
                                });

        result.print("3-ProcessWindowFunction");
        env.execute("WindowFunctionsDemo-3-processwindowfunction");
    }

    /** 4) 增量聚合 + 窗口函数：Iterable 里只剩 1 个元素。 */
    private static void demoIncrementalWithWindowFunction() throws Exception {
        System.out.println();
        System.out.println("=== 4) 增量聚合 + ProcessWindowFunction：Iterable 里只有 1 个元素 ===");
        System.out.println("    .reduce(ReduceFunction, ProcessWindowFunction) / .aggregate(AggregateFunction, ProcessWindowFunction)");
        System.out.println("    每来一条数据就地和累加器合并（内层状态是 ReducingState / AggregatingState，只存 1 个累加器），");
        System.out.println("    触发时才把累加器包装成单元素集合交给窗口函数 —— 这就是官方推荐的写法");
        System.out.println("    对比第 1 节：全量路径存的是每个元素（ListState），这里只存 1 个累加器，状态量差好几条街");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Event> events = env.fromData(
                        new Event("u1", BASE + 0L, 1),
                        new Event("u1", BASE + 1000L, 2),
                        new Event("u1", BASE + 3000L, 3))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        KeyedStream<Event, String> keyed = events.keyBy(e -> e.key);

        // 4.1 reduce(ReduceFunction, ProcessWindowFunction)
        SingleOutputStreamOperator<String> byReduce =
                keyed.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                        .reduce(
                                // 就地合并：两条元素合成一条（这里是 value 相加）
                                (ReduceFunction<Event>) (a, b) -> new Event(a.key, a.ts, a.value + b.value),
                                new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                                    @Override
                                    public void process(
                                            String key,
                                            Context context,
                                            Iterable<Event> elements,
                                            Collector<String> out) {
                                        List<Event> list = materialize(elements);
                                        System.out.println("    [reduce+Process] key=" + key + " 窗口 "
                                                + win(context.window()) + " Iterable 元素数=" + list.size()
                                                + " -> " + list + "（注意：只有 1 个，是归约结果本身）");
                                        out.collect("[reduce+Process] key=" + key + " 归约结果=" + list);
                                    }
                                });

        byReduce.print("4a-reduce+Process");

        // 4.2 aggregate(AggregateFunction, ProcessWindowFunction)
        SingleOutputStreamOperator<String> byAggregate =
                keyed.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                        .aggregate(
                                new SumAggregate(),
                                new ProcessWindowFunction<Integer, String, String, TimeWindow>() {
                                    @Override
                                    public void process(
                                            String key,
                                            Context context,
                                            Iterable<Integer> elements,
                                            Collector<String> out) {
                                        List<Integer> list = new ArrayList<>();
                                        for (Integer v : elements) {
                                            list.add(v);
                                        }
                                        System.out.println("    [aggregate+Process] key=" + key + " 窗口 "
                                                + win(context.window()) + " Iterable 元素数=" + list.size()
                                                + " -> " + list + "（累加器结果）"
                                                + " | watermark=" + rel(context.currentWatermark()));
                                        out.collect("[aggregate+Process] key=" + key + " 求和=" + list);
                                    }
                                });

        byAggregate.print("4b-aggregate+Process");

        // 两条支路在同一个作业里串行不了，但都是同一个 key、同一个窗口，输出对照看即可
        env.execute("WindowFunctionsDemo-4-incremental");
    }

    /** 5) 迟到数据：ctx.output 与 sideOutputLateData 两条出口。 */
    private static void demoLateData() throws Exception {
        System.out.println();
        System.out.println("=== 5) 迟到数据的两个出口 ===");
        System.out.println("    (a) 窗口内乱序到达：把元素放进窗口，同时用 ctx.output(OutputTag, element) 记一份到侧输出");
        System.out.println("    (b) 超过 allowedLateness：窗口状态已经清理，无法补算，被 sideOutputLateData 分流");
        System.out.println("    数据（tumbling 5s，allowedLateness=2s，key=u1）：");
        System.out.println("      +1000 → 窗口[+0,+5000)；  +0 → 同一窗口，但比已见过的 +1000 早 ⇒ 乱序，走 (a)");
        System.out.println("      +6000 → 水位线推到 +5999，窗口[+0,+5000) 首次触发");
        System.out.println("      +2000 → 该窗口清理时刻是 +4999+2000=+6999 > 水位线 +5999 ⇒ 仍在 allowedLateness 内，");
        System.out.println("              元素被补进窗口并触发第二次输出（EventTimeTrigger 在 watermark 已过 maxTimestamp 时直接 FIRE）");
        System.out.println("      +10000 → 水位线推到 +9999，窗口[+0,+5000) 被清理");
        System.out.println("      +500   → 窗口已清理 + 超过 allowedLateness ⇒ 走 (b) 侧输出");
        System.out.println("    注意两点实际现象：");
        System.out.println("      1. 第二次输出时 Iterable 里是【整个窗口的全部元素】= 3 条（不是只有迟到的那一条），");
        System.out.println("         也就是同一窗口的结果被重复输出了 —— 下游必须能按窗口去重，这是 allowedLateness 的代价；");
        System.out.println("      2. 乱序检测会在\"补算\"时把 +0 再报一次，因为窗口内容被整体重放了（不是 bug，是语义）。");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Event> events = env.fromData(
                        new Event("u1", BASE + 1000L, 1),
                        new Event("u1", BASE + 0L, 2),
                        new Event("u1", BASE + 6000L, 3),
                        new Event("u1", BASE + 2000L, 4),
                        new Event("u1", BASE + 10000L, 5),
                        new Event("u1", BASE + 500L, 6))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        WindowedStream<Event, String, TimeWindow> windowed =
                events.keyBy(e -> e.key)
                        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                        // 允许迟到 2s：窗口触发后不清状态，迟到的元素还能补算并再次输出
                        .allowedLateness(Duration.ofSeconds(2))
                        // 超过 2s 的迟到元素分流到侧输出（不设置的话就是直接丢弃 + 计数器 numLateRecordsDropped）
                        .sideOutputLateData(DROPPED_LATE);

        SingleOutputStreamOperator<String> result =
                windowed.process(
                        new ProcessWindowFunction<Event, String, String, TimeWindow>() {

                            /** 窗口状态：这个窗口已经见过的最大事件时间戳，用来识别「乱序到达」的元素。 */
                            private final ValueStateDescriptor<Long> maxTsDesc =
                                    new ValueStateDescriptor<>("max-event-ts", Long.class);

                            @Override
                            public void process(
                                    String key,
                                    Context context,
                                    Iterable<Event> elements,
                                    Collector<String> out) throws Exception {
                                List<Event> list = materialize(elements);

                                // windowState() 是 KeyedStateStore，作用域自动限定在「当前 key + 当前窗口」
                                ValueState<Long> maxTs = context.windowState().getState(maxTsDesc);
                                long running = maxTs.value() == null ? Long.MIN_VALUE : maxTs.value();

                                for (Event e : list) {
                                    if (e.ts < running) {
                                        // 比同一个窗口里已经见过的元素还早 ⇒ 乱序（迟到但还在 allowedLateness 内）
                                        context.output(LATE_IN_WINDOW, e);
                                    }
                                    running = Math.max(running, e.ts);
                                }
                                maxTs.update(running);

                                System.out.println("    [迟到演示] key=" + key + " 窗口 " + win(context.window())
                                        + " 水位线=" + rel(context.currentWatermark())
                                        + " Iterable 元素数=" + list.size() + " -> " + list
                                        + (list.size() > 2 ? "  <== 这是窗口被迟到元素\"补算\"后的再次输出" : ""));
                                out.collect("[迟到演示] " + win(context.window()) + " 内容=" + list
                                        + " watermark=" + rel(context.currentWatermark()));
                            }
                        });

        result.print("5-主输出");
        // ctx.output 写进去的乱序元素
        result.getSideOutput(LATE_IN_WINDOW).print("5-侧输出(late-data 窗口内乱序)");
        // sideOutputLateData 分流的真迟到元素
        result.getSideOutput(DROPPED_LATE).print("5-侧输出(超过 allowedLateness)");

        env.execute("WindowFunctionsDemo-5-late");
    }

    // ==================================================================================

    public static void main(String[] args) throws Exception {
        System.out.println("### WindowFunctionsDemo：WindowFunction / AllWindowFunction / ProcessWindowFunction ###");
        System.out.println("### 每小节一个独立本地作业，串行执行；watermark 每条记录推进一次，窗口随水位线触发 ###");

        demoWindowFunction();
        demoAllWindowFunction();
        demoProcessWindowFunction();
        demoIncrementalWithWindowFunction();
        demoLateData();

        System.out.println();
        System.out.println("=== 全部小节结束 ===");
        System.out.println("选型结论：");
        System.out.println("  只做聚合 → .reduce / .aggregate（增量聚合，状态 = 1 个累加器/键/窗口）");
        System.out.println("  需要窗口元信息（起止时间、水位线、侧输出） → .aggregate(af, ProcessWindowFunction) 或 .reduce(rf, ProcessWindowFunction)");
        System.out.println("  需要遍历窗口内每一条原始元素 → .apply(WindowFunction) / .process(ProcessWindowFunction)，代价是全量元素常驻状态");
    }
}
