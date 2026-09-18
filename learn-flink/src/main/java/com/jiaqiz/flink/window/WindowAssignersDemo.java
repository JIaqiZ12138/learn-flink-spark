package com.jiaqiz.flink.window;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口三件套 <b>WindowAssigner / Trigger / Evictor</b> 的配合关系演示。
 *
 * <p>本 demo 覆盖：
 *
 * <ol>
 *   <li>{@link TumblingEventTimeWindows#of(Duration)} —— 翻滚窗口（窗口之间不重叠）
 *   <li>{@link SlidingEventTimeWindows#of(Duration, Duration)} —— 滑动窗口（一条数据会落进多个窗口）
 *   <li>{@link EventTimeSessionWindows#withGap(Duration)} —— 会话窗口（<b>合并窗口</b>，见下文）
 *   <li>自定义 {@link Trigger} —— 窗口内元素条数达到阈值就<b>提前触发</b>
 *   <li>{@link CountEvictor#of(long)} —— 驱逐器，只保留最后 N 条元素
 * </ol>
 *
 * <p>运行命令（有界源 + 事件时间，作业会自己结束，不需要 ctrl-c）：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.window.WindowAssignersDemo
 * </pre>
 *
 * <p>涉及的运行时算子（源码：{@code flink-1.20-source/flink-streaming-java/}）：
 *
 * <pre>
 *   普通窗口（keyed windowAll 都算）      → WindowOperator          runtime/operators/windowing/WindowOperator.java#L101
 *   只要调用了 .evictor(...)             → EvictingWindowOperator  runtime/operators/windowing/EvictingWindowOperator.java#L63
 *                                          选择逻辑见 WindowOperatorBuilder#L293 buildEvictingWindowOperator
 * </pre>
 *
 * <p><b>1.20 的 API 事实（照 1.13 之前的文档写会编译不过）</b>：
 *
 * <ul>
 *   <li>{@code org.apache.flink.streaming.api.windowing.assigners.SessionWindows} 这个类在 1.20.4 里
 *       <b>已经不存在</b>（jar 里没有这个 class），会话窗口被拆成
 *       {@link EventTimeSessionWindows}（事件时间）与
 *       {@code ProcessingTimeSessionWindows}（处理时间）两个类。本 demo 用前者。
 *   <li>时间参数推荐 {@link Duration} 版本的重载：{@code of(Duration)} / {@code withGap(Duration)}，
 *       {@code org.apache.flink.streaming.api.windowing.time.Time} 版本仍可用但已经不推荐。
 * </ul>
 *
 * <p><b>三者的分工</b>：Assigner 决定「哪条数据进哪些窗口」，Trigger 决定「什么时候把窗口的结果算出来并输出」，
 * Evictor 决定「输出之前先扔掉哪几条元素」。三者叠加进同一个算子：{@code WindowOperatorBuilder}。
 */
public class WindowAssignersDemo {

    /** 固定基准时间，保证窗口边界对齐到整秒，便于人工核对（1700000000000 % 5000 == 0）。 */
    private static final long BASE = 1700000000000L;

    /** 演示用事件。POJO 约定：public 字段 + public 无参构造。 */
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

    /** 时间戳相对基准时间的可读表示，避免输出里出现 13 位大整数。 */
    private static String rel(long ts) {
        return "+" + (ts - BASE) + "ms";
    }

    /** 窗口区间的可读表示。注意会话窗口的 getEnd() == maxTimestamp() + 1，是「右开」区间。 */
    private static String win(TimeWindow w) {
        return "[" + rel(w.getStart()) + ", " + rel(w.getEnd()) + ")";
    }

    /**
     * 每条记录都发一条 watermark 的事件时间生成器。
     *
     * <p>为什么不直接用 {@code WatermarkStrategy.forMonotonousTimestamps()}？
     * 它内部是 {@code AscendingTimestampsWatermarks}：{@code onEvent} 只更新「见过的最大时间戳」，
     * 真正发水位线的动作在 {@code onPeriodicEmit} 里，由
     * {@code ExecutionConfig#getAutoWatermarkInterval()}（默认 <b>200ms</b>）周期性驱动。
     *
     * <p>本 demo 的源是「瞬间跑完」的有界源（{@code env.fromData(...)}），耗时远小于一个 200ms 周期，
     * 结果中间一条 watermark 都发不出来，所有窗口只能等输入结束时那条 {@code MAX_WATERMARK} 才触发。
     * 那样结果虽然正确，但「窗口随着水位线推进被逐个触发」的过程就完全看不见了。
     *
     * <p>所以这里显式实现一个「每条记录都发」的生成器。之所以安全，是因为
     * {@code TimestampsAndWatermarksOperator#processElement} 的顺序是
     * <pre>
     *   output.collect(element);                              // 先转发元素
     *   watermarkGenerator.onEvent(event, ts, wmOutput);      // 再发水位线
     * </pre>
     * （{@code TimestampsAndWatermarksOperator.java#L138/L139}），
     * watermark 一定排在它自己那条元素之后，不会「追尾」，并行度 1 时顺序完全确定。
     * 生产环境不需要这么干，周期性 watermark 就够了（本 demo 只是为了让有界源也能演示推进过程）。
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

    /** 事件时间 + 每条记录都推进水位线的 watermark 策略。 */
    private static WatermarkStrategy<Event> perRecordWatermarks() {
        return WatermarkStrategy.<Event>forGenerator(ctx -> new PerRecordWatermarks())
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.ts);
    }

    /**
     * 新建一个并行度为 1 的本地执行环境。
     *
     * <p>每个小节都用独立的环境 + 独立的 {@code execute()}：这样小节之间是<b>串行</b>的，
     * 不会出现多个算子链的输出互相交错，读起来才像一份「实验记录」。
     */
    private static StreamExecutionEnvironment newEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度 1：print() 的顺序稳定、可逐行核对
        env.setParallelism(1);
        return env;
    }

    // ==================================================================================
    // 自定义 Trigger：窗口内条数到达阈值就提前触发
    // ==================================================================================

    /**
     * 自定义 {@link Trigger}：每收到 {@code maxCount} 条元素就提前触发一次
     * （返回 {@code FIRE_AND_PURGE}，触发后清空窗口内容并从 0 重新计数），
     * 窗口结束时再由事件时间定时器正常触发一次。
     *
     * <p>写自定义 Trigger 必须注意三件事：
     *
     * <ol>
     *   <li>{@code onElement} 里一定要 {@code ctx.registerEventTimeTimer(window.maxTimestamp())}，
     *       否则「窗口结束」这个时机就永远没人通知你（默认的 EventTimeTrigger 也是这么写的）。
     *   <li>触发器自己的状态要用 {@code ctx.getPartitionedState(descriptor)}：它按 key + window 分区，
     *       这样每个窗口各数各的，不会串味。
     *   <li>{@code clear()} 要在窗口被彻底清理时把状态和定时器一起释放，否则状态泄漏。
     * </ol>
     *
     * <p>{@code canMerge()} 默认返回 {@code false}（基类实现），本 demo 只把它配给翻滚窗口，
     * 所以不需要重写；如果配给会话窗口，必须重写 {@code canMerge()} 返回 true 并实现 {@code onMerge}，
     * 否则 {@code WindowedStream#trigger} 会直接抛
     * {@code UnsupportedOperationException("A merging window assigner cannot be used with a trigger that does not support merging.")}。
     */
    public static class EarlyFireCountTrigger extends Trigger<Object, TimeWindow> {

        private static final long serialVersionUID = 1L;

        private final long maxCount;

        /** 分区状态：当前窗口已经到达了多少条元素。ReducingState 用 Long::sum 做累加。 */
        private final ReducingStateDescriptor<Long> countDesc =
                new ReducingStateDescriptor<>("early-fire-count", Long::sum, LongSerializer.INSTANCE);

        public EarlyFireCountTrigger(long maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public TriggerResult onElement(
                Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 1) 仍然要注册「窗口结束」的事件时间定时器
            ctx.registerEventTimeTimer(window.maxTimestamp());

            // 2) 计数 +1。ReducingState 在状态为空时 add(x) 直接得到 x，不会 NPE
            ReducingState<Long> count = ctx.getPartitionedState(countDesc);
            count.add(1L);
            Long current = count.get();

            // 3) 到达阈值：提前触发并清空窗口内容，同时把自己的计数清零，窗口从 0 重新开始数
            if (current != null && current >= maxCount) {
                count.clear();
                return TriggerResult.FIRE_AND_PURGE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
            // 纯事件时间语义，不注册处理时间定时器，所以这里永远返回 CONTINUE
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            // 只认「窗口结束」这一个定时器时刻
            return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            // 窗口被清理时释放自己的状态与定时器，避免状态泄漏
            ctx.getPartitionedState(countDesc).clear();
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }
    }

    // ==================================================================================
    // 各小节
    // ==================================================================================

    /** 1) 翻滚窗口 + windowAll（非 keyed 的 AllWindowedStream）。 */
    private static void demoTumblingWindowAll() throws Exception {
        System.out.println("=== 1) 翻滚窗口 TumblingEventTimeWindows.of(Duration.ofSeconds(5)) + windowAll（非 keyed）===");
        System.out.println("    算子：WindowOperator；windowAll 内部 keyBy(常量) + forceNonParallel()，并行度恒为 1");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Event> events = env.fromData(
                        new Event("u1", BASE + 0L, 1),
                        new Event("u2", BASE + 1000L, 2),
                        new Event("u1", BASE + 3000L, 3),
                        new Event("u2", BASE + 5500L, 4),
                        new Event("u1", BASE + 6000L, 5),
                        new Event("u2", BASE + 9000L, 6))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        // windowAll 返回 AllWindowedStream：没有 key，全量数据进同一个算子实例
        AllWindowedStream<Event, TimeWindow> windowed =
                events.windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(5)));

        SingleOutputStreamOperator<String> result =
                windowed.apply(
                        new AllWindowFunction<Event, String, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<Event> values, Collector<String> out) {
                                List<Event> list = new ArrayList<>();
                                for (Event e : values) {
                                    list.add(e);
                                }
                                // 窗口边界 + 这个窗口到底收到了哪几条数据，全部打出来
                                System.out.println("    [翻滚] 窗口 " + win(window)
                                        + " maxTimestamp=" + rel(window.maxTimestamp())
                                        + " 元素数=" + list.size() + " -> " + list);
                                out.collect("[翻滚] " + win(window) + " 汇总 " + list.size() + " 条: " + list);
                            }
                        });

        result.print("1-翻滚");

        // 单独 execute：保证本小节输出完整、不与其它小节交错
        env.execute("WindowAssignersDemo-1-tumbling");
    }

    /** 2) 滑动窗口（keyed），一条数据会落进多个窗口。 */
    private static void demoSlidingKeyed() throws Exception {
        System.out.println();
        System.out.println("=== 2) 滑动窗口 SlidingEventTimeWindows.of(10s, 5s)（keyed + WindowFunction）===");
        System.out.println("    窗口大小 10s、滑动步长 5s ⇒ 每条数据会被分到 2 个窗口（除了边界），这就是滑动窗口的\"重复计算\"代价");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Event> events = env.fromData(
                        new Event("u1", BASE + 0L, 1),
                        new Event("u1", BASE + 1000L, 2),
                        // 时间戳保持非递减：事件时间窗口 + 会推进的水位线下，乱序到达会被当成迟到数据丢掉
                        new Event("u2", BASE + 2000L, 9),
                        new Event("u1", BASE + 6000L, 3))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        KeyedStream<Event, String> keyed = events.keyBy(e -> e.key);

        WindowedStream<Event, String, TimeWindow> windowed =
                keyed.window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)));

        SingleOutputStreamOperator<String> result =
                windowed.apply(
                        new WindowFunction<Event, String, String, TimeWindow>() {
                            @Override
                            public void apply(
                                    String key,
                                    TimeWindow window,
                                    Iterable<Event> input,
                                    Collector<String> out) {
                                List<Event> list = new ArrayList<>();
                                for (Event e : input) {
                                    list.add(e);
                                }
                                System.out.println("    [滑动] key=" + key + " 窗口 " + win(window)
                                        + " 元素数=" + list.size() + " -> " + list);
                                out.collect("[滑动] key=" + key + " " + win(window) + " -> " + list);
                            }
                        });

        result.print("2-滑动");

        env.execute("WindowAssignersDemo-2-sliding");
    }

    /** 3) 会话窗口：合并窗口（MergingWindowAssigner）。 */
    private static void demoSessionMerging() throws Exception {
        System.out.println();
        System.out.println("=== 3) 会话窗口 EventTimeSessionWindows.withGap(Duration.ofSeconds(2))（合并窗口）===");
        System.out.println("    1.20.4 里已经没有 SessionWindows 这个类，事件时间会话窗口叫 EventTimeSessionWindows");
        System.out.println("    它是 MergingWindowAssigner：新元素可能把相邻两个窗口\"粘\"成一个，窗口边界因此会变宽");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Event> events = env.fromData(
                        new Event("u1", BASE + 0L, 1),
                        new Event("u1", BASE + 1000L, 2),
                        new Event("u2", BASE + 5000L, 3),
                        new Event("u2", BASE + 6000L, 4),
                        new Event("u1", BASE + 12000L, 5))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        AllWindowedStream<Event, TimeWindow> windowed =
                events.windowAll(EventTimeSessionWindows.withGap(Duration.ofSeconds(2)));

        SingleOutputStreamOperator<String> result =
                windowed.apply(
                        new AllWindowFunction<Event, String, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<Event> values, Collector<String> out) {
                                List<Event> list = new ArrayList<>();
                                for (Event e : values) {
                                    list.add(e);
                                }
                                System.out.println("    [会话] 窗口 " + win(window)
                                        + " 宽度=" + (window.getEnd() - window.getStart()) + "ms"
                                        + " 元素数=" + list.size() + " -> " + list);
                                out.collect("[会话] " + win(window) + " 宽度="
                                        + (window.getEnd() - window.getStart()) + "ms -> " + list);
                            }
                        });

        result.print("3-会话");

        env.execute("WindowAssignersDemo-3-session");
    }

    /** 4) 自定义 Trigger：按条数提前触发。 */
    private static void demoCustomTrigger() throws Exception {
        System.out.println();
        System.out.println("=== 4) 自定义 Trigger：EarlyFireCountTrigger（每满 2 条提前触发一次）===");
        System.out.println("    窗口：10s 翻滚窗口，5 条数据（+0 ~ +4000ms）全部落在 [  +0ms, +10000ms) 里");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Event> events = env.fromData(
                        new Event("u1", BASE + 0L, 1),
                        new Event("u1", BASE + 1000L, 2),
                        new Event("u1", BASE + 2000L, 3),
                        new Event("u1", BASE + 3000L, 4),
                        new Event("u1", BASE + 4000L, 5))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        KeyedStream<Event, String> keyed = events.keyBy(e -> e.key);

        SingleOutputStreamOperator<String> result =
                keyed.window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                        // 换成内置的「每隔 N 秒事件时间触发一次」只需：.trigger(ContinuousEventTimeTrigger.of(Duration.ofSeconds(2)))
                        .trigger(new EarlyFireCountTrigger(2))
                        .apply(
                                new WindowFunction<Event, String, String, TimeWindow>() {
                                    @Override
                                    public void apply(
                                            String key,
                                            TimeWindow window,
                                            Iterable<Event> input,
                                            Collector<String> out) {
                                        List<Event> list = new ArrayList<>();
                                        for (Event e : input) {
                                            list.add(e);
                                        }
                                        System.out.println("    [自定义Trigger] 触发! key=" + key + " 窗口 " + win(window)
                                                + " 本次内容(" + list.size() + " 条)=" + list);
                                        out.collect("[自定义Trigger] " + win(window) + " 本次触发内容=" + list);
                                    }
                                });

        result.print("4-自定义Trigger");

        env.execute("WindowAssignersDemo-4-trigger");
    }

    /** 5) 驱逐器 CountEvictor：触发前先扔掉多余元素。 */
    private static void demoCountEvictor() throws Exception {
        System.out.println();
        System.out.println("=== 5) 驱逐器 CountEvictor.of(2)（只保留窗口内最后 2 条）===");
        System.out.println("    只要调用了 .evictor(...)，算子就从 WindowOperator 变成 EvictingWindowOperator：");
        System.out.println("    窗口内容状态是 ListState<StreamRecord<Event>>（状态名 \"window-contents\"），");
        System.out.println("    因此配置 evictor 后窗口函数拿到的是 Iterable<Event>（全量），不能再用增量聚合");

        StreamExecutionEnvironment env = newEnv();

        DataStream<Event> events = env.fromData(
                        new Event("u1", BASE + 0L, 1),
                        new Event("u1", BASE + 1000L, 2),
                        new Event("u1", BASE + 2000L, 3),
                        new Event("u2", BASE + 3000L, 4),
                        new Event("u1", BASE + 4000L, 5),
                        new Event("u2", BASE + 5000L, 6))
                .assignTimestampsAndWatermarks(perRecordWatermarks());

        WindowedStream<Event, String, TimeWindow> windowed =
                events.keyBy(e -> e.key)
                        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                        // 触发前驱逐：窗口里最多留 2 条（CountEvictor.of(2, true) 则是触发后再驱逐）
                        .evictor(CountEvictor.of(2));

        SingleOutputStreamOperator<String> result =
                windowed.apply(
                        new WindowFunction<Event, String, String, TimeWindow>() {
                            @Override
                            public void apply(
                                    String key,
                                    TimeWindow window,
                                    Iterable<Event> input,
                                    Collector<String> out) {
                                List<Event> list = new ArrayList<>();
                                for (Event e : input) {
                                    list.add(e);
                                }
                                System.out.println("    [驱逐器] key=" + key + " 窗口 " + win(window)
                                        + " 驱逐后剩余(" + list.size() + " 条)=" + list);
                                out.collect("[驱逐器] key=" + key + " " + win(window) + " -> " + list);
                            }
                        });

        result.print("5-驱逐器");

        env.execute("WindowAssignersDemo-5-evictor");
    }

    // ==================================================================================

    public static void main(String[] args) throws Exception {
        System.out.println("### WindowAssignersDemo：WindowAssigner / Trigger / Evictor ###");
        System.out.println("### 每个小节都是独立的本地作业，串行执行；有界源 + 事件时间 watermark，跑完自动退出 ###");

        demoTumblingWindowAll();
        demoSlidingKeyed();
        demoSessionMerging();
        demoCustomTrigger();
        demoCountEvictor();

        System.out.println();
        System.out.println("=== 全部小节结束 ===");
        System.out.println("回收一下结论：");
        System.out.println("  1. Assigner 决定数据进哪些窗口 —— 翻滚/滑动是固定边界，会话窗口会合并（边界动态变化）");
        System.out.println("  2. Trigger 决定何时触发 —— 默认 EventTimeTrigger 在 watermark 越过 window.maxTimestamp() 时触发");
        System.out.println("  3. Evictor 决定输出前扔掉什么 —— 配了它算子就变 EvictingWindowOperator，且与增量聚合互斥");
    }
}
