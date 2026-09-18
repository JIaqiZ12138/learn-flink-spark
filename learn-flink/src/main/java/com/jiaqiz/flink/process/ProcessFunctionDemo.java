package com.jiaqiz.flink.process;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * ProcessFunction / KeyedProcessFunction 低阶算子演示。
 *
 * <p>演示内容：
 *
 * <ol>
 *   <li>{@link ProcessFunction}（非 keyed）：用侧输出 {@code ctx.output(OutputTag, value)} 把「异常数据」和
 *       「迟到数据」分流到侧输出流，主输出只保留正常数据。
 *   <li>{@link KeyedProcessFunction} + <b>处理时间</b>定时器：
 *       {@code ctx.timerService().registerProcessingTimeTimer(...)} 注册，{@code onTimer} 里输出该 key
 *       的累计结果。
 *   <li>{@link KeyedProcessFunction} + <b>事件时间</b>定时器：{@code registerEventTimeTimer(...)} +
 *       {@code ValueState} 累加；必须先 {@code assignTimestampsAndWatermarks}，watermark 越过定时器时间戳，
 *       定时器才会触发。
 * </ol>
 *
 * <p><b>重要的 API 事实</b>：Flink 1.20 里<b>非 keyed</b> 的 {@code ProcessFunction} 调
 * {@code registerProcessingTimeTimer / registerEventTimeTimer} 会直接抛
 * {@code UnsupportedOperationException("Setting timers is only supported on a keyed streams.")}
 * （见 {@code ProcessOperator.ContextImpl}）；{@code currentProcessingTime()}、{@code currentWatermark()}、
 * {@code output(...)} 这些是能用的。所以定时器必须挂在 keyed 流上，本 demo 用第一个分支把这个异常<b>真实跑出来</b>，
 * 定时器的真正用法放在 keyed 的两个分支里。
 *
 * <p>运行：
 *
 * <pre>
 * mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.process.ProcessFunctionDemo
 * </pre>
 */
public class ProcessFunctionDemo {

    /** 传感器读数。POJO 约定：public 字段 + public 无参构造，Flink 才能用 POJO 序列化器。 */
    public static class Sensor {
        public String id;
        public long ts;
        public double temp;

        public Sensor() {}

        public Sensor(String id, long ts, double temp) {
            this.id = id;
            this.ts = ts;
            this.temp = temp;
        }

        @Override
        public String toString() {
            return "Sensor{id=" + id + ", ts=" + ts + ", temp=" + temp + "}";
        }
    }

    // ==================================================================================
    // 1) 非 keyed 的 ProcessFunction：侧输出分流（顺便验证「非 keyed 不能注册定时器」）
    // ==================================================================================

    /**
     * 非 keyed 的 {@link ProcessFunction}：主输出正常数据，异常/迟到数据各走一条侧输出。
     *
     * <p>为什么这么写：
     *
     * <ul>
     *   <li>侧输出标签必须用匿名子类 {@code new OutputTag<T>("id") {}} 创建：{@code OutputTag} 的 id
     *       构造器会反射当前实例的类来解析泛型 T，直接 new 会丢掉类型信息。
     *   <li>「迟到」判定用的是算子自己记住的 maxTs：非 keyed 流没有 keyed state，这里退化成普通字段
     *       （本 demo 并行度为 1）；生产里应该用 watermark + allowedLateness 判定。
     * </ul>
     */
    public static class SensorSplitter extends ProcessFunction<Sensor, String> {

        /** 异常数据侧输出。 */
        public static final OutputTag<Sensor> ABNORMAL = new OutputTag<Sensor>("abnormal") {};

        /** 迟到（乱序）数据侧输出。 */
        public static final OutputTag<Sensor> LATE = new OutputTag<Sensor>("late") {};

        private long maxTsSeen = Long.MIN_VALUE;
        private boolean timerProbeDone = false;

        @Override
        public void processElement(Sensor sensor, Context ctx, Collector<String> out) throws Exception {
            // 1) 时间戳回退 = 迟到数据：分到侧输出，不参与主输出
            if (sensor.ts < maxTsSeen) {
                ctx.output(LATE, sensor);
                return;
            }
            maxTsSeen = sensor.ts;

            // 2) 业务校验失败 = 异常数据：同样走侧输出
            if (sensor.temp < -50.0 || sensor.temp > 100.0) {
                ctx.output(ABNORMAL, sensor);
                return;
            }

            // 3) 正常数据走主输出；顺便验证非 keyed 流的 TimerService 能力边界（只验证一次）
            if (!timerProbeDone) {
                timerProbeDone = true;
                try {
                    ctx.timerService()
                            .registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 100L);
                    out.collect("[定时器] 非 keyed 流注册处理时间定时器成功");
                } catch (UnsupportedOperationException e) {
                    out.collect("[定时器] 非 keyed 流注册定时器被拒绝: \"" + e.getMessage()
                            + "\"（currentProcessingTime() 可读=" + ctx.timerService().currentProcessingTime()
                            + "；定时器用法见本文件 keyed 分支）");
                }
            }
            // 没经过 assignTimestampsAndWatermarks 的流，ctx.timestamp() 是默认的 Long.MIN_VALUE
            // （如果上游记录连时间戳字段都没有，这里会返回 null）
            out.collect("主输出 正常数据 -> " + sensor + " | ctx.timestamp()=" + ctx.timestamp());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 非 keyed 分支在 Flink 1.20 里注册不了定时器，所以这里永远进不来；
            // 保留方法是为了展示签名（OnTimerContext 比 Context 多一个 timeDomain()）。
            out.collect("[onTimer] 不该出现: timerTs=" + timestamp + " timeDomain=" + ctx.timeDomain());
        }
    }

    // ==================================================================================
    // 2) keyed + 处理时间定时器
    // ==================================================================================

    /**
     * keyed 的处理时间定时器：每个 key 的第一条数据到达时注册「当前处理时间 + 300ms」的定时器，
     * 定时器触发时输出该 key 当时累计到的条数。
     *
     * <p>处理时间 = 机器的墙钟，和数据的 event time 无关；所以定时器什么时刻触发、触发时能看到几条数据，
     * 取决于数据到达快慢（这正是它和事件时间定时器的本质区别）。
     */
    public static class ProcessingTimeSummary extends KeyedProcessFunction<String, Sensor, String> {

        private static final long DELAY_MS = 300L;

        private transient ValueState<Integer> count;

        @Override
        public void open(OpenContext openContext) throws Exception {
            // Flink 1.19+ 推荐覆盖 open(OpenContext)，旧的 open(Configuration) 已废弃
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Integer.class));
        }

        @Override
        public void processElement(Sensor sensor, Context ctx, Collector<String> out) throws Exception {
            Integer current = count.value();
            if (current == null) {
                current = 0;
                long fireAt = ctx.timerService().currentProcessingTime() + DELAY_MS;
                ctx.timerService().registerProcessingTimeTimer(fireAt);
                out.collect("key=" + ctx.getCurrentKey() + " 第一条数据到达，注册处理时间定时器 @" + fireAt);
            }
            current = current + 1;
            count.update(current);
            out.collect("key=" + ctx.getCurrentKey() + " 收到 " + sensor.id + " -> 累计=" + current);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("[处理时间定时器触发] key=" + ctx.getCurrentKey() + " timerTs=" + timestamp
                    + " timeDomain=" + ctx.timeDomain() + " 触发时累计条数=" + count.value());
            count.clear();
        }
    }

    // ==================================================================================
    // 3) keyed + 事件时间定时器
    // ==================================================================================

    /**
     * keyed 的事件时间定时器：每个 key 的第一条数据到达时，注册 {@code ts + 5s} 的事件时间定时器；
     * 定时器触发时输出该 key 在这段时间里累计的条数（相当于一个自己实现的「按 key 会话小窗口」）。
     *
     * <p>关键点：{@code registerEventTimeTimer(T)} 的定时器在 <b>watermark >= T</b> 时触发，
     * 所以没有 watermark 就永远不会触发 —— 这就是事件时间 demo 必须
     * {@code assignTimestampsAndWatermarks} 的原因。
     */
    public static class EventTimeCountWithTimer extends KeyedProcessFunction<String, Sensor, String> {

        private static final long WINDOW_MS = 5000L;

        private transient ValueState<Integer> count;

        @Override
        public void open(OpenContext openContext) throws Exception {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Integer.class));
        }

        @Override
        public void processElement(Sensor sensor, Context ctx, Collector<String> out) throws Exception {
            Integer current = count.value();
            if (current == null) {
                current = 0;
                // 每个 key 只在第一条数据上注册一次定时器；时间戳来自数据自己的 event time
                ctx.timerService().registerEventTimeTimer(sensor.ts + WINDOW_MS);
            }
            current = current + 1;
            count.update(current);

            out.collect("key=" + ctx.getCurrentKey() + " 收到 ts=" + sensor.ts + " temp=" + sensor.temp
                    + " | 累计=" + current + " | currentWatermark=" + ctx.timerService().currentWatermark());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("[事件时间定时器触发] key=" + ctx.getCurrentKey() + " timerTs=" + timestamp
                    + " timeDomain=" + ctx.timeDomain() + " 累计条数=" + count.value());
            // 定时器触发后清理状态，否则 keyed state 会一直留着
            count.clear();
        }
    }

    // ==================================================================================

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度 1：输出顺序可预期，便于对照学习（真实环境不需要为了正确性调并行度）
        env.setParallelism(1);

        demoProcessFunction(env);
        demoProcessingTimeTimer(env);
        demoEventTimeTimer(env);

        env.execute("ProcessFunctionDemo");
    }

    /** 分支 1：非 keyed ProcessFunction 的侧输出分流。 */
    private static void demoProcessFunction(StreamExecutionEnvironment env) {
        DataStream<Sensor> sensorStream = env.fromData(
                new Sensor("s1", 1000L, 21.0),
                new Sensor("s2", 2000L, 999.0),   // temp 越界 -> 异常侧输出
                new Sensor("s3", 1500L, 22.0),    // ts 回退 -> 迟到侧输出
                new Sensor("s4", 3000L, 23.0),
                new Sensor("s5", 4000L, -100.0),  // temp 越界 -> 异常侧输出
                new Sensor("s6", 5000L, 24.0));

        SingleOutputStreamOperator<String> main = sensorStream.process(new SensorSplitter());

        main.print("主输出");
        // 侧输出流：从同一个算子上按 OutputTag 取出来，可以再 sink 到别的地方（告警、死信队列…）
        main.getSideOutput(SensorSplitter.ABNORMAL).print("侧输出-异常");
        main.getSideOutput(SensorSplitter.LATE).print("侧输出-迟到");
    }

    /** 分支 2：keyed 的处理时间定时器（必须有慢速到达的流把作业撑到定时器到期）。 */
    private static void demoProcessingTimeTimer(StreamExecutionEnvironment env) {
        DataStream<Sensor> readings = env.fromData(
                        // 业务数据：3 个 key 各 3 条，同 key 相邻，便于观察「定时器触发时累计了几条」
                        new Sensor("t1", 0L, 20.0),
                        new Sensor("t1", 0L, 21.0),
                        new Sensor("t1", 0L, 22.0),
                        new Sensor("t2", 0L, 30.0),
                        new Sensor("t2", 0L, 31.0),
                        new Sensor("t2", 0L, 32.0),
                        new Sensor("t3", 0L, 40.0),
                        new Sensor("t3", 0L, 41.0),
                        new Sensor("t3", 0L, 42.0),
                        // 尾部心跳：只为把「源还活着」的时间撑过 t3 的定时器到期时刻。
                        // 有界源一旦耗尽，作业会立刻结束，之后到期的处理时间定时器根本不会触发；
                        // 真实无界流数据持续到达，不存在这个问题。
                        new Sensor("_hb", 0L, 0.0),
                        new Sensor("_hb", 0L, 0.0),
                        new Sensor("_hb", 0L, 0.0),
                        new Sensor("_hb", 0L, 0.0),
                        new Sensor("_hb", 0L, 0.0),
                        new Sensor("_hb", 0L, 0.0))
                // 100ms/条模拟「数据一条条慢慢到」，否则源瞬间跑完、定时器来不及触发
                .map(new MapFunction<Sensor, Sensor>() {
                    @Override
                    public Sensor map(Sensor value) throws Exception {
                        Thread.sleep(100L);
                        return value;
                    }
                })
                // 心跳只是撑时间的，不进业务逻辑
                .filter(sensor -> !sensor.id.startsWith("_"));

        readings.keyBy(sensor -> sensor.id)
                .process(new ProcessingTimeSummary())
                .print("处理时间定时器");
    }

    /** 分支 3：keyed 的事件时间定时器（需要 watermark）。 */
    private static void demoEventTimeTimer(StreamExecutionEnvironment env) {
        final long base = 1700000000000L; // 固定基准时间，便于人工核对窗口/定时器时间

        DataStream<Sensor> sensorStream = env.fromData(
                new Sensor("user-A", base + 0L, 20.0),
                new Sensor("user-B", base + 1000L, 30.0),
                new Sensor("user-A", base + 2000L, 21.0),
                new Sensor("user-A", base + 3000L, 22.0),
                new Sensor("user-B", base + 4000L, 31.0),
                new Sensor("user-C", base + 5000L, 40.0),
                // 末尾补一条「很晚」的数据：把 watermark 一次推到 base+60s 之后，让各 key 注册的
                // base+5s 事件时间定时器全部到期。（有界源在输入结束时也会补一条
                // Watermark(Long.MAX_VALUE)，等效于「后面还有更晚的数据」，两者都会让定时器触发。）
                new Sensor("__WATERMARK__", base + 60000L, 0.0));

        // forMonotonousTimestamps：数据时间戳已经递增（无乱序），watermark = maxTs - 1。
        // withTimestampAssigner 有两个重载，显式转型成 SerializableTimestampAssigner 避免 lambda 歧义。
        WatermarkStrategy<Sensor> watermarkStrategy = WatermarkStrategy
                .<Sensor>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Sensor>)
                        (element, recordTimestamp) -> element.ts);

        sensorStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                // 哨兵记录只负责推 watermark，不参与业务（filter 会透传 watermark，不影响下游）
                .filter(sensor -> !"__WATERMARK__".equals(sensor.id))
                .keyBy(sensor -> sensor.id)
                .process(new EventTimeCountWithTimer())
                .print("事件时间定时器");
    }
}
