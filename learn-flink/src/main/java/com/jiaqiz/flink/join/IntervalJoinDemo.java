package com.jiaqiz.flink.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Interval Join（区间 join）演示：
 * {@code keyedStream1.intervalJoin(keyedStream2).between(Time.seconds(-2), Time.seconds(2)).process(...)}。
 *
 * <p>演示内容：
 *
 * <ol>
 *   <li>下单流 (左) 与支付流 (右) 按 订单号 做区间 join，匹配条件是
 *       {@code 左.ts + lowerBound <= 右.ts <= 左.ts + upperBound}（默认<b>两端都是闭区间</b>）；
 *   <li>刻意构造"区间内"与"区间外"的数据：区间内 3 对匹配，区间外 3 条被丢弃；
 *   <li>同一份数据再用 {@code between(-2s, +5s)} 跑一遍作对照，说明"丢弃"是<b>区间语义</b>决定的
 *       （放宽容忍度后其中 2 条又能匹配上），以及唯一那条仍然匹配不上的原因是
 *       <b>interval join 也是 inner join</b>（左流没有 key=E 的记录）；
 *   <li>{@code ProcessJoinFunction} 里通过 {@code ctx.getLeftTimestamp()/getRightTimestamp()}
 *       拿到两侧的事件时间，把"间隔多少毫秒"打进结果，肉眼验证区间判定。
 * </ol>
 *
 * <p><b>用什么时间、状态怎么管：</b>
 *
 * <ul>
 *   <li>时间语义：<b>只支持事件时间</b>（{@code IntervalJoin} 默认 {@code inEventTime()}；
 *       声明了 {@code inProcessingTime()} 之后再调 {@code between} 会直接抛
 *       {@code UnsupportedTimeCharacteristicException}）。所以两条流都必须先
 *       {@code assignTimestampsAndWatermarks}，否则没有事件时间可用。
 *   <li>必须是 keyed stream：{@code intervalJoin(KeyedStream<T1, KEY>)}，两侧 key 类型相同，
 *       只有同 key 的元素才可能配对（底层按 key 分到同一个子任务）。
 *   <li>实现是 {@code IntervalJoinOperator}：内部用两个
 *       {@code MapState<Long, List<BufferEntry<T>>>}（leftBuffer / rightBuffer）把两侧数据按
 *       <b>事件时间戳 → 元素列表</b> 缓存起来（keyed state，随 key 一起 checkpoint）；
 *       每来一个元素就为它注册一个事件时间<b>定时器</b>（清理时间 = 该元素时间戳 ± 区间边界），
 *       watermark 推进到定时器时间时触发 {@code onEventTime}，把过期元素从 state 里删掉。
 *   <li>因此 interval join 的状态量 ≈「区间长度内、各 key 的元素条数」，是有界的；
 *       而 regular join（普通双流 join）的状态会无限增长——这是选型时最关键的差别。
 *   <li>左元素如果在区间内找不到任何右元素，就<b>什么都不输出</b>（inner join），
 *       并且这个左元素最终也会在定时器触发时被清理掉（"超出区间的数据被丢弃"）。
 *       如果不想静默丢弃，可以用 {@code IntervalJoined.sideOutputLeftLateData(OutputTag)} /
 *       {@code sideOutputRightLateData(OutputTag)} 把"来晚了、已经被清理"的元素接到侧输出流。
 *   <li>边界开闭：默认两端闭区间，想改成开区间用 {@code upperBoundExclusive()} /
 *       {@code lowerBoundExclusive()}。
 * </ul>
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.join.IntervalJoinDemo
 * </pre>
 */
public class IntervalJoinDemo {

    /**
     * 第一轮：between(-2s, +2s)。
     *
     * <p>预期匹配 3 对；被丢弃 3 条（A 的 +4000ms、B 的 +3000ms 超上界，E 是左流没有对应 key）。
     */
    public static void main(String[] args) throws Exception {
        runJob(-2L, 2L, "interval-join-[-2s,+2s]");
        runJob(-2L, 5L, "interval-join-[-2s,+5s]");
    }

    private static void runJob(long lowerBoundSeconds, long upperBoundSeconds, String jobName)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        System.out.println("\n======== Interval Join 演示: between(" + lowerBoundSeconds + "s, +"
                + upperBoundSeconds + "s) ========");
        System.out.println("下单流(左, key=订单号): A@1000ms, B@5000ms, C@9000ms, D@13000ms");
        System.out.println("支付流(右, key=订单号): A@2000(+1000), A@5000(+4000), B@3000(-2000), B@8000(+3000), D@12500(-500), E@20000(左流无E)");

        final KeyedStream<OrderEvent, String> orders =
                env.fromData(
                                new OrderEvent(1000L, "A", 100.0),
                                new OrderEvent(5000L, "B", 200.0),
                                new OrderEvent(9000L, "C", 300.0),
                                new OrderEvent(13000L, "D", 400.0))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(2))
                                        .withTimestampAssigner((order, recordTimestamp) -> order.ts))
                        .keyBy(
                                new KeySelector<OrderEvent, String>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public String getKey(OrderEvent order) {
                                        return order.orderId;
                                    }
                                });

        final KeyedStream<PayEvent, String> pays =
                env.fromData(
                                new PayEvent(2000L, "A", "ALIPAY"),
                                new PayEvent(3000L, "B", "ALIPAY"),
                                new PayEvent(5000L, "A", "WECHAT"),
                                new PayEvent(8000L, "B", "WECHAT"),
                                new PayEvent(12500L, "D", "WECHAT"),
                                new PayEvent(20000L, "E", "ALIPAY"))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<PayEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(2))
                                        .withTimestampAssigner((pay, recordTimestamp) -> pay.ts))
                        .keyBy(
                                new KeySelector<PayEvent, String>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public String getKey(PayEvent pay) {
                                        return pay.orderId;
                                    }
                                });

        // 语义：左.ts + lowerBound <= 右.ts <= 左.ts + upperBound（闭区间）
        // Time.seconds(-2) 表示允许"支付早于下单 2 秒"（比如回调乱序、时钟偏差）。
        orders.intervalJoin(pays)
                .between(Time.seconds(lowerBoundSeconds), Time.seconds(upperBoundSeconds))
                .process(
                        new ProcessJoinFunction<OrderEvent, PayEvent, String>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void processElement(
                                    OrderEvent order,
                                    PayEvent pay,
                                    Context ctx,
                                    Collector<String> out) {

                                // ctx 里的时间戳就是算子用于区间判定的"事件时间"，用它可以肉眼核对区间。
                                final long diff = ctx.getRightTimestamp() - ctx.getLeftTimestamp();
                                out.collect(
                                        String.format(
                                                "[匹配] 订单%s(金额%.1f, ts=%d) ← 支付渠道%s(ts=%d)，右-左 = %+dms ∈ [%+dms, %+dms]",
                                                order.orderId,
                                                order.amount,
                                                ctx.getLeftTimestamp(),
                                                pay.channel,
                                                ctx.getRightTimestamp(),
                                                diff,
                                                lowerBoundSeconds * 1000L,
                                                upperBoundSeconds * 1000L));
                            }
                        })
                .print();

        env.execute(jobName);
    }

    // ------------------------------------------------------------------------
    //  数据模型（标准 POJO）
    // ------------------------------------------------------------------------

    /** 下单事件（左流）。 */
    public static class OrderEvent {

        public long ts;
        public String orderId;
        public double amount;

        public OrderEvent() {}

        public OrderEvent(long ts, String orderId, double amount) {
            this.ts = ts;
            this.orderId = orderId;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return String.format("Order{ts=%d, orderId=%s, amount=%.1f}", ts, orderId, amount);
        }
    }

    /** 支付事件（右流）。 */
    public static class PayEvent {

        public long ts;
        public String orderId;
        public String channel;

        public PayEvent() {}

        public PayEvent(long ts, String orderId, String channel) {
            this.ts = ts;
            this.orderId = orderId;
            this.channel = channel;
        }

        @Override
        public String toString() {
            return String.format("Pay{ts=%d, orderId=%s, channel=%s}", ts, orderId, channel);
        }
    }
}
