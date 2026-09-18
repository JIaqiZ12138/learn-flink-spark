package com.jiaqiz.flink.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Window Join（窗口 join）演示：{@code JoinedStreams} + {@code JoinFunction} / {@code FlatJoinFunction}。
 *
 * <p>演示内容：
 *
 * <ol>
 *   <li>{@code stream1.join(stream2).where(...).equalTo(...).window(TumblingEventTimeWindows.of(...))
 *       .apply(JoinFunction)}：<b>每个匹配对</b>调用一次 {@code join(l, r)}，返回恰好 1 条结果；
 *   <li>{@code apply(FlatJoinFunction)}：每个匹配对可以输出 <b>0..n</b> 条（既能一对多展开，也能过滤掉），
 *       演示里还顺带用了带 {@code TypeInformation} 的重载 {@code apply(fn, Types.STRING)}；
 *   <li>数据里同时放了"能匹配"和"匹配不上"的 key，用于说明<b>窗口 join 是 inner join</b>：
 *       只有左右两侧在<b>同一个窗口</b>内都出现过该 key，才会产生输出；
 *       单边有数据的 key 直接消失（对比 {@link CoGroupDemo} 的 coGroup，它能把单边数据也输出）。
 * </ol>
 *
 * <p><b>为什么匹配不上的不会出现在结果里：</b>
 *
 * <ul>
 *   <li>窗口 join 的底层实现是「两条流先按 key 做 union（{@code CoGroupedStreams.TaggedUnion}）
 *       → 进同一个事件时间窗口 → 窗口触发时把左右两侧的元素集合拿出来做嵌套循环」；
 *   <li>只要任意一侧在窗口内没有该 key 的元素，嵌套循环里就凑不出 (left, right) 对，
 *       {@code JoinFunction} 根本不会被调用，自然也就没有输出；
 *   <li>换句话说：<b>匹配不上的数据不是被"过滤"掉了，而是压根没进入 join 的两两配对阶段</b>。
 *       要保留单边数据请用 {@code coGroup}（见 {@code CoGroupDemo}），或用 outer join 的 SQL 写法。
 *   <li>另一层含义：join 只输出"窗口内"的匹配。元素放在不同窗口（比如差 6 秒）即使 key 相同也不会匹配，
 *       这就是窗口 join 相对 interval join / regular join 的语义限制。
 * </ul>
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.join.WindowJoinDemo
 * </pre>
 */
public class WindowJoinDemo {

    /** 窗口大小：5 秒的事件时间滚动窗口（epoch 对齐，即 [0,5000)、[5000,10000) ...）。 */
    private static final long WINDOW_SIZE_MS = 5_000L;

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度 1：窗口内配对结果的输出顺序与 hash 有关，单并发更容易对照预期。
        env.setParallelism(1);

        System.out.println("======== Window Join 演示 ========");
        System.out.println("窗口: TumblingEventTimeWindows.of(Time.seconds(5)) → [0,5000) 与 [5000,10000)");
        System.out.println("用户流: id=1(ts=1000), id=2(ts=1500), id=3(ts=2600,无订单), id=1(ts=3000,资料更新), id=4(ts=6500)");
        System.out.println("订单流: id=1(ts=1200), id=2(ts=1800), id=1(ts=3000), id=9(ts=4200,无用户), id=4(ts=6500), id=9(ts=8800,无用户)");
        System.out.println("预期: JoinFunction 共 6 对（每对 1 条）；FlatJoinFunction 共 5 条（金额<50 的 2 对被过滤，金额>=200 的那对多输出 1 条风控）；");
        System.out.println("      id=3(用户无订单) 与 id=9(订单无用户) 永远不出现 → 窗口 join 是 inner join。");

        // ---------------- 两条流：事件时间 + watermark ----------------
        // 有界数据也必须显式赋事件时间，否则窗口永远不会按事件时间触发。
        // forBoundedOutOfOrderness(1s)：容忍 1 秒乱序，watermark = 最大事件时间 - 1s。
        // 输入本身已按时间戳升序，保证不会有元素被判为迟到而丢弃。
        final DataStream<UserEvent> userStream =
                env.fromData(
                                new UserEvent(1000L, 1, "用户1-v1"),
                                new UserEvent(1500L, 2, "用户2"),
                                new UserEvent(2600L, 3, "用户3(无订单)"),
                                new UserEvent(3000L, 1, "用户1-v2"),
                                new UserEvent(6500L, 4, "用户4"))
                        .assignTimestampsAndWatermarks(watermarkForUsers());

        final DataStream<OrderEvent> orderStream =
                env.fromData(
                                new OrderEvent(1200L, 1, "O-1001", 99.5),
                                new OrderEvent(1800L, 2, "O-1002", 200.0),
                                new OrderEvent(3000L, 1, "O-1003", 30.0),
                                new OrderEvent(4200L, 9, "O-1004(无用户)", 500.0),
                                new OrderEvent(6500L, 4, "O-1005", 66.6),
                                new OrderEvent(8800L, 9, "O-1006(无用户)", 7.0))
                        .assignTimestampsAndWatermarks(watermarkForOrders());

        // ---------------- 1) JoinFunction：一个匹配对 → 恰好一条输出 ----------------
        final DataStream<String> joined =
                userStream
                        .join(orderStream)
                        // where/equalTo 必须给出同一个 KEY 类型（这里都是 Integer userId）。
                        // 用匿名类而不是 lambda：泛型 KEY 显式写出来，避免类型推断问题。
                        .where(
                                new KeySelector<UserEvent, Integer>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Integer getKey(UserEvent user) {
                                        return user.userId;
                                    }
                                })
                        .equalTo(
                                new KeySelector<OrderEvent, Integer>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Integer getKey(OrderEvent order) {
                                        return order.userId;
                                    }
                                })
                        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                        .apply(
                                new JoinFunction<UserEvent, OrderEvent, String>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public String join(UserEvent user, OrderEvent order) {
                                        return String.format(
                                                "[JoinFunction] %s 订单%s(金额%.1f, ts=%d) × %s",
                                                windowOf(order.ts),
                                                order.orderId,
                                                order.amount,
                                                order.ts,
                                                user.name);
                                    }
                                });
        joined.print();

        // ---------------- 2) FlatJoinFunction：一个匹配对 → 0..n 条输出 ----------------
        final DataStream<String> flatJoined =
                userStream
                        .join(orderStream)
                        .where(
                                new KeySelector<UserEvent, Integer>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Integer getKey(UserEvent user) {
                                        return user.userId;
                                    }
                                })
                        .equalTo(
                                new KeySelector<OrderEvent, Integer>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Integer getKey(OrderEvent order) {
                                        return order.userId;
                                    }
                                })
                        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                        // 带 TypeInformation 的重载：泛型擦除后 Flink 拿不到返回值类型时就得显式给
                        // （等价写法是 .returns(Types.STRING)）。这里显式传，演示两种写法。
                        .apply(
                                new FlatJoinFunction<UserEvent, OrderEvent, String>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void join(
                                            UserEvent user, OrderEvent order, Collector<String> out) {

                                        // (a) 0 条：小额订单不参与对账 → 这一对直接不出现在结果里
                                        if (order.amount < 50.0) {
                                            return;
                                        }
                                        // (b) 1 条：正常明细
                                        out.collect(
                                                String.format(
                                                        "[FlatJoin-明细] %s 订单%s(金额%.1f) ← %s",
                                                        windowOf(order.ts),
                                                        order.orderId,
                                                        order.amount,
                                                        user.name));
                                        // (c) 同一个匹配对再输出第 2 条：一对多输出
                                        if (order.amount >= 200.0) {
                                            out.collect(
                                                    String.format(
                                                            "[FlatJoin-风控] 订单%s 金额%.1f 超过 200，人工复核（%s）",
                                                            order.orderId, order.amount, user.name));
                                        }
                                    }
                                },
                                Types.STRING);
        flatJoined.print();

        env.execute("window-join-demo");
    }

    // ------------------------------------------------------------------------
    //  watermark
    // ------------------------------------------------------------------------

    private static WatermarkStrategy<UserEvent> watermarkForUsers() {
        return WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((user, recordTimestamp) -> user.ts);
    }

    private static WatermarkStrategy<OrderEvent> watermarkForOrders() {
        return WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((order, recordTimestamp) -> order.ts);
    }

    /** 打印元素所在的事件时间窗口，方便肉眼确认"同一个窗口内才会配对"。 */
    private static String windowOf(long ts) {
        final long start = ts / WINDOW_SIZE_MS * WINDOW_SIZE_MS;
        return "win[" + start + "," + (start + WINDOW_SIZE_MS) + ")";
    }

    // ------------------------------------------------------------------------
    //  数据模型（必须是标准 POJO：public 类 + public 字段 + 无参构造）
    // ------------------------------------------------------------------------

    /** 用户资料事件：同一个 userId 在一个窗口内可能出现多条（资料变更）。 */
    public static class UserEvent {

        public long ts;
        public int userId;
        public String name;

        public UserEvent() {}

        public UserEvent(long ts, int userId, String name) {
            this.ts = ts;
            this.userId = userId;
            this.name = name;
        }

        @Override
        public String toString() {
            return String.format("User{ts=%d, userId=%d, name=%s}", ts, userId, name);
        }
    }

    /** 订单事件。 */
    public static class OrderEvent {

        public long ts;
        public int userId;
        public String orderId;
        public double amount;

        public OrderEvent() {}

        public OrderEvent(long ts, int userId, String orderId, double amount) {
            this.ts = ts;
            this.userId = userId;
            this.orderId = orderId;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return String.format(
                    "Order{ts=%d, orderId=%s, userId=%d, amount=%.1f}",
                    ts, orderId, userId, amount);
        }
    }
}
