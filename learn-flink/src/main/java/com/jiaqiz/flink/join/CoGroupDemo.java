package com.jiaqiz.flink.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Window CoGroup 演示：{@code stream1.coGroup(stream2).where(...).equalTo(...).window(...).apply(...)}。
 *
 * <p>演示内容：
 *
 * <ol>
 *   <li>同一个「窗口 + key」只调用一次 {@code coGroup(Iterable left, Iterable right, Collector)}，
 *       两个 Iterable 是该 key 在该窗口内的<b>全部</b>元素；
 *   <li>重点演示 {@code coGroup} 能输出「只有单边有数据」的组合：<b>仅左</b>（用户没下单）、
 *       <b>仅右</b>（订单找不到用户），这是 {@code join} 做不到的；
 *   <li>与 {@link WindowJoinDemo} 的 join 结果对照：coGroup 的输出条数 = 出现过的 (key, window) 组合数，
 *       而 join 的输出条数 = 匹配对数量。
 * </ol>
 *
 * <p><b>coGroup 与 join 的语义差别：</b>
 *
 * <ul>
 *   <li>{@code join} = <b>inner join</b>：只有左右两侧在同一窗口内都有该 key 时才配对，
 *       并且对每一对 (left, right) 都调用一次 {@code JoinFunction}；
 *       实现上 Flink 的 {@code WindowJoin} 就是在 {@code CoGroupedStreams} 之上加了「两侧都非空」的判断。
 *   <li>{@code coGroup} = 更底层的<b>分组协作</b>原语：它不预设连接语义，两侧 Iterable 都可以为空，
 *       由用户决定输出什么，因此能表达 inner / left / right / full outer join，甚至完全自定义逻辑。
 *   <li>代价：coGroup 每个 (key, window) 都要在任务里把两侧数据过一遍内存/状态，且两个 Iterable 都可能是
 *       大集合（数据倾斜时尤其明显）；只想要 inner join 就别用 coGroup，语义更啰嗦、开销也更大。
 * </ul>
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.join.CoGroupDemo
 * </pre>
 */
public class CoGroupDemo {

    private static final long WINDOW_SIZE_MS = 5_000L;

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        System.out.println("======== CoGroup 演示 ========");
        System.out.println("窗口: TumblingEventTimeWindows.of(Time.seconds(5))");
        System.out.println("用户流: id=1(ts=1000), id=2(ts=1500), id=3(ts=2600,无订单), id=4(ts=6500)");
        System.out.println("订单流: id=1(ts=1200), id=2(ts=1800), id=1(ts=3000), id=9(ts=4200,无用户), id=4(ts=6500), id=9(ts=8800,无用户)");
        System.out.println("预期: 双侧匹配 3 组(key 1/2/4)；仅左 1 组(key 3)；仅右 2 组(key 9 在两个窗口各一次)。");

        final DataStream<UserEvent> userStream =
                env.fromData(
                                new UserEvent(1000L, 1, "用户1"),
                                new UserEvent(1500L, 2, "用户2"),
                                new UserEvent(2600L, 3, "用户3(无订单)"),
                                new UserEvent(6500L, 4, "用户4"))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(1))
                                        .withTimestampAssigner((user, recordTimestamp) -> user.ts));

        final DataStream<OrderEvent> orderStream =
                env.fromData(
                                new OrderEvent(1200L, 1, "O-1001", 99.5),
                                new OrderEvent(1800L, 2, "O-1002", 200.0),
                                new OrderEvent(3000L, 1, "O-1003", 30.0),
                                new OrderEvent(4200L, 9, "O-1004(无用户)", 500.0),
                                new OrderEvent(6500L, 4, "O-1005", 66.6),
                                new OrderEvent(8800L, 9, "O-1006(无用户)", 7.0))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(1))
                                        .withTimestampAssigner((order, recordTimestamp) -> order.ts));

        final DataStream<String> coGrouped =
                userStream
                        .coGroup(orderStream)
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
                                new CoGroupFunction<UserEvent, OrderEvent, String>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void coGroup(
                                            Iterable<UserEvent> users,
                                            Iterable<OrderEvent> orders,
                                            Collector<String> out) {

                                        // 两侧 Iterable 由 Flink 从窗口状态里迭代给出：
                                        // 只保证「遍历一次」有效，元素对象在开启对象重用(enableObjectReuse)时还会被复用，
                                        // 想多次遍历或长期持有，必须先自己拷进 List。
                                        final List<UserEvent> userList = toList(users);
                                        final List<OrderEvent> orderList = toList(orders);

                                        if (userList.isEmpty()) {
                                            // 只有右表有数据（比如"野"订单：userId 在用户表里查不到）。
                                            // join 在这里什么都不会输出，coGroup 可以照常处理。
                                            final long start = windowStart(orderList.get(0).ts);
                                            out.collect(
                                                    String.format(
                                                            "[仅右/右外] %s 没有用户记录的订单 %d 条，订单号=%s",
                                                            window(start),
                                                            orderList.size(),
                                                            orderIds(orderList)));
                                            return;
                                        }
                                        if (orderList.isEmpty()) {
                                            // 只有左表有数据（比如"沉默用户"：窗口内没下过单）。
                                            final long start = windowStart(userList.get(0).ts);
                                            out.collect(
                                                    String.format(
                                                            "[仅左/左外] %s %s(userId=%d) 窗口内没有订单",
                                                            window(start),
                                                            userList.get(0).name,
                                                            userList.get(0).userId));                                            return;
                                        }

                                        // 两侧都有：这里等价于 inner join，但输出粒度是"每个 (key, window) 一条"，
                                        // 而不是"每个匹配对一条"（对比 WindowJoinDemo 里 6 对 6 条输出）。
                                        final long start = windowStart(userList.get(0).ts);
                                        double total = 0.0;
                                        for (OrderEvent order : orderList) {
                                            total += order.amount;
                                        }
                                        out.collect(
                                                String.format(
                                                        "[双侧  ] %s key=%d 左%d条 × 右%d条，订单=%s，合计金额=%.1f → coGroup 里可以自己实现 left/right/full outer",
                                                        window(start),
                                                        userList.get(0).userId,
                                                        userList.size(),
                                                        orderList.size(),
                                                        orderIds(orderList),
                                                        total));
                                    }
                                });

        coGrouped.print();
        env.execute("cogroup-demo");
    }

    // ------------------------------------------------------------------------
    //  工具方法
    // ------------------------------------------------------------------------

    /**
     * 把 Iterable 拷进 List（迭代期间完成遍历）。
     *
     * <p>Flink 传入的 Iterable 是状态/缓存的视图，不能假定可以重复遍历，所以这里先落地。
     */
    private static <T> List<T> toList(Iterable<T> iterable) {
        final List<T> list = new ArrayList<>();
        for (T element : iterable) {
            list.add(element);
        }
        return list;
    }

    private static long windowStart(long ts) {
        return ts / WINDOW_SIZE_MS * WINDOW_SIZE_MS;
    }

    private static String window(long start) {
        return "win[" + start + "," + (start + WINDOW_SIZE_MS) + ")";
    }

    private static String orderIds(List<OrderEvent> orders) {
        final StringBuilder sb = new StringBuilder();
        for (OrderEvent order : orders) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(order.orderId);
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------------
    //  数据模型（标准 POJO）
    // ------------------------------------------------------------------------

    /** 用户资料事件。 */
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
