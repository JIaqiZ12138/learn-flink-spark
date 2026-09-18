package com.jiaqiz.flink.process;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * CoProcessFunction / KeyedCoProcessFunction 演示：两条流按 key 关联（订单 & 支付对账）。
 *
 * <p>演示内容：
 *
 * <ol>
 *   <li>{@code orders.connect(payments).keyBy(orderKey, payKey)} 之后用
 *       {@link KeyedCoProcessFunction}：一侧先到就把它存进 {@code ValueState}，另一侧到达时读出来做关联输出。
 *   <li>用 <b>事件时间定时器</b> 做超时清理：订单先到时注册 {@code orderTs + 10s} 的定时器，
 *       支付一直没来就在 {@code onTimer} 里把这条「悬挂订单」清掉并报警；支付先到同理注册超时清理。
 *   <li>事件时间定时器要求 <b>两条流都</b> assignTimestampsAndWatermarks：两输入算子的 watermark 是
 *       各输入 watermark 的最小值，只有一条流推进 watermark 是推不动的。
 * </ol>
 *
 * <p>运行：
 *
 * <pre>
 * mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.process.CoProcessFunctionDemo
 * </pre>
 */
public class CoProcessFunctionDemo {

    /** 订单。POJO：public 字段 + public 无参构造。 */
    public static class Order {
        public String orderId;
        public String userId;
        public double amount;
        public long ts;

        public Order() {}

        public Order(String orderId, String userId, double amount, long ts) {
            this.orderId = orderId;
            this.userId = userId;
            this.amount = amount;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "Order{" + orderId + ", user=" + userId + ", amount=" + amount + ", ts=" + ts + "}";
        }
    }

    /** 支付流水，用 orderId 与订单关联。 */
    public static class Payment {
        public String orderId;
        public double amount;
        public long ts;

        public Payment() {}

        public Payment(String orderId, double amount, long ts) {
            this.orderId = orderId;
            this.amount = amount;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "Payment{" + orderId + ", amount=" + amount + ", ts=" + ts + "}";
        }
    }

    /** 只用来推 watermark 的哨兵 id：它只出现在流里，不会进入业务状态。 */
    private static final String WATERMARK_SENTINEL = "__WATERMARK__";

    /**
     * 订单流 vs 支付流的按 key 关联 + 超时清理。
     *
     * <p>为什么用 {@link KeyedCoProcessFunction} 而不是 {@code CoProcessFunction}：
     * keyed 版本才有 {@code getCurrentKey()} 和 keyed state（{@code ValueState}），
     * 而且定时器只能注册在 keyed 流上（非 keyed 的 CoProcessFunction 会抛
     * UnsupportedOperationException）。所以 connect 之后必须先 {@code keyBy}。
     */
    public static class OrderPaymentJoiner
            extends KeyedCoProcessFunction<String, Order, Payment, String> {

        /** 订单等支付的超时时间。 */
        private static final long PAY_TIMEOUT_MS = 10_000L;

        /** 支付等订单的超时时间。 */
        private static final long ORDER_TIMEOUT_MS = 15_000L;

        private transient ValueState<Order> pendingOrder;
        private transient ValueState<Payment> pendingPayment;

        @Override
        public void open(OpenContext openContext) throws Exception {
            // 两侧各一个 ValueState：同一 key 下最多缓存一条未匹配记录
            pendingOrder = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pending-order", Order.class));
            pendingPayment = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pending-payment", Payment.class));
        }

        @Override
        public void processElement1(Order order, Context ctx, Collector<String> out) throws Exception {
            Payment payment = pendingPayment.value();
            if (payment != null) {
                // 支付先到了：直接关联，清掉缓存和支付侧的超时定时器
                out.collect("[关联成功·支付先到] key=" + ctx.getCurrentKey() + " 订单=" + order.orderId
                        + " 订单金额=" + order.amount + " 支付金额=" + payment.amount);
                pendingPayment.clear();
                ctx.timerService().deleteEventTimeTimer(payment.ts + ORDER_TIMEOUT_MS);
                return;
            }

            // 订单先到：暂存 + 注册「等支付超时」的事件时间定时器
            pendingOrder.update(order);
            long fireAt = order.ts + PAY_TIMEOUT_MS;
            ctx.timerService().registerEventTimeTimer(fireAt);
            out.collect("[订单暂存] key=" + ctx.getCurrentKey() + " 订单=" + order.orderId
                    + " 存入 ValueState，注册超时清理定时器 @" + fireAt);
        }

        @Override
        public void processElement2(Payment payment, Context ctx, Collector<String> out) throws Exception {
            Order order = pendingOrder.value();
            if (order != null) {
                out.collect("[关联成功·订单先到] key=" + ctx.getCurrentKey() + " 订单=" + payment.orderId
                        + " 订单金额=" + order.amount + " 支付金额=" + payment.amount);
                pendingOrder.clear();
                // 已经匹配上，取消原来的超时清理定时器，否则到期会重复输出一条「超时」
                ctx.timerService().deleteEventTimeTimer(order.ts + PAY_TIMEOUT_MS);
                return;
            }

            pendingPayment.update(payment);
            long fireAt = payment.ts + ORDER_TIMEOUT_MS;
            ctx.timerService().registerEventTimeTimer(fireAt);
            out.collect("[支付暂存] key=" + ctx.getCurrentKey() + " 支付=" + payment.orderId
                    + " 存入 ValueState 等订单，注册超时清理定时器 @" + fireAt);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 同一个 onTimer 要区分是哪一侧的超时：看哪一侧还有缓存
            Order order = pendingOrder.value();
            Payment payment = pendingPayment.value();

            if (order != null) {
                out.collect("[超时清理·订单未支付] key=" + ctx.getCurrentKey() + " 订单=" + order.orderId
                        + " timerTs=" + timestamp + " timeDomain=" + ctx.timeDomain()
                        + " 超过 " + (timestamp - order.ts) + "ms 未收到支付，清空 ValueState");
                pendingOrder.clear();
            } else if (payment != null) {
                out.collect("[超时清理·支付无订单] key=" + ctx.getCurrentKey() + " 支付=" + payment.orderId
                        + " timerTs=" + timestamp + " timeDomain=" + ctx.timeDomain()
                        + " 超过 " + (timestamp - payment.ts) + "ms 未收到订单，清空 ValueState");
                pendingPayment.clear();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final long base = 1700000000000L;

        DataStream<Order> orders = env.fromData(
                        new Order("O1", "u1", 100.0, base + 0L),
                        new Order("O2", "u1", 200.0, base + 1000L),
                        new Order("O3", "u2", 300.0, base + 5000L),
                        new Order("O4", "u2", 400.0, base + 6000L),
                        // 哨兵：把订单流的 watermark 推到 base+60s，让两侧定时器都能到期
                        new Order(WATERMARK_SENTINEL, "-", 0.0, base + 60000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Order>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Order>)
                                (element, recordTimestamp) -> element.ts))
                .filter(order -> !WATERMARK_SENTINEL.equals(order.orderId));

        DataStream<Payment> payments = env.fromData(
                        new Payment("O1", 100.0, base + 2000L), // 订单 O1 之后到 -> 正常匹配
                        new Payment("O3", 300.0, base + 3000L), // 订单 O3 之前到 -> 支付先到，暂存
                        new Payment("O9", 50.0, base + 4000L),  // 压根没有订单 -> 超时清理
                        new Payment(WATERMARK_SENTINEL, 0.0, base + 60000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Payment>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Payment>)
                                (element, recordTimestamp) -> element.ts))
                .filter(payment -> !WATERMARK_SENTINEL.equals(payment.orderId));

        // connect 之后必须 keyBy（两侧 key 语义要一致：都是 orderId），
        // 否则只能用非 keyed 的 CoProcessFunction，既拿不到 key 也不能注册定时器
        orders.connect(payments)
                .keyBy(order -> order.orderId, payment -> payment.orderId)
                .process(new OrderPaymentJoiner())
                .print("订单支付关联");

        env.execute("CoProcessFunctionDemo");
    }
}
