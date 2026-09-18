package com.jiaqiz.flink.process;

import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 广播状态（Broadcast State）演示：用一条「维表/规则」流去补全另一条「业务」流。
 *
 * <p>本 demo 覆盖：
 *
 * <ol>
 *   <li>非 keyed 分支：{@code events.connect(broadcastStream).process(new BroadcastProcessFunction<...>())}
 *       → 算子 {@code CoBroadcastWithNonKeyedOperator}
 *   <li>keyed 分支：{@code events.keyBy(...).connect(broadcastStream).process(new KeyedBroadcastProcessFunction<...>())}
 *       → 算子 {@code CoBroadcastWithKeyedOperator}
 *   <li>主流侧 {@code ctx.getBroadcastState(descriptor).get(key)} 做维表补全（只读）；
 *       广播侧 {@code ctx.getBroadcastState(descriptor).put(k, v)} 写状态
 *   <li>两条流都必须是有界的（{@code env.fromData(...)}），作业跑完自动退出
 * </ol>
 *
 * <p>运行命令：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.process.BroadcastProcessFunctionDemo
 * </pre>
 *
 * <p>涉及的运行时算子（源码 {@code flink-1.20-source/flink-streaming-java/}）：
 *
 * <pre>
 *   BroadcastProcessFunction      → CoBroadcastWithNonKeyedOperator   api/operators/co/CoBroadcastWithNonKeyedOperator.java#L54
 *   KeyedBroadcastProcessFunction → CoBroadcastWithKeyedOperator      api/operators/co/CoBroadcastWithKeyedOperator.java#L64
 * </pre>
 *
 * <p><b>广播状态的六条硬约束（都是踩过的坑）</b>：
 *
 * <ol>
 *   <li><b>只能在 {@code processBroadcastElement} 里写</b>。{@code processElement} 拿到的上下文是
 *       {@code ReadOnlyContext}，它的 {@code getBroadcastState(...)} 返回
 *       {@code ReadOnlyBroadcastState}（只有 get / contains / immutableEntries），<b>编译期</b>就写不了 ——
 *       权限由类型系统固定，不用靠自觉。写错侧 = 编译失败。
 *   <li><b>广播状态没有 key</b>，它的 namespace 是「整张算子的状态」，与 keyBy 的 key 无关。
 *       所以 {@code KeyedBroadcastProcessFunction.processBroadcastElement} 的上下文里<b>没有</b>
 *       {@code getCurrentKey()}，也不能直接读写 keyed state；想批量改 keyed state 只能用
 *       {@code ctx.applyToKeyedState(descriptor, KeyedStateFunction)}（KeyedBroadcastProcessFunction.java#L147）。
 *   <li><b>每个并行子任务都会收到全量广播数据</b>（{@code DataStream#broadcast(descriptors)} 底层是
 *       {@code BroadcastPartitioner}，见 DataStream.java#L437）。因此广播状态在<b>每个 subtask 里各存一份副本</b>，
 *       用户必须保证「同样的元素发给了所有实例」，各副本内容才能一致。
 *       本 demo 的并行度是 1（为了输出顺序稳定），所以只有一个 subtask；
 *       把并行度调成 N 的话，你会看到每个 subtask 的广播状态里都有同样的规则。
 *   <li>广播状态存在 <b>operator state</b> 里（非 keyed 版见
 *       {@code CoBroadcastWithNonKeyedOperator#open}: {@code getOperatorStateBackend().getBroadcastState(descriptor)}），
 *       扩缩容时按 key group 轮转重新分配。
 *   <li>流类型必须配对：{@code BroadcastProcessFunction} 只能配非 keyed 流，{@code KeyedBroadcastProcessFunction}
 *       只能配 keyed 流，配错在 {@code BroadcastConnectedStream#process} 里当场抛
 *       {@code "A BroadcastProcessFunction can only be used on a non-keyed stream."}。
 *   <li>广播状态适合「少变、全量可见」的数据（维表、规则、阈值配置）；不适合频繁变更的大流量数据 ——
 *       每条广播记录都会复制到所有 subtask。
 * </ol>
 *
 * <p><b>关于本 demo 里的 {@link DelayMapper}</b>：广播流和主流是两个独立的源、两个独立的线程，
 * 谁先被算子消费没有 API 层面的保证。为了演示「维表先到、业务数据后到」的正常顺序，
 * 这里给主流加了固定延时（真实场景里广播维表是提前灌好的，业务流是持续不断的）。
 * keyed 分支还故意让第二条规则（更新）晚 1s 到达，用来看「运行中更新维表」的效果。
 */
public class BroadcastProcessFunctionDemo {

    /** 固定基准时间。 */
    private static final long BASE = 1700000000000L;

    /** 业务流元素：一条订单/事件，需要根据 userId 去维表里查名字。 */
    public static class Event {
        public String userId;
        public long ts;
        public double amount;

        public Event() {}

        public Event(String userId, long ts, double amount) {
            this.userId = userId;
            this.ts = ts;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Event{" + userId + ", " + (ts - BASE) + "ms, " + amount + "}";
        }
    }

    /** 广播流元素：维表的一条 upsert（key → value）。 */
    public static class Rule {
        public String key;
        public String value;

        public Rule() {}

        public Rule(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }

    /**
     * 广播状态描述符。{@code .broadcast(descriptor)} 会用它创建 / 恢复广播状态，
     * 主流与广播侧必须用<b>同一个</b>描述符实例去取状态。
     *
     * <p>非 keyed 与 keyed 两个分支各用一个名字不同的描述符，避免混淆。
     */
    public static final MapStateDescriptor<String, String> DIM_NON_KEYED =
            new MapStateDescriptor<>("dim-table-nonkeyed", String.class, String.class);

    /** keyed 分支用的广播状态描述符。 */
    public static final MapStateDescriptor<String, String> DIM_KEYED =
            new MapStateDescriptor<>("dim-table-keyed", String.class, String.class);

    /** 固定延时器：第一条延时 firstDelayMs，其余每条延时 delayMs，用来编排两条流的先后顺序。 */
    public static class DelayMapper<T> implements MapFunction<T, T> {

        private final long firstDelayMs;
        private final long delayMs;
        private boolean firstRecord = true;

        public DelayMapper(long firstDelayMs, long delayMs) {
            this.firstDelayMs = firstDelayMs;
            this.delayMs = delayMs;
        }

        @Override
        public T map(T value) throws Exception {
            long sleepMs = firstRecord ? firstDelayMs : delayMs;
            firstRecord = false;
            if (sleepMs > 0) {
                Thread.sleep(sleepMs);
            }
            return value;
        }
    }

    // ==================================================================================
    // 1) 非 keyed：BroadcastProcessFunction → CoBroadcastWithNonKeyedOperator
    // ==================================================================================

    /**
     * 非 keyed 的维表补全。
     *
     * <p>{@code BroadcastProcessFunction<IN1, IN2, OUT>} 里 IN1 是主流元素、IN2 是广播流元素。
     */
    public static class NonKeyedEnricher extends BroadcastProcessFunction<Event, Rule, String> {

        @Override
        public void processElement(Event event, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            // 主流侧：只读！类型是 ReadOnlyBroadcastState，根本没有 put/remove 方法
            ReadOnlyBroadcastState<String, String> dim = ctx.getBroadcastState(DIM_NON_KEYED);

            String name = dim.get(event.userId);
            // ReadOnlyBroadcastState 只提供 get / contains / immutableEntries 三个方法（1.20 里没有 keys()/values()）
            int size = 0;
            for (Map.Entry<String, String> ignored : dim.immutableEntries()) {
                size++;
            }

            out.collect("[非keyed processElement] " + event + " → 维表命中 key=\"" + event.userId + "\" ⇒ "
                    + (name == null ? "<未命中，走默认值 UNKNOWN>" : name)
                    + " | 当前广播态条目数=" + size);
        }

        @Override
        public void processBroadcastElement(Rule rule, Context ctx, Collector<String> out) throws Exception {
            // 广播侧：可写。ctx 是 BroadcastProcessFunction.Context，getBroadcastState 返回可写的 BroadcastState
            BroadcastState<String, String> dim = ctx.getBroadcastState(DIM_NON_KEYED);
            String old = dim.get(rule.key);
            dim.put(rule.key, rule.value);

            // 注意：广播侧没有 key context，这里没有「当前 key」的概念
            out.collect("[非keyed processBroadcastElement] 写入广播状态: " + rule.key + " = " + rule.value
                    + "（旧值=" + old + "）；这条记录会被复制到算子的每一个并行子任务");
        }
    }

    private static void demoNonKeyed() throws Exception {
        System.out.println();
        System.out.println("=== 1) 非 keyed 广播：events.connect(rules.broadcast(dim)) → CoBroadcastWithNonKeyedOperator ===");
        System.out.println("    维表 3 条规则先到（t=0），业务数据每条延时 300ms 后再到，因此都能命中维表");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 广播流：维表 upsert，必须也是有界流
        DataStream<Rule> rules = env.fromData(
                new Rule("user-1", "张三"),
                new Rule("user-2", "李四"),
                new Rule("user-3", "王五"));

        // .broadcast(descriptor) 返回 BroadcastStream：带上了「要维护哪些广播状态」的信息
        BroadcastStream<Rule> broadcastRules = rules.broadcast(DIM_NON_KEYED);

        DataStream<Event> events = env.fromData(
                        new Event("user-1", BASE + 0L, 11.5),
                        new Event("user-2", BASE + 100L, 22.0),
                        new Event("user-9", BASE + 200L, 33.5),
                        new Event("user-3", BASE + 300L, 44.0))
                .map(new DelayMapper<Event>(300L, 300L));

        // 非 keyed 流 + 广播流：注意这里不 keyBy，所以只能用 BroadcastProcessFunction
        events.connect(broadcastRules)
                .process(new NonKeyedEnricher())
                .print("1-非keyed广播");

        env.execute("BroadcastProcessFunctionDemo-1-nonkeyed");
    }

    // ==================================================================================
    // 2) keyed：KeyedBroadcastProcessFunction → CoBroadcastWithKeyedOperator
    // ==================================================================================

    /**
     * keyed 的维表补全 + 运行中更新维表。
     *
     * <p>{@code KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT>}：
     * 主流有 key（KS），广播流<b>没有</b> key。所以：
     *
     * <ul>
     *   <li>{@code processElement} 里可以 {@code ctx.getCurrentKey()} 读 keyed state（本 demo 只用广播态）；
     *   <li>{@code processBroadcastElement} 里<b>不能</b>读 keyed state（没有 key context），
     *       要改 keyed state 只能 {@code ctx.applyToKeyedState(...)}。
     * </ul>
     */
    public static class KeyedEnricher extends KeyedBroadcastProcessFunction<String, Event, Rule, String> {

        @Override
        public void processElement(Event event, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            // keyed 分支的 ReadOnlyContext 额外提供 getCurrentKey() / timerService()（keyed 版才有定时器）
            String key = ctx.getCurrentKey();
            ReadOnlyBroadcastState<String, String> dim = ctx.getBroadcastState(DIM_KEYED);
            String name = dim.get(event.userId);

            out.collect("[keyed processElement] key=" + key + " " + event + " → 维表=\"" + event.userId + "\" ⇒ "
                    + (name == null ? "<未命中，走默认值 UNKNOWN>" : name));
        }

        @Override
        public void processBroadcastElement(Rule rule, Context ctx, Collector<String> out) throws Exception {
            // 广播侧可写；这里没有 key，所以不能调用 getCurrentKey()
            BroadcastState<String, String> dim = ctx.getBroadcastState(DIM_KEYED);
            String old = dim.get(rule.key);
            dim.put(rule.key, rule.value);

            out.collect("[keyed processBroadcastElement] key 不存在（广播状态无 key）: " + rule.key + " : "
                    + old + " → " + rule.value + "；所有并行子任务都会收到并各自存一份副本");
        }
    }

    private static void demoKeyed() throws Exception {
        System.out.println();
        System.out.println("=== 2) keyed 广播：events.keyBy(userId).connect(rules.broadcast(dim)) → CoBroadcastWithKeyedOperator ===");
        System.out.println("    规则 user-1 先写入 张三-v1，1s 后又被更新成 张三-v2；");
        System.out.println("    业务数据每 400ms 一条（首条延后 400ms）：前 2 条看到 v1，后 4 条看到 v2 → 维表更新实时生效");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 广播流：第 1 条立即，第 2 条延后 1s（模拟运行中更新维表）
        DataStream<Rule> rules = env.fromData(new Rule("user-1", "张三-v1"), new Rule("user-1", "张三-v2"))
                .map(new DelayMapper<Rule>(0L, 1000L));

        BroadcastStream<Rule> broadcastRules = rules.broadcast(DIM_KEYED);

        // 业务流：每条延时 400ms（含第一条），整段落在 400ms ~ 2400ms
        DataStream<Event> events = env.fromData(
                        new Event("user-1", BASE + 0L, 1.0),
                        new Event("user-1", BASE + 100L, 2.0),
                        new Event("user-1", BASE + 200L, 3.0),
                        new Event("user-1", BASE + 300L, 4.0),
                        new Event("user-1", BASE + 400L, 5.0),
                        new Event("user-1", BASE + 500L, 6.0))
                .map(new DelayMapper<Event>(400L, 400L));

        // keyed 流 + 广播流：必须用 KeyedBroadcastProcessFunction
        events.keyBy(e -> e.userId)
                .connect(broadcastRules)
                .process(new KeyedEnricher())
                .print("2-keyed广播");

        env.execute("BroadcastProcessFunctionDemo-2-keyed");
    }

    // ==================================================================================

    public static void main(String[] args) throws Exception {
        System.out.println("### BroadcastProcessFunctionDemo：广播状态（维表补全）###");
        System.out.println("### 主流与广播流都是有界源；为了编排\"维表先到\"的顺序给主流加了固定延时 ###");

        demoNonKeyed();
        demoKeyed();

        System.out.println();
        System.out.println("=== 全部小节结束 ===");
        System.out.println("回收结论：");
        System.out.println("  1. 广播状态只能在 processBroadcastElement 里写；processElement 拿到的是只读视图（编译期拦截）");
        System.out.println("  2. 广播状态与 key 无关：它是 operator state，每个并行子任务各存一份完整副本");
        System.out.println("  3. 广播侧的 keyed state 只能通过 ctx.applyToKeyedState(...) 间接改");
        System.out.println("  4. 换来的能力：主流每条数据都能 O(1) 查到维表/规则，不用每条数据都去外部系统查一次");
    }
}
