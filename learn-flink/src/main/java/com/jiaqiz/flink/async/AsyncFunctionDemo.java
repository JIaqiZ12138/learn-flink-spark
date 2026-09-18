package com.jiaqiz.flink.async;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * {@code AsyncDataStream} 异步 I/O 演示：{@code orderedWait} vs {@code unorderedWait}。
 *
 * <p>演示内容：
 *
 * <ol>
 *   <li>用 {@link AsyncFunction} + {@link CompletableFuture#supplyAsync} 模拟一次 100ms 级别的异步外部调用
 *       （内部 {@code Thread.sleep} 模拟网络往返）；
 *   <li>同一份输入分别走 {@link AsyncDataStream#orderedWait} 与 {@link AsyncDataStream#unorderedWait}，
 *       对比输出<b>顺序</b>与<b>发射时刻</b>；
 *   <li>用「标记驱动的 watermark」把输入切成两段（segment），演示 <b>unordered 也不是完全无序</b>：
 *       watermark 不能越过元素，慢元素会顶住 watermark，进而顶住 watermark 之后的所有元素；
 *   <li>额外演示 {@code timeout}（超时降级）与 {@code capacity}（in-flight 上限）的真实行为。
 * </ol>
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.async.AsyncFunctionDemo
 * </pre>
 *
 * <p><b>关键 API 语义（阅读代码时对照）：</b>
 *
 * <ul>
 *   <li>{@code AsyncDataStream.orderedWait(stream, func, timeout, unit, capacity)}：结果按<b>输入顺序</b>
 *       发射（单并发子任务内严格保序），即使后面的异步调用先完成也要等前面的。代价是队头阻塞（head-of-line
 *       blocking）：最慢的那个请求会拖住它后面的所有输出。
 *   <li>{@code AsyncDataStream.unorderedWait(...)}：结果按<b>完成顺序</b>发射，先完成先输出。代价是乱序。
 *       吞吐更高的根本原因就在这里：不需要为保序而等待，慢请求不占用后续元素发射的机会，
 *       同样的 capacity 下单位时间能压出更多元素。
 *   <li>{@code capacity}：允许同时在飞（in-flight，已调用 asyncInvoke 但还没 complete）的请求数上限，
 *       也就是算子内部 {@code StreamElementQueue} 的容量。队列满时算子会阻塞上游（背压），因此
 *       capacity ≈ 并发度 × 单请求平均耗时 / 目标吞吐 的数量级估计。capacity 太小 → 异步退化成串行；
 *       太大 → 状态/内存压力大（这些在飞请求在 checkpoint 时会被 snapshot）。
 *   <li>{@code timeout}：单条元素从进入算子到 {@code resultFuture.complete(...)} 的最长等待，
 *       超时会调用 {@link AsyncFunction#timeout}（默认实现是抛异常让作业失败），演示里重写为「降级输出」。
 *   <li><b>注意</b>：{@code AsyncFunction.asyncInvoke(IN, ResultFuture)} 返回值是 {@code void}，
 *       并<b>不是</b>返回 {@code CompletableFuture}。约定是：{@code asyncInvoke} 里发起异步请求后立刻返回，
 *       由异步回调（线程池线程）调用 {@code resultFuture.complete(Collection)} 恰好一次；
 *       自己用 {@code CompletableFuture} 的话就用 {@code thenAccept(resultFuture::complete)} 把两者桥接起来。
 *       另外 {@code asyncInvoke} <b>不允许阻塞</b>，否则退化成同步调用（这也是它和 map 的本质区别）。
 * </ul>
 *
 * @see org.apache.flink.streaming.api.operators.async.queue.OrderedStreamElementQueue
 * @see org.apache.flink.streaming.api.operators.async.queue.UnorderedStreamElementQueue
 */
public class AsyncFunctionDemo {

    /** 输入里约定的 watermark 标记：遇到它就按"已见过的最大事件时间"发一个 watermark。 */
    private static final String MARKER = "WM";

    /** 异步调用超时（毫秒）。 */
    private static final long TIMEOUT_MS = 5_000L;

    /** in-flight 上限（算子内部队列容量）。 */
    private static final int CAPACITY = 16;

    /**
     * 模拟"外部异步客户端"的公共线程池。
     *
     * <p>为什么是 static：{@code AsyncFunction} 实例会被序列化后分发到 TaskManager，而
     * {@link ExecutorService} 不可序列化；static 字段不参与序列化，所以放这里最省事。
     * 生产代码更应该像 {@link RichAsyncFunctionDemo} 那样在 {@code open()} 里按子任务创建。
     */
    private static final ExecutorService ASYNC_POOL =
            Executors.newFixedThreadPool(
                    8,
                    r -> {
                        Thread t = new Thread(r, "mock-async-client");
                        t.setDaemon(true);
                        return t;
                    });

    /** 作业开始时间，仅用于在输出里打印"相对耗时"，让保序代价肉眼可见。 */
    private static long jobStartMs;

    public static void main(String[] args) throws Exception {

        jobStartMs = System.currentTimeMillis();

        try {
            runModeDemo(true);
            runModeDemo(false);
            runTimeoutAndCapacityDemo();
        } finally {
            // 作业结束后回收线程池，否则 JVM 里会残留非 daemon 线程。
            ASYNC_POOL.shutdown();
            ASYNC_POOL.awaitTermination(5, TimeUnit.SECONDS);
            System.out.println("\n[main] 异步客户端线程池已 shutdown()");
        }
    }

    // ------------------------------------------------------------------------
    //  1) ordered / unordered 顺序 + 时刻对比
    // ------------------------------------------------------------------------

    /**
     * 同一份输入，同一份"快慢不一"的输入，跑一遍 ordered 或 unordered。
     *
     * <p>输入被一个 WM 标记切成两段：r1..r6（segment 1）→ WM → r7..r9（segment 2）。
     * r1 慢（600ms），其余快（60ms）。
     */
    private static void runModeDemo(boolean ordered) throws Exception {

        final String mode = ordered ? "orderedWait  " : "unorderedWait";
        System.out.println("\n================ " + mode + " ================");
        System.out.println(
                "输入: [r1 A 慢600ms, r2..r6 快60ms] --WM--> [r7, r8, r9 快60ms]，并行度 1");
        System.out.println("预期: ordered 严格按输入序发射(r1 先卡 600ms，后面全被顶住)；");
        System.out.println("      unordered 段内按完成序发射(r2..r6 先出来)，但 WM 之后要等 r1 完成。");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度 1：让"全局顺序"这件事在输出里看得清楚。
        // 多并行度下 order 只在单个子任务内成立，跨子任务没有全局顺序。
        env.setParallelism(1);

        final DataStream<String> source =
                env.fromData(
                        "r1|A|600|1000",
                        "r2|B|60|1100",
                        "r3|C|60|1200",
                        "r4|D|60|1300",
                        "r5|E|60|1400",
                        "r6|F|60|1500",
                        MARKER, // 事件时间 1500 处的 watermark：segment 边界
                        "r7|A|60|2000",
                        "r8|B|60|2100",
                        "r9|C|60|2200");

        // 事件时间 + watermark：async 算子在事件时间下会把 watermark 也放进队列里排队，
        // 所以 watermark 同样会被前面的 in-flight 元素"顶住"。
        final DataStream<String> timed =
                source.assignTimestampsAndWatermarks(
                        // forGenerator：自定义 WatermarkGenerator（这里用标记驱动，见类尾注释）
                        WatermarkStrategy.<String>forGenerator(ctx -> new MarkerDrivenWatermarks())
                                .withTimestampAssigner(
                                        (element, recordTimestamp) ->
                                                MARKER.equals(element) ? 0L : parseTs(element)));

        // 标记本身不是业务数据，赋完时间戳（并触发 watermark）之后丢掉。
        final DataStream<String> records = timed.filter(element -> !MARKER.equals(element));

        final SingleOutputStreamOperator<String> result =
                ordered
                        ? AsyncDataStream.orderedWait(
                                records,
                                new MockAsyncLookup(),
                                TIMEOUT_MS,
                                TimeUnit.MILLISECONDS,
                                CAPACITY)
                        : AsyncDataStream.unorderedWait(
                                records,
                                new MockAsyncLookup(),
                                TIMEOUT_MS,
                                TimeUnit.MILLISECONDS,
                                CAPACITY);

        // 关键观测点：asyncDone 是"异步调用完成"的时刻（线程池里），emit 是"元素真正被算子往下游发射"
        // 的时刻。两者之差就是被 ordered 保序（或被 watermark 挡住）白白等待的时间。
        result.map(new EmitStamp(mode)).print();
        env.execute("async-" + (ordered ? "ordered" : "unordered"));
    }

    // ------------------------------------------------------------------------
    //  2) timeout / capacity 演示
    // ------------------------------------------------------------------------

    /**
     * timeout 与 capacity 的真实行为：t2 的异步耗时 900ms > timeout 400ms，
     * 触发 {@link AsyncFunction#timeout}（被重写为"降级输出"，否则默认实现会让作业失败）。
     */
    private static void runTimeoutAndCapacityDemo() throws Exception {

        System.out.println("\n================ timeout / capacity ================");
        System.out.println("输入: t1 K 100ms, t2 K 900ms, t3 K 100ms；timeout=400ms，capacity=2");
        System.out.println("预期: t2 在 400ms 处超时降级输出；capacity=2 使 t3 必须等前面腾出名额。");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStream<String> source =
                env.fromData("t1|K|100|1000", "t2|K|900|1100", "t3|K|100|1200");

        final DataStream<String> records =
                source.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (element, recordTimestamp) -> parseTs(element)));

        AsyncDataStream.orderedWait(
                        records,
                        new MockAsyncLookup(),
                        400L, // timeout：比 t2 的 900ms 短，故意制造超时
                        TimeUnit.MILLISECONDS,
                        2) // capacity：只允许 2 个在飞请求
                .map(new EmitStamp("timeout/cap2"))
                .print();

        env.execute("async-timeout-capacity");
    }

    // ------------------------------------------------------------------------
    //  模拟的异步外部调用
    // ------------------------------------------------------------------------

    /**
     * 模拟一次异步外部调用（比如异步查 HBase / 调 RPC）。
     *
     * <p>输入格式：{@code id|key|delayMs|eventTimeTs}（第四个字段只给 watermark 生成器用）。
     *
     * <p>为什么用"不同 key"：async 算子的保序/乱序是<b>按子任务</b>而不是按 key 的，
     * 不同 key 的元素也会互相排队；这里用不同 key 只是为了让输出里能看清是哪条数据。
     */
    public static class MockAsyncLookup implements AsyncFunction<String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

            final String[] parts = input.split("\\|");
            final String id = parts[0];
            final long delayMs = Long.parseLong(parts[2]);

            // 关键：asyncInvoke 立刻返回，真正的等待发生在别的线程池里。
            // 如果这里直接 Thread.sleep，就退化成同步 map，异步 I/O 的意义（用少量并发换吞吐）就没了。
            CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    Thread.sleep(delayMs);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return "外部查询结果(" + id + ")";
                            },
                            ASYNC_POOL)
                    // CompletableFuture → ResultFuture 的桥接：complete() 必须恰好调用一次。
                    // 回调运行在 ASYNC_POOL 线程上，ResultHandler 内部是线程安全的。
                    .thenAccept(
                            value ->
                                    resultFuture.complete(
                                            Collections.singletonList(
                                                    "OK|" + id + "|" + delayMs + "|" + elapsedMs())))
                    .exceptionally(
                            error -> {
                                // 异步调用抛异常时也必须 complete（这里用 completeExceptionally 让作业失败，
                                // 生产上通常换成降级/重试）。
                                resultFuture.completeExceptionally(error);
                                return null;
                            });
        }

        /** 重写超时逻辑：默认实现是抛异常（作业失败），这里降级输出一条标记，作业继续跑。 */
        @Override
        public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
            final String[] parts = input.split("\\|");
            resultFuture.complete(
                    Collections.singletonList(
                            "TIMEOUT|" + parts[0] + "|" + Long.parseLong(parts[2]) + "|" + elapsedMs()));
        }
    }

    /**
     * 给每条结果补上"发射时刻"：{@code {flag}|{id}|{异步耗时}|{完成时刻}} → 一行可读文本。
     *
     * <p>这个 map 会被链到 async 算子的下游（同一 task 线程），所以它执行的时间就是元素真正被
     * async 算子发射出来的时间。async 算子的保序/watermark 阻塞都发生在它之前。
     */
    public static class EmitStamp implements MapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private final String mode;

        public EmitStamp(String mode) {
            this.mode = mode;
        }

        @Override
        public String map(String value) throws Exception {
            final String[] p = value.split("\\|");
            return String.format(
                    "[%-7s] %-13s %-4s asyncDone@+%-5sms  emit@+%-5sms  (异步耗时 %sms)",
                    p[0], mode, p[1], p[3], elapsedMs(), p[2]);
        }
    }

    // ------------------------------------------------------------------------
    //  工具方法 / 自定义 watermark 生成器
    // ------------------------------------------------------------------------

    /** 相对 main 开始时刻的耗时（含 MiniCluster 启动的固定开销，看同一作业内的相对差值即可）。 */
    private static long elapsedMs() {
        return System.currentTimeMillis() - jobStartMs;
    }

    private static long parseTs(String element) {
        return Long.parseLong(element.split("\\|")[3]);
    }

    /**
     * 标记驱动的 watermark 生成器：只有读到 {@link #MARKER} 才发 watermark。
     *
     * <p>为什么不用 {@code forBoundedOutOfOrderness}/{@code forMonotonousTimestamps}：
     * 它们的 watermark 由<b>处理时间定时器</b>周期触发（{@code autoWatermarkInterval} 默认 200ms），
     * 对"跑几百毫秒就结束"的本地 demo 来说，发几次、什么时候发都不确定，
     * 输入到底被切成几段就无法预期。用标记驱动可以精确控制 segment 边界，
     * 从而稳定复现"unordered 段内乱序、段间不能越过 watermark"这一现象。
     *
     * <p>顺带说明：{@code onPeriodicEmit} 在这里<b>故意留空</b>（真实项目里通常在这里发 watermark）。
     * 有界 source 跑完时，框架仍会往下游发一个 {@code Long.MAX_VALUE} 的 watermark，用来触发收尾。
     */
    public static class MarkerDrivenWatermarks implements WatermarkGenerator<String> {

        private static final long serialVersionUID = 1L;

        private long maxTimestamp = Long.MIN_VALUE;

        @Override
        public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
            if (MARKER.equals(event)) {
                // 注意 TimestampsAndWatermarksOperator 是"先 collect 元素、再调 onEvent"，
                // 所以这个 watermark 排在标记元素之后（标记随后被 filter 丢掉，不影响正确性）。
                output.emitWatermark(new Watermark(maxTimestamp));
            } else {
                maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 故意不发：本 demo 的 watermark 完全由输入里的 WM 标记驱动。
        }
    }
}
