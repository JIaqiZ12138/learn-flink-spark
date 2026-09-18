package com.jiaqiz.flink.async;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * {@link RichAsyncFunction} 演示：把「连接/线程池」的生命周期交给算子。
 *
 * <p>演示内容：
 *
 * <ol>
 *   <li>{@code open()} 里创建线程池（真实项目里就是异步客户端 / 连接池），并保存为成员变量；
 *   <li>{@code asyncInvoke()} 里复用这个线程池（每个子任务一份，绝不每条数据建一个）；
 *   <li>{@code close()} 里关闭，避免任务取消/重启时泄漏线程；
 *   <li>顺带演示 {@code getRuntimeContext().getIndexOfThisSubtask()} /
 *       {@code getNumberOfParallelSubtasks()} —— 说明为什么并行度是 2 时会看到两个线程池。
 * </ol>
 *
 * <p><b>{@code RichAsyncFunction} 与默认 {@link AsyncFunction} 的区别：</b>
 *
 * <ul>
 *   <li>{@code AsyncFunction} 只是 {@code Serializable} 接口，只有 {@code asyncInvoke} + {@code timeout}，
 *       <b>没有</b> {@code open/close}，也<b>拿不到</b> {@code RuntimeContext}：所以连接必须靠
 *       static 字段或「首次调用时懒加载」来创建，既不优雅也不可控（不能在任务结束时释放）。
 *   <li>{@code RichAsyncFunction} 继承 {@code AbstractRichFunction}，多了：
 *       {@code open(Configuration)} / {@code close()} 生命周期、{@code getRuntimeContext()}
 *       （子任务编号、并行度、任务名）、{@code getMetricGroup()}（注册 Counter/Gauge）、
 *       {@code getIterationRuntimeContext()}，以及 Flink 托管状态（{@code getListState} 等）。
 *   <li>结论：只要需要「打开/关闭资源」或「按子任务做隔离」，就用 Rich 版本。
 *       每并行子任务各持一份线程池 ⇒ 全作业线程总数 = 线程池大小 × 并行度，容量估计时要乘上去。
 * </ul>
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.async.RichAsyncFunctionDemo
 * </pre>
 */
public class RichAsyncFunctionDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度 2：可以看到 open() 被两个子任务各调用一次，各建一个线程池。
        env.setParallelism(2);

        final DataStream<String> source =
                env.fromData(
                        "p1|A|120|1000",
                        "p2|B|80|1100",
                        "p3|C|150|1200",
                        "p4|D|80|1300",
                        "p5|E|120|1400",
                        "p6|F|80|1500");

        final DataStream<String> timed =
                source.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (element, recordTimestamp) ->
                                                Long.parseLong(element.split("\\|")[3])));

        AsyncDataStream.unorderedWait(
                        timed,
                        new RichMockAsyncLookup(),
                        5_000L,
                        TimeUnit.MILLISECONDS,
                        8)
                .print();

        env.execute("rich-async-function-demo");
    }

    /**
     * 一个"有生命周期"的异步函数：open 建资源、asyncInvoke 用资源、close 放资源。
     *
     * <p>输入格式：{@code id|key|delayMs|eventTimeTs}。
     */
    public static class RichMockAsyncLookup extends RichAsyncFunction<String, String> {

        private static final long serialVersionUID = 1L;

        /**
         * 异步客户端/线程池：必须在 open() 里 new。
         *
         * <p>为什么加 transient：算子实例会被序列化分发（{@code ExecutorService} 不可序列化），
         * 标注 transient 让它只存在于"运行时实例"上，反序列化后由 open() 重新创建。
         */
        private transient ExecutorService asyncClient;

        /** 子任务编号：open() 里取一次即可，asyncInvoke 里复用（避免每条数据都查上下文）。 */
        private transient int subtaskIndex;

        /** 指标：Rich 版本才能拿到 metricGroup，默认 AsyncFunction 做不到。 */
        private transient Counter callCounter;

        @Override
        public void open(Configuration parameters) throws Exception {
            // Flink 1.20 的 AbstractRichFunction 只有 open(Configuration)；
            // 2.0 起换成了 open(OpenContext)，这里保持 1.20 的签名。
            subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            callCounter = getRuntimeContext().getMetricGroup().counter("asyncCalls");
            asyncClient =
                    Executors.newFixedThreadPool(
                            2,
                            r -> {
                                Thread t =
                                        new Thread(
                                                r,
                                                "rich-async-subtask-" + subtaskIndex + "-worker");
                                t.setDaemon(true);
                                return t;
                            });

            System.out.printf(
                    "[open ] subtask %d/%d task=%s -> 创建线程池 %s%n",
                    subtaskIndex,
                    getRuntimeContext().getNumberOfParallelSubtasks(),
                    getRuntimeContext().getTaskName(),
                    asyncClient);
        }

        @Override
        public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

            final String[] parts = input.split("\\|");
            final String id = parts[0];
            final long delayMs = Long.parseLong(parts[2]);
            callCounter.inc();

            // 复用 open() 里的线程池；每条数据不再 new 资源。
            CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    Thread.sleep(delayMs);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return "客户端返回(" + id + " on " + Thread.currentThread().getName() + ")";
                            },
                            asyncClient)
                    .thenAccept(
                            value ->
                                    resultFuture.complete(
                                            Collections.singletonList(
                                                    "[subtask-" + subtaskIndex + "] " + id + " -> " + value)))
                    .exceptionally(
                            error -> {
                                resultFuture.completeExceptionally(error);
                                return null;
                            });
        }

        @Override
        public void close() throws Exception {
            // 任务取消、失败重启、正常结束时都会调用，必须在这里释放资源。
            if (asyncClient != null) {
                asyncClient.shutdown();
                System.out.printf("[close] subtask %d -> 线程池已关闭%n", subtaskIndex);
            }
        }
    }
}
