package com.jiaqiz.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * 从零实现一个<b>有界 FLIP-27 Source</b>（新版 Source API），并跑起来打印结果。
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.source.CustomSourceDemo
 * </pre>
 *
 * <h2>三段式结构</h2>
 *
 * <ul>
 *   <li>{@code NumberRangeSource implements Source&lt;String, NumberRangeSplit, List&lt;NumberRangeSplit&gt;&gt;}
 *       —— 「工厂」：声明有界性、造 reader、造 enumerator、提供两个序列化器。
 *   <li>{@code NumberRangeEnumerator implements SplitEnumerator&lt;...&gt;} —— 跑在 <b>JobManager</b>
 *       （SourceCoordinator）线程上，负责把 split 分给各个 reader。
 *   <li>{@code NumberRangeReader implements SourceReader&lt;...&gt;} —— 跑在 <b>TaskManager</b> 的
 *       StreamTask 线程上，负责真正读数据并 {@code ReaderOutput.collect(...)}。
 *   <li>{@code NumberRangeSplit implements SourceSplit} —— 分片元数据，必须能被 split serializer 序列化。
 * </ul>
 *
 * <h2>常见误区澄清</h2>
 *
 * <ol>
 *   <li><b>Source 也是一种 StreamOperator。</b>{@code env.fromSource(...)} 在
 *       StreamGraph 里生成的是一条 {@code SourceTransformation}，算子链翻译时它被翻译成
 *       {@code org.apache.flink.streaming.api.operators.SourceOperator}。也就是说 Source 并不是什么
 *       「特殊入口」，它和 map/filter 一样占一个算子、占一个 task slot，同样有
 *       open/initializeState/snapshotState/close 生命周期（SourceOperator 的
 *       {@code snapshotState} 会把 {@code sourceReader.snapshotState(checkpointId)} 存进 operator state）。
 *   <li><b>SourceReader 跑在 StreamTask 线程上。</b>SourceOperator 在 {@code initializeState} 里
 *       {@code createReader(...)} 并调 {@code reader.start()}；之后每次
 *       {@code emitNext} 都直接在当前 StreamTask 的 mailbox 线程里调
 *       {@code reader.pollNext(output)}。所以 pollNext 里千万不要阻塞（旧 API 里
 *       "run() 里 while(true) + 自己的线程" 那种模型在新 API 中是不存在的）。
 *   <li><b>MPP：pollNext 一次只产一条。</b>该方法是协作式的，返回 {@code MORE_AVAILABLE}
 *       表示「还有」，{@code NOTHING_AVAILABLE} 表示「暂时没有，等我 isAvailable() 那个 future 完成」，
 *       {@code END_OF_INPUT} 表示「我这个读者彻底读完了」。SourceOperator 把
 *       END_OF_INPUT 映射成 {@code DataInputStatus.END_OF_DATA} 并进入 DATA_FINISHED，
 *       有界作业（Boundedness.BOUNDED）就靠这条路径正常结束。
 *   <li><b>谁告诉 reader「没有更多 split 了」？</b>是 enumerator 通过
 *       {@code context.signalNoMoreSplits(subtask)}。reader 侧会收到
 *       {@code notifyNoMoreSplits()}。这一步绝对不能漏，否则 reader 永远停在
 *       NOTHING_AVAILABLE，作业会挂住（本 demo 用 4 个 split + 2 个 reader 演示分配与收尾）。
 * </ol>
 *
 * <p>本 demo 的数据：4 个 split，每个 split 产出 3 条字符串，共 12 条。
 */
public class CustomSourceDemo {

    /** 每个 split 产出的记录条数。 */
    private static final int RECORDS_PER_SPLIT = 3;

    /** split 的个数（故意多于 source 的并行度，这样能看到多次分配）。 */
    private static final int NUM_SPLITS = 4;

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度 2：SourceOperator 会有 2 个并发实例，各自持有一个 NumberRangeReader，
        // 它们会各自向 SourceCoordinator 里的 SplitEnumerator 索要 split。
        env.setParallelism(2);

        final NumberRangeSource source = new NumberRangeSource(NUM_SPLITS, RECORDS_PER_SPLIT);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "custom-source")
                .print()
                .name("print-sink");

        env.execute("learn-flink FLIP-27 custom source demo");
    }

    // ==================================================================================
    //  1) Source 本体：一个「工厂」
    // ==================================================================================

    /**
     * 产出 {@code [0, NUM_SPLITS * RECORDS_PER_SPLIT)} 的字符串，按 split 切分。
     *
     * <p>实现 {@link ResultTypeQueryable} 是为了让 {@code env.fromSource(...)} 能推断出
     * {@code TypeInformation<String>}（不实现也行，但就要显式传第 4 个参数 typeInfo）。
     *
     * <p>本类必须可序列化：客户端会把 Source 对象序列化后随 JobGraph 发给 JobManager。
     */
    public static class NumberRangeSource
            implements Source<String, NumberRangeSplit, List<NumberRangeSplit>>,
                    ResultTypeQueryable<String> {

        private static final long serialVersionUID = 1L;

        private final int numSplits;
        private final int recordsPerSplit;

        public NumberRangeSource(int numSplits, int recordsPerSplit) {
            this.numSplits = numSplits;
            this.recordsPerSplit = recordsPerSplit;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }

        /** 有界：作业处理完所有 split 后正常结束（对比 KafkaSource 的 CONTINUOUS_UNBOUNDED）。 */
        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        /**
         * 在 TaskManager 上被调用（实际上是在客户端/JobManager 反序列化 Source 后、
         * SourceOperator.initializeState 里调用 createReader）。并行度是几，就会被调用几次。
         */
        @Override
        public SourceReader<String, NumberRangeSplit> createReader(SourceReaderContext readerContext) {
            return new NumberRangeReader(readerContext);
        }

        /**
         * 在 JobManager 的 SourceCoordinator 线程上被调用一次。这里同步地把 split 列表算好，
         * 交给 enumerator 后续按需分配（也可以在 enumerator 里用 context.callAsync 异步发现分片，
         * 那是 Kafka/文件这类「分片要动态发现」的连接器的做法）。
         */
        @Override
        public SplitEnumerator<NumberRangeSplit, List<NumberRangeSplit>> createEnumerator(
                SplitEnumeratorContext<NumberRangeSplit> enumContext) throws Exception {

            final List<NumberRangeSplit> splits = new ArrayList<>(numSplits);
            for (int i = 0; i < numSplits; i++) {
                final int from = i * recordsPerSplit;
                splits.add(new NumberRangeSplit("split-" + i, from, from + recordsPerSplit - 1));
            }
            return new NumberRangeEnumerator(enumContext, splits);
        }

        /** 从 checkpoint 恢复 enumerator（本 demo 没开 checkpoint，走不到，但接口要求实现）。 */
        @Override
        public SplitEnumerator<NumberRangeSplit, List<NumberRangeSplit>> restoreEnumerator(
                SplitEnumeratorContext<NumberRangeSplit> enumContext, List<NumberRangeSplit> checkpoint)
                throws Exception {
            return new NumberRangeEnumerator(enumContext, checkpoint);
        }

        /** split 从 enumerator 发给 reader 时要序列化，reader 做快照时也要序列化。 */
        @Override
        public SimpleVersionedSerializer<NumberRangeSplit> getSplitSerializer() {
            return new NumberRangeSplitSerializer();
        }

        /** enumerator 自己的 checkpoint（这里是「还没发出去的 split」）。 */
        @Override
        public SimpleVersionedSerializer<List<NumberRangeSplit>> getEnumeratorCheckpointSerializer() {
            return new NumberRangeSplitListSerializer();
        }
    }

    // ==================================================================================
    //  2) SourceSplit：分片元数据
    // ==================================================================================

    /** 一个闭区间 {@code [from, to]}，产出 {@code from..to} 每条一个字符串。 */
    public static class NumberRangeSplit implements SourceSplit {

        private final String splitId;
        private final int from;
        private final int to;

        public NumberRangeSplit(String splitId, int from, int to) {
            this.splitId = splitId;
            this.from = from;
            this.to = to;
        }

        /** 唯一标识，Flink 用它跟踪 split 的分配与恢复，绝不能重复。 */
        @Override
        public String splitId() {
            return splitId;
        }

        public int from() {
            return from;
        }

        public int to() {
            return to;
        }

        @Override
        public String toString() {
            return "NumberRangeSplit{" + splitId + ", [" + from + ", " + to + "]}";
        }
    }

    // ==================================================================================
    //  3) SplitEnumerator：跑在 JobManager 上，负责分配 split
    // ==================================================================================

    /**
     * 最朴素的分配策略：<b>先到先得</b>。每当某个 reader 索要 split
     * （{@link SourceReaderContext#sendSplitRequest()} 发来的 RequestSplitEvent），
     * 就从队列里弹一个给它；队列空了就 {@code signalNoMoreSplits} 告诉它「你读完了」。
     *
     * <p>这也是 Flink 自带 {@code IteratorSourceEnumerator}（用于 {@code NumberSequenceSource}）
     * 的实现方式。另一种常见写法是在 {@code addReader(int)} 里主动分配，或在
     * {@code start()} 里用 {@code context.callAsync(callable, handler)} 异步发现分片后再分配。
     */
    public static class NumberRangeEnumerator
            implements SplitEnumerator<NumberRangeSplit, List<NumberRangeSplit>> {

        private final SplitEnumeratorContext<NumberRangeSplit> context;
        private final Queue<NumberRangeSplit> remainingSplits;

        NumberRangeEnumerator(
                SplitEnumeratorContext<NumberRangeSplit> context, List<NumberRangeSplit> splits) {
            this.context = context;
            this.remainingSplits = new ArrayDeque<>(splits);
        }

        @Override
        public void start() {
            // 什么都不用做：本 demo 的 split 在构造时就已知（createEnumerator 里算好了），
            // 分配动作放在 handleSplitRequest 里，等 reader 主动开口。
            System.out.println(
                    "[enumerator] start, parallelism="
                            + context.currentParallelism()
                            + ", pending splits="
                            + remainingSplits);
        }

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            final NumberRangeSplit split = remainingSplits.poll();
            if (split != null) {
                // assignSplit 内部就是 assignSplits(new SplitsAssignment<>(split, subtask))，
                // split 会经由 coordinator 的 RPC 送到对应 reader 的 addSplits(...)。
                context.assignSplit(split, subtaskId);
                System.out.println(
                        "[enumerator] assign " + split + " -> reader#" + subtaskId);
            } else {
                // 关键收尾：告诉该 reader「不会再有 split 了」。
                // reader 收到 notifyNoMoreSplits() 后，pollNext 才会返回 END_OF_INPUT。
                context.signalNoMoreSplits(subtaskId);
                System.out.println("[enumerator] no more splits -> reader#" + subtaskId);
            }
        }

        @Override
        public void addSplitsBack(List<NumberRangeSplit> splits, int subtaskId) {
            // reader 失败/重启时把未完成的 split 还回来，重新入队。
            System.out.println("[enumerator] addSplitsBack " + splits + " from reader#" + subtaskId);
            remainingSplits.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {
            // 故意留空：本 demo 选择「reader 主动索要」的模型（见类注释）。
            System.out.println("[enumerator] reader#" + subtaskId + " registered");
        }

        @Override
        public List<NumberRangeSplit> snapshotState(long checkpointId) {
            return new ArrayList<>(remainingSplits);
        }

        @Override
        public void close() throws IOException {
            // 释放外部资源（Kafka admin client 之类）。本 demo 没有。
        }
    }

    // ==================================================================================
    //  4) SourceReader：跑在 TaskManager 的 StreamTask 线程上，真正读数据
    // ==================================================================================

    /**
     * 一次只产出一条记录，语义与 Flink 自带 {@code IteratorSourceReaderBase} 对齐。
     *
     * <p>核心状态机：
     *
     * <ul>
     *   <li>当前 split 还没读完 → 发一条，返回 MORE_AVAILABLE；
     *   <li>当前 split 读完 → 若手上没别的 split 且没收到 no-more-splits，则 sendSplitRequest() 再要一个；
     *   <li>手上没 split → 有就换过去；没有且已 noMoreSplits → END_OF_INPUT；否则 NOTHING_AVAILABLE。
     * </ul>
     */
    public static class NumberRangeReader implements SourceReader<String, NumberRangeSplit> {

        private final SourceReaderContext context;

        /** 已分配但还没开始读的 split。 */
        private final Queue<NumberRangeSplit> remainingSplits = new ArrayDeque<>();

        /** 当前正在读的 split，以及下一个要发的数字（闭区间，next > to 表示读完）。 */
        private NumberRangeSplit currentSplit;

        private int next;

        /** 是否已经收到 enumerator 的 signalNoMoreSplits。 */
        private boolean noMoreSplits;

        /**
         * 可用性 future：SourceOperator.getAvailableFuture() 就是拿它。
         * 一旦 addSplits/notifyNoMoreSplits 到达，complete 它，StreamTask 就会重新调 pollNext。
         */
        private CompletableFuture<Void> availability = new CompletableFuture<>();

        NumberRangeReader(SourceReaderContext context) {
            this.context = context;
        }

        @Override
        public void start() {
            // 开局先要一个 split。这个调用会发出 RequestSplitEvent，
            // 最终触发 enumerator.handleSplitRequest(getIndexOfSubtask(), ...)。
            System.out.println("[reader#" + context.getIndexOfSubtask() + "] start, request a split");
            context.sendSplitRequest();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
            while (true) {
                if (currentSplit != null) {
                    if (next <= currentSplit.to()) {
                        output.collect(
                                "split="
                                        + currentSplit.splitId()
                                        + " reader="
                                        + context.getIndexOfSubtask()
                                        + " value="
                                        + next);
                        next++;
                        return InputStatus.MORE_AVAILABLE;
                    }
                    // 当前 split 读完了
                    currentSplit = null;
                    if (remainingSplits.isEmpty() && !noMoreSplits) {
                        context.sendSplitRequest();
                    }
                }

                final NumberRangeSplit split = remainingSplits.poll();
                if (split != null) {
                    currentSplit = split;
                    next = split.from();
                    continue; // 回到循环顶部，马上产出新 split 的第一条
                }

                if (noMoreSplits) {
                    // 所有 split 都读完了 —— SourceOperator 会把 END_OF_INPUT 变成 END_OF_DATA，
                    // 有界作业由此收敛。
                    System.out.println(
                            "[reader#" + context.getIndexOfSubtask() + "] END_OF_INPUT");
                    return InputStatus.END_OF_INPUT;
                }

                // 暂时没活干：重置 future，等 addSplits / notifyNoMoreSplits 把它 complete 掉。
                if (availability.isDone()) {
                    availability = new CompletableFuture<>();
                }
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        @Override
        public List<NumberRangeSplit> snapshotState(long checkpointId) {
            // 没读完的 split（含当前 split 的进度）要存进 state，恢复时才能续读。
            if (currentSplit == null && remainingSplits.isEmpty()) {
                return Collections.emptyList();
            }
            final List<NumberRangeSplit> all = new ArrayList<>(remainingSplits.size() + 1);
            if (currentSplit != null && next <= currentSplit.to()) {
                all.add(new NumberRangeSplit(currentSplit.splitId(), next, currentSplit.to()));
            }
            all.addAll(remainingSplits);
            return all;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return availability;
        }

        @Override
        public void addSplits(List<NumberRangeSplit> splits) {
            System.out.println(
                    "[reader#" + context.getIndexOfSubtask() + "] addSplits " + splits);
            remainingSplits.addAll(splits);
            availability.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {
            System.out.println(
                    "[reader#" + context.getIndexOfSubtask() + "] notifyNoMoreSplits");
            noMoreSplits = true;
            availability.complete(null);
        }

        @Override
        public void close() throws Exception {
            // 释放 reader 持有的资源（Kafka consumer 等）。本 demo 没有。
        }
    }

    // ==================================================================================
    //  5) 两个序列化器
    // ==================================================================================

    /** split 的序列化：splitId(UTF) + from(int) + to(int)。 */
    private static class NumberRangeSplitSerializer
            implements SimpleVersionedSerializer<NumberRangeSplit> {

        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(NumberRangeSplit split) throws IOException {
            final DataOutputSerializer out = new DataOutputSerializer(32);
            out.writeUTF(split.splitId());
            out.writeInt(split.from());
            out.writeInt(split.to());
            return out.getCopyOfBuffer();
        }

        @Override
        public NumberRangeSplit deserialize(int version, byte[] serialized) throws IOException {
            if (version != VERSION) {
                throw new IOException("Unsupported split serializer version: " + version);
            }
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return new NumberRangeSplit(in.readUTF(), in.readInt(), in.readInt());
        }
    }

    /** enumerator checkpoint 的序列化：size(int) + 每个 split。 */
    private static class NumberRangeSplitListSerializer
            implements SimpleVersionedSerializer<List<NumberRangeSplit>> {

        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(List<NumberRangeSplit> checkpoint) throws IOException {
            final DataOutputSerializer out = new DataOutputSerializer(64 + checkpoint.size() * 24);
            out.writeInt(checkpoint.size());
            for (NumberRangeSplit split : checkpoint) {
                out.writeUTF(split.splitId());
                out.writeInt(split.from());
                out.writeInt(split.to());
            }
            return out.getCopyOfBuffer();
        }

        @Override
        public List<NumberRangeSplit> deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != VERSION) {
                throw new IOException("Unsupported checkpoint serializer version: " + version);
            }
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            final int size = in.readInt();
            final List<NumberRangeSplit> splits = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                splits.add(new NumberRangeSplit(in.readUTF(), in.readInt(), in.readInt()));
            }
            return splits;
        }
    }
}
