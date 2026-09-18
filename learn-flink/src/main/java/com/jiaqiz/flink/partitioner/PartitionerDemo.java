package com.jiaqiz.flink.partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分区器（StreamPartitioner）全家桶：在<b>同一份数据</b>上分别使用
 * forward / rebalance / rescale / shuffle / broadcast / global / partitionCustom，
 * 每个分支都在 map 里打印 {@code getRuntimeContext().getIndexOfThisSubtask()}，
 * 直接看出数据被分到了哪些 subtask。
 *
 * <p>运行命令：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.partitioner.PartitionerDemo
 * </pre>
 *
 * <p>容易混淆的三个（重点）：
 *
 * <pre>
 *   forward()   一对一「就近转发」：要求上下游并行度相同，上游 subtask i 只发给下游 subtask i。
 *               因为不跨网络、且是 ForwardPartitioner，Flink 会把上下游合并进同一条算子链（同线程执行）。
 *               并行度不同会直接报错/语义不成立。
 *
 *   rebalance() 全局轮询：上游每条记录按 round-robin 发给所有下游 subtask，
 *               并行度可以不同；数据会被"均摊"，是解决数据倾斜最常用的手段。
 *               分布是「全局」轮询，上游每个 subtask 都面向全部下游。
 *
 *   rescale()   本地轮询：只在本地上游 subtask 对应的「下游子集」里 round-robin，
 *               即把下游按上游数量切片，上游 i 只发给它那一片；不跨整个集群，
 *               比 rebalance 网络开销小，但分布不一定全局均匀。
 *               上游并行度 m、下游 n 时，上游 i 负责下游 [i*n/m, (i+1)*n/m) 这一段。
 * </pre>
 *
 * <p>另外两个极端：shuffle() 完全随机；broadcast() 把每条记录复制给所有下游 subtask（数据量 ×下游并行度）；
 * global() 把所有记录都塞给下游 subtask 0（后续只有 1 个并行实例在干活）。
 * partitionCustom() 则由你按 key 自己算目标 subtask 下标。
 */
public class PartitionerDemo {

    /**
     * 观察者：把「值 + 当前处理它的 subtask 下标」打出来，用来看分区效果。
     * 必须是 RichMapFunction 才能拿到 RuntimeContext。
     */
    public static class SubtaskReporter extends RichMapFunction<Integer, String> {

        private static final long serialVersionUID = 1L;

        private final String branch;

        public SubtaskReporter(String branch) {
            this.branch = branch;
        }

        @Override
        public String map(Integer value) {
            return String.format(
                    "%-22s value=%-3d -> subtask %d/%d",
                    branch,
                    value,
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getNumberOfParallelSubtasks());
        }
    }

    /** partitionCustom 用的 key 选择器：直接拿元素本身当 key。 */
    public static class ValueKeySelector implements KeySelector<Integer, Integer> {

        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(Integer value) {
            return value;
        }
    }

    /**
     * 自定义分区器：返回值必须落在 [0, numPartitions) 区间。
     *
     * <p>这里刻意只返回 0 或 1（模 2），而不是 {@code key % numPartitions}，
     * 这样在并行度 4 的情况下能一眼看出「自定义分区确实生效了」——
     * 只有 subtask 0 和 1 会收到数据，2 和 3 一条都没有。
     */
    public static class ModTwoPartitioner implements Partitioner<Integer> {

        private static final long serialVersionUID = 1L;

        @Override
        public int partition(Integer key, int numPartitions) {
            int target = Math.abs(key) % 2; // 只映射到 0/1，无视 numPartitions=4
            // 防御性校验：返回越界下标会在运行时抛 IndexOutOfBoundsException
            return Math.min(target, numPartitions - 1);
        }
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度 4：分区效果才看得明显（2 个 subtask 时几乎看不出 rebalance/rescale 的差别）
        env.setParallelism(4);

        // 注意：env.fromData(...) 建出来的是「非并行 Source」，它的 Transformation 并行度被固定为 1
        // （不是 env 的并行度）。而 forward() 要求上下游并行度完全相等，所以这里必须显式
        // setParallelism(4)，否则 execute() 时会直接抛：
        //   java.lang.UnsupportedOperationException: Forward partitioning does not allow change of
        //   parallelism. Upstream operation: Source: Collection Source-1 parallelism: 1,
        //   downstream operation: Map-3 parallelism: 4 You must use another partitioning strategy,
        //   such as broadcast, rebalance, shuffle or global.
        final DataStream<Integer> source = env.fromData(1, 2, 3, 4, 5, 6, 7, 8).setParallelism(4);

        System.out.println("=== 同一份数据（1..8），并行度 4，各分区器的数据分布 ===");
        System.out.println("提示：print 输出的 \"N> \" 前缀就是写出该行的 subtask 下标");
        System.out.println();

        // ---------- forward()：一对一，上下游并行度必须相同 ----------
        // 上游 subtask i → 下游 subtask i；因为不跨网络，会与下游合并成算子链（同线程）。
        source.forward().map(new SubtaskReporter("forward()")).print();

        // ---------- rebalance()：全局 round-robin，最常用 ----------
        // 实现细节（RebalancePartitioner）：setup() 时随机挑一个起始 channel
        // （nextChannelToSendTo = ThreadLocalRandom.nextInt(numChannels)），之后每条记录 +1 取模，
        // 且每个上游 subtask 的 writer 都面向"全部"下游 subtask。
        // 所以数据量大时分布均匀；本项目数据量很小（每个上游 subtask 只有 2 条记录），
        // 实测会出现 3/2/1/2 这种"接近但不完全均匀"的结果，这是小样本 + 随机起点的正常现象。
        source.rebalance().map(new SubtaskReporter("rebalance()")).print();

        // ---------- rescale()：本地 round-robin ----------
        // 与 rebalance 的两点差别（RescalePartitioner 源码可对照）：
        //   1) 起始 channel 固定为 0（nextChannelToSendTo 初值 -1），不是随机；
        //   2) 只在自己的"下游子集"里轮询（JobGraph 层面被标记成 POINTWISE 分布模式）。
        // 源与下游并行度都是 4 时，每个上游 subtask 只对应 1 个下游 subtask，
        // 此时 rescale 的表现会"看起来像 forward"；真正的差异要并行度不同才看得到（见下面的 2->4）。
        source.rescale().map(new SubtaskReporter("rescale()(4->4)")).print();

        // ---------- shuffle()：随机 ----------
        // 语义：每条记录随机挑一个下游 channel。但本地小数据下分布会"看起来很不随机"，
        // 实测（本工程 8 条数据、上游 4 个 subtask、每个 subtask 只有 2 条记录）连续三次运行分别是
        // {subtask2×4, subtask3×4} / {subtask2×8} / {subtask0×4, subtask2×4}，只落在 1~2 个 subtask 上。
        // 原因有两层，都不是 bug：
        //   1) ShufflePartitioner 是 Serializable 的，4 个上游 subtask 拿到的是同一个 Random 的
        //      序列化副本 —— 种子状态相同，于是它们做出的 channel 选择序列完全相同；
        //   2) 每个上游 subtask 只有 2 条记录，所以整轮最多只会命中 2 个不同 channel。
        // 数据量一大（每个 subtask 成千上万条）就会自然铺开；只是"随机"也意味着**不保证均匀**，
        // 需要确定性均匀分布时应该用 rebalance()。
        source.shuffle().map(new SubtaskReporter("shuffle()")).print();

        // ---------- broadcast()：每条记录复制给所有下游 subtask ----------
        // 8 条输入 × 4 个下游 subtask = 32 行输出（数据量被放大 4 倍，慎用于大流）。
        // 典型用途：把小表/配置流广播给所有并行实例做 join。
        source.broadcast().map(new SubtaskReporter("broadcast()")).print();

        // ---------- global()：全部塞给 subtask 0 ----------
        // 下游只有 1 个实例真正干活，等于把并行度降成 1；常用于"全局有序"或"单点写外部系统"。
        source.global().map(new SubtaskReporter("global()")).print();

        // ---------- partitionCustom(Partitioner, keySelector) ----------
        // 由用户决定「哪个 key 去哪个 subtask」，常用于本地化 join、按业务维度隔离写出。
        source.partitionCustom(new ModTwoPartitioner(), new ValueKeySelector())
                .map(new SubtaskReporter("partitionCustom(%2)"))
                .print();

        // ---------- 额外对照：rescale() 在「上游并行度 2 → 下游并行度 4」时的真实效果 ----------
        // 上游 2 个 subtask 会被切成 2 片：subtask0 → 下游{0,1}，subtask1 → 下游{2,3}，
        // 每片内 round-robin。对比 rebalance() 的"全局轮询"，能看出 rescale 只做局部负载均衡。
        // 这里刻意「重新建一个独立的 source」而不是改上面那个的并行度：DataStream 是共享的构建器
        // 对象，改它的 setParallelism 会连带影响前面已经建好的所有分支（forward 分支会因此直接报错）。
        final DataStream<Integer> narrowSource =
                env.fromData(1, 2, 3, 4, 5, 6, 7, 8).setParallelism(2);
        narrowSource.rescale().map(new SubtaskReporter("rescale()(2->4)")).print();

        env.execute("PartitionerDemo");
    }
}
