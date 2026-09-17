package com.jiaqiz.flink.sample;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.util.Collector;

/**
 * 用同一个 WordCount 程序，依次打印出 Flink 的三张图，直观对比它们的差异：
 *
 * <ol>
 *   <li><b>StreamGraph</b> —— 逻辑图：每个算子一个节点，保留完整拓扑
 *   <li><b>JobGraph</b> —— 物理图：算子链已合并，JobVertex 数量变少，出现 IntermediateDataSet
 *   <li><b>ExecutionGraph</b> —— 执行图：按并行度展开成 ExecutionVertex，带状态与 attempt
 * </ol>
 *
 * <p>运行：
 *
 * <pre>
 *   mvn -pl learn-flink compile exec:exec -Dmain.class=com.jiaqiz.flink.sample.GraphComparisonJob
 * </pre>
 *
 * <p>说明：本类是"三张图"的对照组，普通 WordCount 见 {@link WordCountJob}。
 */
public class GraphComparisonJob {

    public static void main(String[] args) throws Exception {

        // ============================================================
        // 第 0 步：构建逻辑拓扑（原始程序）
        // ============================================================
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设为 2，这样 ExecutionGraph 的"展开"才看得出来（1 和 1 对比没意义）
        env.setParallelism(2);

        final DataStream<String> lines =
                env.fromData("hello flink", "hello spark", "flink and spark").name("source");

        final DataStream<Tuple2<String, Integer>> counts =
                lines.flatMap(
                                (String line, Collector<Tuple2<String, Integer>> out) -> {
                                    for (String w : line.split("\\s+")) {
                                        if (!w.isEmpty()) {
                                            out.collect(Tuple2.of(w, 1));
                                        }
                                    }
                                })
                        .returns(Types.TUPLE(Types.STRING, Types.INT))
                        .name("flatmap")
                        .keyBy(t -> t.f0)
                        .sum(1)
                        .name("sum");

        counts.print().name("sink");

        // ============================================================
        // 第 1 步：StreamGraph
        // 注意 getStreamGraph(false) —— 传 false 表示"生成后不清空 transformations"，
        // 否则后面没法再拿它转 JobGraph。
        // ============================================================
        final StreamGraph streamGraph = env.getStreamGraph(false);
        printStreamGraph(streamGraph);

        // ============================================================
        // 第 2 步：JobGraph（由 StreamGraph 转换，算子链在这一步生成）
        // ============================================================
        final JobGraph jobGraph = streamGraph.getJobGraph();
        printJobGraph(jobGraph);

        // ============================================================
        // 第 3 步：ExecutionGraph
        // 前两张图在客户端就能拿到；ExecutionGraph 只存在于 JobManager 里，
        // 所以必须真的把作业跑起来。这里用 MiniCluster 把 JM/TM 拉在同一个 JVM 内。
        // ============================================================

        final MiniCluster miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(new Configuration())
                                .setNumTaskManagers(2)
                                .setNumSlotsPerTaskManager(2)
                                .build());
        miniCluster.start();
        try {
            final JobID jobId = miniCluster.submitJob(jobGraph).get().getJobID();
            System.out.println("\n作业已提交，jobId = " + jobId);

            // 等作业跑到终态。本作业是有限流，几秒内就会 FINISHED。
            // 等到终态再取 ExecutionGraph，输出才是确定的（每个子任务状态都为 FINISHED）。
            JobStatus status = JobStatus.INITIALIZING;
            for (int i = 0; i < 300; i++) {
                status = miniCluster.getJobStatus(jobId).get();
                if (status.isGloballyTerminalState()) {
                    break;
                }
                Thread.sleep(100);
            }
            System.out.println("最终状态 = " + status);

            final ArchivedExecutionGraph execGraph =
                    miniCluster.getArchivedExecutionGraph(jobId).get();
            printExecutionGraph(execGraph);
        } finally {
            miniCluster.closeAsync().get();
        }
    }

    // ================================================================
    // 打印 StreamGraph
    // ================================================================
    private static void printStreamGraph(StreamGraph graph) {
        banner("StreamGraph（逻辑图）");
        System.out.printf(
                "算子节点数 = %d，Source = %s，Sink = %s，并行度=%d%n",
                graph.getStreamNodes().size(),
                graph.getSourceIDs(),
                graph.getSinkIDs(),
                graph.getStreamNodes().size() > 0
                        ? graph.getStreamNodes().iterator().next().getParallelism()
                        : -1);

        System.out.println("\n-- 节点（StreamNode）--");
        for (StreamNode node : graph.getStreamNodes()) {
            System.out.printf(
                    "  id=%-3d %-14s 并行度=%d  in=%-2d out=%-2d  slotSharingGroup=%s%n",
                    node.getId(),
                    node.getOperatorName(),
                    node.getParallelism(),
                    node.getInEdges().size(),
                    node.getOutEdges().size(),
                    node.getSlotSharingGroup());
        }

        System.out.println("\n-- 边（StreamEdge）--");
        for (StreamNode node : graph.getStreamNodes()) {
            for (StreamEdge edge : node.getOutEdges()) {
                System.out.printf(
                        "  %d(%s) --[%s]--> %d%n",
                        edge.getSourceId(),
                        node.getOperatorName(),
                        edge.getPartitioner().getClass().getSimpleName(),
                        edge.getTargetId());
            }
        }
        System.out.println("\n-- JSON 计划（getStreamingPlanAsJSON，前 600 字符）--");
        System.out.println(truncate(graph.getStreamingPlanAsJSON(), 600));
    }

    // ================================================================
    // 打印 JobGraph
    // ================================================================
    private static void printJobGraph(JobGraph graph) {
        banner("JobGraph（物理图 / 提交信封）");
        System.out.printf("JobVertex 数量 = %d%n", graph.getVerticesAsArray().length);

        System.out.println("\n-- 顶点（JobVertex，已合并算子链）--");
        for (JobVertex v : graph.getVerticesSortedTopologicallyFromSources()) {
            System.out.printf(
                    "  %-38s 并行度=%d  max并行度=%d  是Source=%s  内含算子数=%d%n",
                    v.getName(),
                    v.getParallelism(),
                    v.getMaxParallelism(),
                    v.isInputVertex(),
                    v.getOperatorIDs().size());
            System.out.printf("      JobVertexID = %s%n", v.getID());
            System.out.printf("      产出中间结果 = %d 个%n", v.getProducedDataSets().size());
        }

        System.out.println("\n-- 边（JobEdge，只存在于断链处）--");
        for (JobVertex v : graph.getVerticesSortedTopologicallyFromSources()) {
            for (JobEdge e : v.getInputs()) {
                System.out.printf(
                        "  %s(产出集) --> %s%n",
                        e.getSource().getProducer().getName(), v.getName());
            }
        }

        System.out.println("\n-- 提交信封里挂了什么 --");
        System.out.printf("  userJars      = %s%n", graph.getUserJars());
        System.out.printf("  classpaths    = %s%n", graph.getClasspaths());
        System.out.printf("  savepoint恢复  = %s%n", graph.getSavepointRestoreSettings());
        System.out.printf("  jobConfiguration 条目数 = %d%n", graph.getJobConfiguration().toMap().size());

        System.out.println("\n-- JSON 计划（JsonPlanGenerator.generatePlan(JobGraph)，前 600 字符）--");
        System.out.println(truncate(JsonPlanGenerator.generatePlan(graph), 600));
    }

    // ================================================================
    // 打印 ExecutionGraph
    // ================================================================
    private static void printExecutionGraph(ArchivedExecutionGraph graph) {
        banner("ExecutionGraph（执行图，来自 JobManager）");
        System.out.printf("作业 = %s (%s)  状态 = %s%n", graph.getJobName(), graph.getJobID(), graph.getState());

        System.out.println("\n-- ExecutionJobVertex（每个 JobVertex 一个）--");
        for (AccessExecutionJobVertex ejv : graph.getVerticesTopologically()) {
            System.out.printf(
                    "  %-38s 并行度=%d  max并行度=%d  聚合状态=%s  子任务数=%d%n",
                    ejv.getName(),
                    ejv.getParallelism(),
                    ejv.getMaxParallelism(),
                    ejv.getAggregateState(),
                    ejv.getTaskVertices().length);
        }

        System.out.println("\n-- ExecutionVertex（按并行度展开！这是与 JobGraph 最大的差别）--");
        for (AccessExecutionJobVertex ejv : graph.getVerticesTopologically()) {
            for (AccessExecutionVertex ev : ejv.getTaskVertices()) {
                System.out.printf(
                        "  %-52s 子任务序号=%d  当前 attempt=%d  状态=%s%n",
                        ev.getTaskNameWithSubtaskIndex(),
                        ev.getParallelSubtaskIndex(),
                        ev.getCurrentExecutionAttempt().getAttemptNumber(),
                        ev.getCurrentExecutionAttempt().getState());
            }
        }
    }

    // ================================================================
    private static void banner(String title) {
        System.out.println("\n");
        System.out.println("=".repeat(78));
        System.out.println("  " + title);
        System.out.println("=".repeat(78));
    }

    private static String truncate(String s, int n) {
        return s.length() <= n ? s : s.substring(0, n) + "\n  ...(已截断，共 " + s.length() + " 字符)";
    }
}
