package com.jiaqiz.flink.source;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * <b>原生 JDBC + SQLite</b> 的 Source / Sink 演示（不依赖任何 Flink JDBC 连接器）。
 *
 * <p>运行方式：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.source.JdbcSourceSinkDemo
 * </pre>
 *
 * <h2>做了什么</h2>
 *
 * <ol>
 *   <li><b>Sink 作业</b>：{@link SqliteSinkFunction}（{@code RichSinkFunction<String>}）
 *       在 {@code open()} 里 {@code Class.forName("org.sqlite.JDBC")} + {@code DriverManager.getConnection(...)}
 *       + {@code CREATE TABLE IF NOT EXISTS}；{@code invoke()} 里用 {@link PreparedStatement}
 *       {@code addBatch()} 攒批、每 3 条 {@code executeBatch()}；{@code close()} 里
 *       把最后一批刷完、{@code commit()}、关连接。用 {@code env.execute()} 跑完。
 *   <li><b>Source 作业</b>：{@link SqliteSourceFunction}（{@code RichSourceFunction<String>}）
 *       在 {@code run()} 里 {@code SELECT} 全表，逐行 {@code ctx.collect(...)} 发给下游
 *       并 {@code print()}，跑完打印行数。用第二个 {@code env.execute()} 跑。
 *   <li>两个作业串起来就验证了「写入 → 读回」闭环；最后 main 里再直接查一次
 *       {@code SELECT COUNT(*)} 做交叉校验。
 * </ol>
 *
 * <p>数据库文件放在 {@code /tmp/flink-jdbc-demo.db}，<b>每次运行前会先删掉旧文件</b>
 * （含 {@code -journal} / {@code -wal} / {@code -shm}），保证可重复运行。
 *
 * <h2>关于官方 {@code flink-connector-jdbc}</h2>
 *
 * <p>Flink 1.20 对应的官方 JDBC 连接器是 <b>3.3.0-1.20</b>，它给出的是：
 *
 * <pre>{@code
 * // ---- Sink ----
 * JdbcSink.sink(
 *     "INSERT INTO demo_records (content, created_at) VALUES (?, ?)",
 *     (JdbcStatementBuilder<String>) (ps, value, ctx) -> {
 *         ps.setString(1, value);
 *         ps.setLong(2, System.currentTimeMillis());
 *     },
 *     JdbcExecutionOptions.builder()
 *         .withBatchSize(1000)
 *         .withBatchIntervalMs(200)
 *         .withMaxRetries(3)
 *         .build(),
 *     new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
 *         .withUrl("jdbc:mysql://localhost:3306/demo")
 *         .withDriverName("com.mysql.cj.jdbc.Driver")
 *         .withUsername("root")
 *         .withPassword("root")
 *         .build());
 * stream.sinkTo(jdbcSink);   // 注意：官方 JdbcSink 返回的是 Sink V2，用 sinkTo
 * }</pre>
 *
 * <p>本机 Maven 仓库里<b>没有</b> 3.3.0-1.20（无外网，仓库里只有 3.1.1-1.17），
 * 所以本工程没有把它纳入构建，而是手写上面这两个 Function 来把 JDBC 的机制讲清楚。
 * 官方连接器内部做的事（攒批、重试、连接管理、方言）与手工版本是同一套思路，
 * 区别只是它把这些封装好了、并支持 exactly-once（配合 XA / 幂等写入）。
 *
 * <p>关于 {@code JdbcSource}：本机缓存的 3.1.1-1.17 里<b>只有</b> {@code JdbcSink}，
 * 没有 {@code JdbcSource}；官方在后续版本（3.2+）才加入了
 * {@code org.apache.flink.connector.jdbc.source.JdbcSource}。它的能力边界
 * （是否支持增量/断点续读、哪些 dialect 支持、是否只适合 bounded 场景）
 * <b>以官方文档为准</b>；需要真正的「数据库变更流」时，通常仍然选 CDC 连接器。
 */
public class JdbcSourceSinkDemo {

    /** SQLite 数据库文件（放 /tmp 下，便于反复清理）。 */
    private static final String DB_FILE = "/tmp/flink-jdbc-demo.db";

    /** JDBC URL 形如 jdbc:sqlite:/tmp/flink-jdbc-demo.db。 */
    private static final String JDBC_URL = "jdbc:sqlite:" + DB_FILE;

    /** 要写入的记录。 */
    private static final List<String> RECORDS =
            Arrays.asList("hello-flink", "hello-spark", "hello-jdbc", "hello-sqlite", "hello-sink-v2");

    public static void main(String[] args) throws Exception {

        // 0. 清理旧的数据库文件，保证可重复运行
        cleanupDbFiles();

        // ---------------------------------------------------------------- 作业 1：写入
        System.out.println("[main] ===== 作业 1：JDBC Sink 写入 " + JDBC_URL + " =====");
        final StreamExecutionEnvironment sinkEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        sinkEnv.setParallelism(1);

        sinkEnv
                .fromData(RECORDS.toArray(new String[0]))
                .addSink(new SqliteSinkFunction(JDBC_URL))
                .name("sqlite-sink");

        sinkEnv.execute("learn-flink JDBC sink demo");
        System.out.println(
                "[main] 作业 1 结束：env.execute() 返回意味着 open/invoke/close 已全部走完，"
                        + "数据已经 commit 落盘");

        // ---------------------------------------------------------------- 作业 2：读回
        System.out.println("[main] ===== 作业 2：JDBC Source 从 " + JDBC_URL + " 读回 =====");
        final StreamExecutionEnvironment sourceEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        sourceEnv.setParallelism(1);

        // addSource(...) 返回 DataStreamSource；链式 .name(...) 之后是 SingleOutputStreamOperator。
        sourceEnv
                .addSource(new SqliteSourceFunction(JDBC_URL))
                .name("sqlite-source")
                .print()
                .name("print-sink");

        sourceEnv.execute("learn-flink JDBC source demo");

        // ---------------------------------------------------------------- 交叉校验
        final long count = countRowsDirectly();
        System.out.println("[main] 直接查库 SELECT COUNT(*) = " + count);
        if (count == RECORDS.size()) {
            System.out.println(
                    "[main] 闭环校验 OK：写入 " + RECORDS.size() + " 条，读回 " + count + " 条");
        } else {
            throw new IllegalStateException(
                    "闭环校验失败：期望 " + RECORDS.size() + " 条，实际 " + count + " 条");
        }
    }

    // ==================================================================================
    //  Sink：RichSinkFunction<String>
    // ==================================================================================

    /**
     * 用 PreparedStatement 批处理写 SQLite。
     *
     * <p>生命周期：{@code open()} 建连接建表 → {@code invoke()} 攒批 → {@code close()} 刷尾批 + commit + 关连接。
     * 并行度必须为 1 吗？不必，但 SQLite 是单文件库、并发写会锁表，所以本 demo 用并行度 1，
     * 并加 {@code busy_timeout} 让并发场景下不至于立刻 SQLITE_BUSY。
     */
    public static class SqliteSinkFunction extends RichSinkFunction<String> {

        private static final long serialVersionUID = 1L;

        private static final int BATCH_SIZE = 3;

        private static final String CREATE_TABLE =
                "CREATE TABLE IF NOT EXISTS demo_records ("
                        + "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
                        + "  content TEXT NOT NULL,"
                        + "  created_at INTEGER NOT NULL)";

        private static final String INSERT_SQL =
                "INSERT INTO demo_records (content, created_at) VALUES (?, ?)";

        private final String jdbcUrl;

        // 运行时资源：transient，不参与序列化，只在 TaskManager 上创建
        private transient Connection connection;
        private transient PreparedStatement insertStatement;
        private transient int pendingInBatch;
        private transient int totalInserted;

        public SqliteSinkFunction(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);

            // 1) 显式加载驱动。JDBC 4.0 起靠 SPI 也能自动加载，
            //    但生产代码里显式 Class.forName 更直观、也能尽早暴露缺驱动的问题。
            Class.forName("org.sqlite.JDBC");

            // 2) 建连接
            connection = DriverManager.getConnection(jdbcUrl);

            // 3) 手动事务：攒批期间不自动提交，close() 里一次性 commit
            connection.setAutoCommit(false);

            // 4) 建表（幂等）
            try (Statement statement = connection.createStatement()) {
                statement.execute(CREATE_TABLE);
                // SQLite 是库级写锁，给点等待时间，避免并发写直接报 database is locked
                statement.execute("PRAGMA busy_timeout = 3000");
            }

            insertStatement = connection.prepareStatement(INSERT_SQL);
            pendingInBatch = 0;
            totalInserted = 0;

            System.out.println(
                    "[jdbc-sink subtask#"
                            + getRuntimeContext().getIndexOfThisSubtask()
                            + "] open(): 驱动已加载、连接已建立、表 demo_records 就绪");
        }

        /** 1.20 里已废弃，留空实现只为满足抽象方法。 */
        @Override
        public void open(Configuration parameters) throws Exception {
            // no-op：真正的初始化在 open(OpenContext) 里
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            insertStatement.setString(1, value);
            insertStatement.setLong(2, System.currentTimeMillis());
            insertStatement.addBatch();
            pendingInBatch++;
            totalInserted++;

            if (pendingInBatch >= BATCH_SIZE) {
                flushBatch();
            }
        }

        /** 把攒下的这批一次性发给数据库。 */
        private void flushBatch() throws SQLException {
            if (pendingInBatch == 0) {
                return;
            }
            final int[] affected = insertStatement.executeBatch();
            System.out.println(
                    "[jdbc-sink] executeBatch() 提交 "
                            + affected.length
                            + " 条（累计 "
                            + totalInserted
                            + " 条）");
            pendingInBatch = 0;
        }

        @Override
        public void close() throws Exception {
            try {
                // 尾巴上不足 BATCH_SIZE 的那批不能丢
                flushBatch();
                connection.commit();
                System.out.println(
                        "[jdbc-sink] close(): 尾批已刷 + commit()，本次共写入 " + totalInserted + " 条");
            } finally {
                if (insertStatement != null) {
                    insertStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
            super.close();
        }
    }

    // ==================================================================================
    //  Source：RichSourceFunction<String>
    // ==================================================================================

    /**
     * 全表扫描并逐行发给下游。这是一个 bounded 的旧式 source：
     * {@code run()} 返回即代表「读完了」，SourceStreamTask 会由此结束作业。
     *
     * <p>（FLIP-27 版本的做法见 {@link CustomSourceDemo}：把「查哪一段」抽象成 split，
     * 这样才有分片并行、断点续读。这里刻意保持最朴素的写法。）
     */
    public static class SqliteSourceFunction extends RichSourceFunction<String> {

        private static final long serialVersionUID = 1L;

        private static final String SELECT_SQL =
                "SELECT id, content, created_at FROM demo_records ORDER BY id";

        private final String jdbcUrl;

        /** volatile：cancel() 由另一个线程调用。 */
        private volatile boolean running = true;

        public SqliteSourceFunction(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            Class.forName("org.sqlite.JDBC");
            System.out.println("[jdbc-source] open(): sqlite 驱动已加载");
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // no-op
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int rows = 0;
            try (Connection connection = DriverManager.getConnection(jdbcUrl);
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(SELECT_SQL)) {

                while (running && rs.next()) {
                    final long id = rs.getLong("id");
                    final String content = rs.getString("content");
                    final long createdAt = rs.getLong("created_at");
                    // 旧 API：collect 需要和 checkpoint 对齐
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(
                                "row{id=" + id + ", content=" + content + ", created_at=" + createdAt + "}");
                    }
                    rows++;
                }
            }
            System.out.println("[jdbc-source] run(): SELECT 结束，共读回 " + rows + " 行");
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // ==================================================================================
    //  工具
    // ==================================================================================

    /** 删掉旧库文件和 SQLite 可能留下的边角文件，保证 demo 可重复运行。 */
    private static void cleanupDbFiles() throws IOException {
        final String[] suffixes = {"", "-journal", "-wal", "-shm"};
        for (String suffix : suffixes) {
            final Path path = Paths.get(DB_FILE + suffix);
            if (Files.deleteIfExists(path)) {
                System.out.println("[main] 已删除旧文件 " + path);
            }
        }
    }

    /** main 里直接查一次库，和 Flink 作业读回的结果做交叉校验。 */
    private static long countRowsDirectly() throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM demo_records")) {
            return rs.next() ? rs.getLong(1) : -1L;
        }
    }
}
