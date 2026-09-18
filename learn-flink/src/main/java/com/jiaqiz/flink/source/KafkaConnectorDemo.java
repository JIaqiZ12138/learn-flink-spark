package com.jiaqiz.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Properties;

/**
 * 官方 Kafka 连接器（{@code org.apache.flink:flink-connector-kafka:3.3.0-1.20}）。
 *
 * <p><b>本 demo 默认只做「编译验证」</b>：本机没有 Kafka broker，所以 main 里只打印提示信息并
 * exit 0，<b>不会</b>调用 {@code env.execute()}。
 *
 * <p>运行方式：
 *
 * <pre>
 *   # 只做编译验证 / 打印前置条件（不需要 broker，退出码 0）
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.source.KafkaConnectorDemo
 *
 *   # 真要跑，必须带上任意一个参数（args.length &gt; 0），并且先满足下面的前置条件
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.source.KafkaConnectorDemo -Dexec.args="run"
 * </pre>
 *
 * <h2>运行前置条件（缺一不可）</h2>
 *
 * <ul>
 *   <li>一个可达的 Kafka 集群，且 {@code localhost:9092} 上有名为 {@code learn-flink-input}
 *       / {@code learn-flink-output} 的 topic（或改本类的常量）。
 *   <li>Kafka 版本 ≥ 2.8 且 <b>开启事务</b>（EXACTLY_ONCE 依赖事务）；broker 的
 *       {@code transaction.max.timeout.ms} 必须大于下面设置的 {@code transaction.timeout.ms}。
 *   <li>作业必须开启 checkpoint（{@code env.enableCheckpointing(...)}），否则
 *       EXACTLY_ONCE 的 KafkaSink 会直接在构建/运行期报错 —— 两阶段提交是挂在
 *       checkpoint 上的。
 *   <li>consumer 能读、producer 能写对应的 topic（ACL）。
 * </ul>
 *
 * <h2>包路径核实结果（用 unzip -l 在本地 jar 里核对过）</h2>
 *
 * <ul>
 *   <li>{@code org.apache.flink.connector.kafka.source.KafkaSource} +
 *       {@code KafkaSourceBuilder}
 *   <li>{@code org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer}
 *   <li>{@code org.apache.flink.connector.kafka.sink.KafkaSink} + {@code KafkaSinkBuilder}
 *   <li>{@code org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema} +
 *       {@code KafkaRecordSerializationSchemaBuilder}
 *   <li>{@code org.apache.flink.connector.base.DeliveryGuarantee} —— 注意这个枚举
 *       <b>不在</b> kafka 连接器 jar 里，而在 {@code flink-connector-base} 里。
 *   <li>该连接器把 {@code flink-streaming-java} / {@code flink-connector-base} 声明为
 *       {@code provided}，所以本工程的 pom 必须显式引入 {@code flink-connector-base}
 *       才能编译（见 learn-flink/pom.xml 注释）。
 * </ul>
 */
public class KafkaConnectorDemo {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "learn-flink-input";
    private static final String OUTPUT_TOPIC = "learn-flink-output";
    private static final String GROUP_ID = "learn-flink-kafka-demo";
    private static final String TRANSACTIONAL_ID_PREFIX = "learn-flink-kafka-sink-";

    public static void main(String[] args) throws Exception {

        System.out.println("========================================================");
        System.out.println(" KafkaConnectorDemo —— 只做编译验证，需要 Kafka broker 才能运行");
        System.out.println("========================================================");
        System.out.println(" 期望的 broker      : " + BOOTSTRAP_SERVERS);
        System.out.println(" 输入 topic         : " + INPUT_TOPIC);
        System.out.println(" 输出 topic         : " + OUTPUT_TOPIC);
        System.out.println(" consumer group     : " + GROUP_ID);
        System.out.println(" 事务前缀           : " + TRANSACTIONAL_ID_PREFIX);
        System.out.println(" 前置条件：");
        System.out.println("   1) 一个可达的 Kafka 集群（bootstrap.servers=" + BOOTSTRAP_SERVERS + "）");
        System.out.println("   2) 上面两个 topic 已创建，且当前账号有读写权限");
        System.out.println("   3) broker 版本 >= 2.8 且开启事务（EXACTLY_ONCE 走两阶段提交）");
        System.out.println("   4) broker 的 transaction.max.timeout.ms > 本 demo 的 transaction.timeout.ms(900000)");
        System.out.println("   5) 作业必须 enableCheckpointing(...)，否则 KafkaSink 的 EXACTLY_ONCE 不可用");
        System.out.println("--------------------------------------------------------");

        if (args.length == 0) {
            System.out.println("[main] 未传入任何参数 -> 跳过真实执行（不调用 env.execute()），exit 0");
            System.out.println("[main] 想真跑：给 main 传任意参数，例如 -Dexec.args=\"run\"");
            System.out.println("[main] 编译验证通过：KafkaSource / KafkaSink 的 builder 链已全部解析到本地 jar 中的方法");
            return;
        }

        System.out.println("[main] 收到参数 " + java.util.Arrays.toString(args) + " -> 尝试连接真实 broker");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // EXACTLY_ONCE 的两阶段提交依赖 checkpoint，必须打开。
        env.enableCheckpointing(10_000L);

        final KafkaSource<String> kafkaSource = buildKafkaSource();
        final KafkaSink<String> kafkaSink = buildKafkaSink();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(value -> "reprocessed: " + value)
                .name("reprocess")
                .sinkTo(kafkaSink)
                .name("kafka-sink");

        env.execute("learn-flink kafka connector demo");
    }

    // ==================================================================================
    //  KafkaSource：完整的消费端配置
    // ==================================================================================

    /**
     * KafkaSource 是 FLIP-27 Source：每个 Kafka 分区被映射成一个 split，
     * 由 JobManager 上的 {@code KafkaSourceEnumerator} 动态发现并分配；
     * reader 侧是 {@code KafkaSourceReader}，用 {@code snapshotState} 上报各分区的消费位点。
     */
    public static KafkaSource<String> buildKafkaSource() {

        final Properties consumerProps = new Properties();
        // 关闭自动提交：位点由 Flink 的 checkpoint 统一管理，才能做到精确一次/至少一次
        consumerProps.setProperty("enable.auto.commit", "false");
        // 动态分区发现：每 60s 重新拉一次元数据，新分区会被自动识别
        consumerProps.setProperty("partition.discovery.interval.ms", "60000");
        consumerProps.setProperty("session.timeout.ms", "30000");
        consumerProps.setProperty("max.poll.records", "500");

        return KafkaSource.<String>builder()
                // 必填：broker 地址
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                // 必填：订阅的 topic（也可用 setTopicPattern / setPartitions）
                .setTopics(INPUT_TOPIC)
                // 消费组：决定位点提交到哪里
                .setGroupId(GROUP_ID)
                // 起始位点：优先用 group 已提交的位点，没有则从头开始。
                // 其它选择：OffsetsInitializer.earliest() / latest() / timestamp(ms) / offsets(map)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                // 反序列化器：值是一个 String。
                // 等价写法：setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 客户端 id 前缀，Kafka 端便于排查
                .setClientIdPrefix("learn-flink-kafka-source")
                // 其余原生 consumer 配置
                .setProperties(consumerProps)
                // 无界：Kafka 是持续消费的（若要用位点边界做批读，改用 setBounded(...)）
                .setUnbounded(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();
    }

    // ==================================================================================
    //  KafkaSink：完整的生产端配置
    // ==================================================================================

    /**
     * KafkaSink 是 Sink V2。EXACTLY_ONCE 下它内部用的是
     * {@code KafkaCommitter} + 事务型 producer：checkpoint 时 pre-commit，
     * checkpoint 完成后再 commit 事务，从而保证「作业失败回滚、恢复后不重不丢」。
     */
    public static KafkaSink<String> buildKafkaSink() {

        // 序列化器：决定 key / value / 发到哪个 topic
        final KafkaRecordSerializationSchema<String> recordSerializer =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(OUTPUT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        // 需要 key 时可以加：.setKeySerializationSchema(new SimpleStringSchema())
                        // 需要按 key 分区时可以加：.setPartitioner(new FlinkFixedPartitioner<>())
                        .build();

        final Properties producerProps = new Properties();
        // 事务超时：必须小于 broker 的 transaction.max.timeout.ms（默认 15 分钟）
        producerProps.setProperty("transaction.timeout.ms", "900000");
        producerProps.setProperty("acks", "all");
        producerProps.setProperty("retries", "3");
        producerProps.setProperty("linger.ms", "20");
        producerProps.setProperty("compression.type", "lz4");

        return KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(recordSerializer)
                // 投递语义：NONE / AT_LEAST_ONCE / EXACTLY_ONCE
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // EXACTLY_ONCE 必填：producer 事务 id 前缀，Flink 会在后面拼 subtaskId
                .setTransactionalIdPrefix(TRANSACTIONAL_ID_PREFIX)
                .setKafkaProducerConfig(producerProps)
                .build();
    }
}
