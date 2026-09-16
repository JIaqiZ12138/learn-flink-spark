package com.learn.spark.sample;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * 最小可运行的 Spark 作业：RDD WordCount + Spark SQL 示例。
 *
 * <p>运行方式（JDK 17 必须带 --add-opens，见 README）：
 *
 * <pre>
 *   export MAVEN_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED ..."
 *   mvn -pl learn-spark exec:java
 * </pre>
 *
 * <p>对照 Flink 的概念映射：
 *
 * <table border="1">
 *   <caption>概念对照</caption>
 *   <tr><th>Flink</th><th>Spark</th></tr>
 *   <tr><td>StreamExecutionEnvironment</td><td>SparkSession / SparkContext</td></tr>
 *   <tr><td>DataStream</td><td>Dataset / RDD</td></tr>
 *   <tr><td>env.execute()</td><td>action（如 collect/count）触发惰性求值</td></tr>
 *   <tr><td>算子链（同一个线程内串行）</td><td>stage 内 pipeline，按宽窄依赖切分</td></tr>
 * </table>
 */
public class WordCountApp {

    public static void main(String[] args) {

        // local[*] 表示用本机所有核心起一个本地 Spark 集群，driver 与 executor 同 JVM。
        final SparkSession spark =
                SparkSession.builder()
                        .appName("learn-spark WordCount")
                        .master("local[*]")
                        .getOrCreate();

        // 降噪：默认 Spark 会打印大量 INFO
        spark.sparkContext().setLogLevel("WARN");

        try {
            rddWordCount(spark);
            sqlWordCount(spark);
        } finally {
            spark.stop();
        }
    }

    /** RDD 版：最贴近底层，适合对照 Flink 的 DataStream API。 */
    private static void rddWordCount(SparkSession spark) {
        final List<String> lines =
                Arrays.asList(
                        "flink spark flink",
                        "spark flink flink",
                        "hello big data",
                        "flink and spark");

        final JavaRDD<String> rdd = new JavaSparkContext(spark.sparkContext()).parallelize(lines);

        // flatMap → mapToPair → reduceByKey
        // reduceByKey 是宽依赖，会触发 shuffle（类比 Flink 的 keyBy 重分区）
        final JavaPairRDD<String, Integer> counts =
                rdd.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                        .filter(word -> !word.isEmpty())
                        .mapToPair(word -> new Tuple2<>(word, 1))
                        .reduceByKey(Integer::sum);

        System.out.println("========== RDD WordCount ==========");
        counts.collect().forEach(t -> System.out.println(t._1() + " -> " + t._2()));
    }

    /** Spark SQL 版：与 Flink SQL 的定位对应。 */
    private static void sqlWordCount(SparkSession spark) {
        final Dataset<Row> df =
                spark.createDataFrame(
                        Arrays.asList(
                                new Word("flink", 3), new Word("spark", 2), new Word("flink", 1)),
                        Word.class);

        df.createOrReplaceTempView("words");

        System.out.println("========== Spark SQL WordCount ==========");
        spark.sql(
                        "SELECT word, SUM(cnt) AS total FROM words GROUP BY word ORDER BY total DESC")
                .show();

        System.out.println("========== DataFrame 物理计划 ==========");
        spark.sql("SELECT word, SUM(cnt) AS total FROM words GROUP BY word").explain(true);
    }

    /** Spark SQL 需要 POJO 有 public getter/setter。 */
    public static class Word {
        private String word;
        private int cnt;

        public Word() {}

        public Word(String word, int cnt) {
            this.word = word;
            this.cnt = cnt;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCnt() {
            return cnt;
        }

        public void setCnt(int cnt) {
            this.cnt = cnt;
        }
    }
}
