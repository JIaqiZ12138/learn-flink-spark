package com.jiaqiz.flink.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 演示 DataStream 三个最基础的转换函数：{@link MapFunction} / {@link FlatMapFunction} /
 * {@link FilterFunction}，每种都用「lambda」与「匿名类或具名静态内部类」两种写法写一遍。
 *
 * <p>运行命令：
 *
 * <pre>
 *   mvn -o -pl learn-flink exec:exec -Dmain.class=com.jiaqiz.flink.operators.BasicFunctionsDemo
 * </pre>
 *
 * <p>三个算子的语义差异（记住这张表，后面所有算子都是它的变体）：
 *
 * <pre>
 *   map      1 : 1   一条输入 → 恰好一条输出（返回值就是输出）
 *   flatMap  1 : N   一条输入 → 0..N 条输出（通过 Collector 手动 emit，可以一条都不 emit）
 *   filter   N : M   一条输入 → 0 或 1 条输出（返回值是 boolean，false 就等于"静默丢弃"）
 * </pre>
 */
public class BasicFunctionsDemo {

    /** 具名静态内部类写法：把一行文本映射成它的长度。 */
    public static class LineLengthMapper implements MapFunction<String, Integer> {

        @Override
        public Integer map(String value) {
            return value.length();
        }
    }

    /** 具名静态内部类写法：一行拆成多个单词，每个单词发射一条 (word, 1)，即经典的 1:N。 */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String word : value.split("\\s+")) {
                if (!word.isEmpty()) {
                    // 一个输入对应循环里 N 次 collect —— 这就是 flatMap 的"一对多"
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }
    }

    /** 具名静态内部类写法：返回 boolean，false 的元素不会出现在下游。 */
    public static class LongWordFilter implements FilterFunction<String> {

        @Override
        public boolean filter(String value) {
            return value.length() > 4;
        }
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设为 1：下面的 print() 输出顺序才稳定，便于逐行核对结果
        env.setParallelism(1);

        final DataStream<String> lines =
                env.fromData("hello flink world", "flink spark flink", "big data");

        // ============================================================
        // 1) map —— 一对一
        // ============================================================

        // 写法 A：lambda。参数类型可以省略（javac 从 MapFunction<String, Integer> 推断出来），
        //         返回类型是 Integer（非泛型），Flink 能自动推断，不需要 returns(...)。
        final DataStream<Integer> lengthsByLambda = lines.map(line -> line.length());

        // 写法 B：匿名内部类。等价于上面，但把类型参数写死在 new MapFunction<String, Integer> 里，
        //         可读性差一点，好处是能塞入多行逻辑与断点。
        final DataStream<Integer> lengthsByAnon =
                lines.map(
                        new MapFunction<String, Integer>() {
                            @Override
                            public Integer map(String value) {
                                return value.length();
                            }
                        });

        // 写法 C：具名静态内部类。类型签名写在 class 声明上，可被 TypeExtractor 反射读到，
        //         所以同样不需要 returns(...)。生产代码推荐这种（可复用、可单测）。
        final DataStream<Integer> lengthsByName = lines.map(new LineLengthMapper());

        System.out.println("=== 1) map：三种写法输出的行长度 ===");
        lengthsByLambda.map(len -> "lambda  -> " + len).print();
        lengthsByAnon.map(len -> "匿名类  -> " + len).print();
        lengthsByName.map(len -> "具名类  -> " + len).print();

        // ============================================================
        // 2) flatMap —— 一对多
        // ============================================================

        // 写法 A：lambda。注意这里必须 .returns(Types.TUPLE(Types.STRING, Types.INT))，原因见文件末尾 5)。
        final DataStream<Tuple2<String, Integer>> wordsByLambda =
                lines.flatMap(
                                (String line, Collector<Tuple2<String, Integer>> out) -> {
                                    for (String word : line.split("\\s+")) {
                                        out.collect(Tuple2.of(word, 1));
                                    }
                                })
                        .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 写法 B：匿名内部类
        final DataStream<Tuple2<String, Integer>> wordsByAnon =
                lines.flatMap(
                        new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                                for (String word : value.split("\\s+")) {
                                    out.collect(Tuple2.of(word, 1));
                                }
                            }
                        });

        // 写法 C：具名静态内部类
        final DataStream<Tuple2<String, Integer>> wordsByName = lines.flatMap(new Tokenizer());

        // 三种写法语义完全一致，词数必须相同
        System.out.println("=== 2) flatMap：三种写法的一对多结果（流式 WordCount） ===");
        wordsByLambda
                .keyBy(word -> word.f0)
                .sum(1)
                .map(t -> "lambda  -> " + t)
                .print("flatMap-lambda");
        wordsByAnon.keyBy(word -> word.f0).sum(1).map(t -> "匿名类  -> " + t).print("flatMap-anon");
        wordsByName.keyBy(word -> word.f0).sum(1).map(t -> "具名类  -> " + t).print("flatMap-named");

        // ============================================================
        // 3) filter —— 返回 boolean
        // ============================================================

        // 先把上面的 (word,1) 还原成纯单词流，方便演示 filter
        final DataStream<String> words =
                wordsByName.map(tuple -> tuple.f0).returns(Types.STRING);

        // 写法 A：lambda
        final DataStream<String> longWordsByLambda = words.filter(word -> word.length() > 4);

        // 写法 B：匿名内部类
        final DataStream<String> longWordsByAnon =
                words.filter(
                        new FilterFunction<String>() {
                            @Override
                            public boolean filter(String value) {
                                return value.length() > 4;
                            }
                        });

        // 写法 C：具名静态内部类
        final DataStream<String> longWordsByName = words.filter(new LongWordFilter());

        System.out.println("=== 3) filter：三种写法都只留下长度 > 4 的单词 ===");
        longWordsByLambda.map(w -> "lambda  -> " + w).print();
        longWordsByAnon.map(w -> "匿名类  -> " + w).print();
        longWordsByName.map(w -> "具名类  -> " + w).print();

        // ============================================================
        // 4) 「用 Collector 但可能不发射」——filter 本身没有 Collector
        // ============================================================

        // 澄清一个常见误解：FilterFunction 的签名是 boolean filter(T value)，它**没有** Collector，
        // 只能回答"留 / 不留"。真正能由用户决定"发射几条（可以是 0 条）"的算子是 flatMap。
        // 下面这个 flatMap 一条都不 emit 时，效果就等价于 filter —— 但控制权完全在 Collector 手里，
        // 因此它还能做"一条输入拆成 0 条 / 1 条 / 多条"的动态决定。
        final DataStream<String> longWordsAsFlatMap =
                words.flatMap(
                                (String word, Collector<String> out) -> {
                                    if (word.length() > 4) {
                                        out.collect(word); // 命中条件才发射
                                    }
                                    // 不满足条件时一次都不调用 out.collect() —— 该输入被"过滤"掉了
                                })
                        .returns(Types.STRING);

        System.out.println("=== 4) flatMap 当 filter 用（Collector 可以一次都不发射） ===");
        longWordsAsFlatMap.map(w -> "flatMap-as-filter -> " + w).print();

        // ============================================================
        // 5) 类型推断：什么时候必须写 returns(...)
        // ============================================================

        // Flink 需要 TypeInformation 来决定序列化器（网络传输、状态、Comparator 都依赖它）。
        // JVM 会擦除 lambda 的泛型信息，所以只要 lambda 的返回类型"带泛型参数"
        // （Tuple2<K,V>、List<T>、Map<K,V> ...），Flink 就无从得知 K/V 到底是什么。

        // 实测结论（Flink 1.20.4，本工程已验证）：漏写 returns(...) 并不会"退化成 GenericType 继续跑"，
        // 而是在 flatMap(...) 这一次调用当场抛出：
        //   org.apache.flink.api.common.functions.InvalidTypesException:
        //     The return type of function 'xxx' could not be determined automatically,
        //     due to type erasure. You can give type information hints by using the returns(...)
        //     method on the result of the transformation call, or by letting your function
        //     implement the 'ResultTypeQueryable' interface.
        // 所以 lambda + 泛型返回值 = 必须显式 returns(...)。
        System.out.println("=== 5) 类型推断 ===");
        System.out.println("wordsByLambda(显式 returns TUPLE) 的 TypeInformation = " + wordsByLambda.getType());
        // 对照：具名类把 Tuple2<String, Integer> 写在了 implements 子句里，TypeExtractor 能反射读到，
        // 因此不写 returns 也拿得到一模一样的 TupleTypeInfo。
        System.out.println("wordsByName(未写 returns)      的 TypeInformation = " + wordsByName.getType());
        System.out.println("环境实际并行度 = " + env.getParallelism());

        env.execute("BasicFunctionsDemo");
    }
}
