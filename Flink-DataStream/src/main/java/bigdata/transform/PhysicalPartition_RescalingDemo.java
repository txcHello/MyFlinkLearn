package bigdata.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Administrator
 * @Date 2022/7/5 15:56
 * @Version 1.0
 * Desc:
 *
 *   Rescaling #
 * DataStream → DataStream #
 * 将元素以 Round-robin 轮询的方式分发到下游算子。如果你想实现数据管道，这将很有用，例如，想将数据源多个并发实例的数据分发到多个下游 map 来实现负载分配，但又不想像 rebalance() 那样引起完全重新平衡。
 * 该算子将只会到本地数据传输而不是网络数据传输，这取决于其它配置值，例如 TaskManager 的 slot 数量。
 *
 * 上游算子将元素发往哪些下游的算子实例集合同时取决于上游和下游算子的并行度。例如，如果上游算子并行度为 2，下游算子的并发度为 6， 那么上游算子的其中一个并行实例将数据分发到下游算子的三个并行实例，
 * 另外一个上游算子的并行实例则将数据分发到下游算子的另外三个并行实例中。再如，当下游算子的并行度为2，而上游算子的并行度为 6 的时候，
 * 那么上游算子中的三个并行实例将会分发数据至下游算子的其中一个并行实例，而另外三个上游算子的并行实例则将数据分发至另下游算子的另外一个并行实例。
 *
 * 当算子的并行度不是彼此的倍数时，一个或多个下游算子将从上游算子获取到不同数量的输入。
 */
public class PhysicalPartition_RescalingDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 8888);
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                System.out.println(value);
                String[] split = value.split(",");

                for (String word : split) {
                    System.out.println(word);

                    out.collect(Tuple2.of(word, 1l));
                }
            }
        }).setParallelism(2);
        DataStream<Tuple2<String, Long>> shuffle = flatMap.rescale();



        shuffle.print();
        env.execute();
    }
}
