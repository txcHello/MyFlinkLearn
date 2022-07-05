package bigdata.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Administrator
 * @Date 2022/7/4 14:00
 * @Version 1.0
 * Desc:
 *
 *  DataStream → DataStream #
 *  输入一个元素同时产生零个、一个或多个元素。下面是将句子拆分为单词的 flatmap function：
 */
public class FlatMap_Transform {

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
        });

        flatMap.print();
        env.execute();
    }
}
