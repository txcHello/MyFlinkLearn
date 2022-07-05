package bigdata.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Administrator
 * @Date 2022/7/4 15:50
 * @Version 1.0
 * Desc:
 *
 *
 */
public class KeyedDataStream_Demo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 8888);
        SingleOutputStreamOperator<WC> flatMap = socketTextStream.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String value, Collector<WC> out) throws Exception {
                System.out.println(value);
                String[] split = value.split(",");

                for (String word : split) {
                    System.out.println(word);

                    out.collect( new WC(word,1));
                }
            }
        });

        KeyedStream<WC, String> tuple2StringKeyedStream = flatMap.keyBy(new KeySelector<WC, String>() {
            @Override
            public String getKey(WC value) throws Exception {
                return value.getWord();
            }
        });

        tuple2StringKeyedStream.print();

        env.execute();
    }
}
