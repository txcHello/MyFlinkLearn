package bigdata.transform;

import bigdata.bean.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
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
 */
public class PhysicalPartition_ShuffleDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> ds2 = env.fromElements(new Event("zhangsan", "/url", 1656997370000l),
                new Event("jobs", "/ii", 1656997371000l),
                new Event("lisi", "/app", 1656997372000l),
                new Event("zhangsan", "/url", 1656997373000l),
                new Event("zhangsan", "/app", 1656997374000l)
        );

        ds2.print("ds2");
        DataStream<Event> shuffle = ds2.shuffle();
        shuffle.print().setParallelism(4);
        env.execute();
    }
}
