package bigdata.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Administrator
 * @Date 2022/7/3 19:14
 * @Version 1.0
 * Desc:
 *   Iterate
 *  DataStream → IterativeStream → ConnectedStream #
 * 通过将一个算子的输出重定向到某个之前的算子来在流中创建“反馈”循环。这对于定义持续更新模型的算法特别有用。下面的代码从一个流开始，并不断地应用迭代自身。大于 0 的元素被发送回反馈通道，其余元素被转发到下游。
 *
 */
public class ItetationsDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Long> longDataStreamSource = env.generateSequence(0, 10);
        longDataStreamSource.print("source");
        //进行循环迭代
        //1次迭代   1，2，3，4，5，6，7，8，9，10
        //2次  2，3，4，5，6，7
        IterativeStream<Long> iterate = longDataStreamSource.iterate();
        SingleOutputStreamOperator<Long> minusOne = iterate.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value -1;
            }
        });
        //iterate.print("iterate");
        minusOne.print("minusOne");
        SingleOutputStreamOperator<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });
        //关闭迭代并定义迭代尾部
          iterate.closeWith(stillGreaterThanZero);
        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value <= 0);
            }
        });
        env .execute();
    }
}
