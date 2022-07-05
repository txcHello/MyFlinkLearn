package bigdata.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Administrator
 * @Date 2022/7/4 14:00
 * @Version 1.0
 * Desc:
 *
 * DataStream → DataStream #
 * 输入一个元素同时输出一个元素。下面是将输入流中元素数值加倍的 map function：
 */
public class Map_Transform {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 8888);
        SingleOutputStreamOperator<Long> map = socketTextStream.map(new MapFunction<String, Long>() {
            @Override
            public Long map(String value) throws Exception {
                return Long.parseLong(value) * 2;
            }
        });

        map.print();
        env.execute();
    }
}
