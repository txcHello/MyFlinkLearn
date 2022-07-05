package bigdata.transform;

import bigdata.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author Administrator
 * @Date 2022/7/4 14:00
 * @Version 1.0
 * Desc:
 * Connect #
 * DataStream,DataStream → ConnectedStream #
 * “连接” 两个数据流并保留各自的类型。connect 允许在两个流的处理逻辑之间共享状态。
 */
public class Connect_Transform {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> ds1 = env.fromElements(
                new Event("zhangsan", "/url", 1656997370000l)
              //  new Event("lisi", "/url", 1656997371000l),
               // new Event("wangwu", "/url", 1656997372000l),
               // new Event("maliu", "/url", 1656997373000l),
               // new Event("zhangsan", "/url", 1656997374000l)
        );

        DataStreamSource<Event> ds2 = env.fromElements(
                new Event("zhangsan", "/url", 1656997370000l),
               // new Event("jobs", "/ii", 1656997371000l),
             //   new Event("lisi", "/app", 1656997372000l),
                new Event("zhangsan", "/url", 1656997373000l),
                new Event("zhangsan", "/app", 1656997374000l)
        );

        ConnectedStreams<Event, Event> connect = ds1.connect(ds2);

        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Event, Event, String>() {
            @Override
            public String map1(Event value) throws Exception {
                return value.user + "first";
            }

            @Override
            public String map2(Event value) throws Exception {
                return value.user + "second";
            }
        });
        map.print();
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = connect.flatMap(new CoFlatMapFunction<Event, Event, String>() {
            @Override
            public void flatMap1(Event value, Collector<String> out) throws Exception {

            }

            @Override
            public void flatMap2(Event value, Collector<String> out) throws Exception {

            }
        });

        env.execute();
    }
}
