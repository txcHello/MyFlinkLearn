package bigdata.transform;

import bigdata.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.omg.CORBA.CTX_RESTRICT_SCOPE;

import java.time.Duration;

/**
 * @Author Administrator
 * @Date 2022/7/4 14:00
 * @Version 1.0
 * Desc:
 *   Interval Join #
 * KeyedStream,KeyedStream → DataStream #
 * 根据 key 相等并且满足指定的时间范围内（e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound）的条件将分别属于两个 keyed stream 的元素 e1 和 e2 Join 在一起。
 * Interval join 目前仅支持 event time。
 *
 * 我们 join 了橙色和绿色两个流，join 的条件是：以 -2 毫秒为下界、+1 毫秒为上界。 默认情况下，上下界也被包括在区间内，但 .lowerBoundExclusive() 和 .upperBoundExclusive() 可以将它们排除在外。
 */
public class IntervalJoin_Transform {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> ds1 = env.fromElements(new Event("zhangsan", "/url", 1656997370000l),
                new Event("lisi", "/url", 1656997371000l),
                new Event("wangwu", "/url", 1656997372000l),
                new Event("maliu", "/url", 1656997373000l),
                new Event("zhangsan", "/url", 1656997374000l)
        );

        DataStreamSource<Event> ds2 = env.fromElements(new Event("zhangsan", "/url", 1656997370000l),
                new Event("jobs", "/ii", 1656997371000l),
                new Event("lisi", "/app", 1656997372000l),
                new Event("zhangsan", "/url", 1656997373000l),
                new Event("zhangsan", "/app", 1656997374000l)
        );

        SingleOutputStreamOperator<Event> eventDs1 = ds1.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));

        SingleOutputStreamOperator<Event> eventDs2 = ds2.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));

        KeyedStream<Event, String> eventStringKeyedStream1 = eventDs1.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        });
        KeyedStream<Event, String> eventStringKeyedStream2 = eventDs2.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        });
 //       eventStringKeyedStream1.print();
        SingleOutputStreamOperator<String> process = eventStringKeyedStream2.intervalJoin(eventStringKeyedStream1).inEventTime().between(Time.milliseconds(-3000), Time.milliseconds(3000))
                .process(new ProcessJoinFunction<Event, Event, String>() {
                    @Override
                    public void processElement(Event left, Event right, ProcessJoinFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        String s = left.toString() + "++++" + right.toString();
                        out.collect(s);
                    }
                });

        process.print();


        env.execute();
    }
}
