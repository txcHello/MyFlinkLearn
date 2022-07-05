package bigdata.transform;

import bigdata.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Author Administrator
 * @Date 2022/7/4 14:00
 * @Version 1.0
 * Desc:
 *  Window Join #
 *   DataStream,DataStream → DataStream #
 *   根据指定的 key 和窗口 join 两个数据流。
 */
public class WindowJoin_Transform {

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
        DataStream<String> apply = eventDs1.join(eventDs2).where(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event value) throws Exception {
                        return value.user;
                    }
                }).equalTo(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event value) throws Exception {
                        return value.user;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Event, Event, String>() {
                    @Override
                    public String join(Event first, Event second) throws Exception {
                        return first.user + first.url + "======" + second.url + first.timestamp + ":::::::" + second.timestamp;
                    }
                });
        apply.print();
        env.execute();
    }
}
