package bigdata.window;

import bigdata.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * @Author Administrator
 * @Date 2022/7/4 14:00
 * @Version 1.0
 * Desc:
 *  全局窗口（Global Windows） #
 *  全局窗口的 assigner 将拥有相同 key 的所有数据分发到一个全局窗口。
 *  这样的窗口模式仅在你指定了自定义的 trigger 时有用。
 *  否则，计算不会发生，因为全局窗口没有天然的终点去触发其中积累的数据。
 */
public class Window_Assigners_GlobalWindows {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> ds1 = env.fromElements(new Event("zhangsan", "/url", 1656997370000l),
                new Event("lisi", "/url", 1656997371000l),
                new Event("wangwu", "/url", 1656997372000l),
                new Event("maliu", "/url", 1656997373000l),
                new Event("zhangsan", "/url", 1656997374000l)
        );

        SingleOutputStreamOperator<Event> eventDs1 = ds1.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));


        KeyedStream<Event, String> keyedStream = eventDs1.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        });


        WindowedStream<Event, String, GlobalWindow> window = keyedStream.window(GlobalWindows.create());


        env.execute();
    }
}
