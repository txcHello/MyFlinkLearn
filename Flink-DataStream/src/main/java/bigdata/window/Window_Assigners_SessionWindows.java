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
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * @Author Administrator
 * @Date 2022/7/4 14:00
 * @Version 1.0
 * Desc:
 *  会话窗口的 assigner 会把数据按活跃的会话分组。 与滚动窗口和滑动窗口不同，会话窗口不会相互重叠，且没有固定的开始或结束时间。 会话窗口在一段时间没有收到数据之后会关闭，即在一段不活跃的间隔之后。
 *  会话窗口的 assigner 可以设置固定的会话间隔（session gap）或 用 session gap extractor 函数来动态地定义多长时间算作不活跃。 当超出了不活跃的时间段，当前的会话就会关闭，并且将接下来的数据分发到新的会话窗口。
 *
 *
 *  会话窗口并没有固定的开始或结束时间，所以它的计算方法与滑动窗口和滚动窗口不同。在 Flink 内部，会话窗口的算子会为每一条数据创建一个窗口， 然后将距离不超过预设间隔的窗口合并。
 *  想要让窗口可以被合并，会话窗口需要拥有支持合并的 Trigger 和 Window Function， 比如说 ReduceFunction、AggregateFunction 或 ProcessWindowFunction。
 */
public class Window_Assigners_SessionWindows {

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

        // 设置了固定间隔的 event-time 会话窗口
        WindowedStream<Event, String, TimeWindow> eventTimeWindow = keyedStream.window(EventTimeSessionWindows.withGap(Time.minutes(10)));

        // 设置了动态间隔的 event-time 会话窗口
        WindowedStream<Event, String, TimeWindow> eventTimeWindowWithDynamicGap = keyedStream.window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Event>() {
            @Override
            public long extract(Event element) {
                return  element.timestamp;
            }
        }));

        WindowedStream<Event, String, TimeWindow> eventTimeWindowWithOffset = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2),Time.seconds(-8)));

        env.execute();
    }
}
