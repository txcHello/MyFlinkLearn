package bigdata.window;

import bigdata.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author Administrator
 * @Date 2022/7/4 14:00
 * @Version 1.0
 * Desc:
 *  滚动窗口的 assigner 分发元素到指定大小的窗口。滚动窗口的大小是固定的，且各自范围之间不重叠。 比如说，如果你指定了滚动窗口的大小为 5 分钟，那么每 5 分钟就会有一个窗口被计算，且一个新的窗口被创建（如下图所示）。
 */
public class Window_Assigners_TumblingWindows_AggregateFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> ds1 = env.fromElements(new Event("zhangsan", "/url", 1656997370000l),
                new Event("lisi", "/url", 1656997371000l),
                new Event("wangwu", "/url", 1656997372000l),
                new Event("maliu", "/url", 1656997373000l),
                new Event("zhangsan", "/app", 1656997374000l)
        );

        SingleOutputStreamOperator<Event> eventDs1 = ds1.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
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


        WindowedStream<Event, String, TimeWindow> eventWindow = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(6)));

        SingleOutputStreamOperator<Tuple2<String, Long>> aggregate = eventWindow.aggregate(new AggregateFunction<Event, Tuple2<String, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> createAccumulator() {
                return new Tuple2<>("", 0l);
            }

            @Override
            public Tuple2<String, Long> add(Event value, Tuple2<String, Long> accumulator) {
                return Tuple2.of(value.user, 1l + accumulator.f1);
            }

            @Override
            public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                return accumulator;
            }

            @Override
            public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                return Tuple2.of(a.f0, a.f1 + b.f1);
            }
        }, new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {

                long start = context.window().getStart();
                long end = context.window().getEnd();

                for (Tuple2<String, Long> element : elements) {

                    out.collect(  Tuple2. of("[ " + start  + "----" + end +"  ]" + element.f0 ,element.f1) );
                }
            }
        });


        aggregate.print();


        env.execute();
    }
}
