package bigdata.transform;

import bigdata.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Administrator
 * @Date 2022/7/5 15:56
 * @Version 1.0
 * Desc:
 *
 *   将所有分区数据放入一个分区内
 */
public class PhysicalPartition_GlobDemo {

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
        DataStream<Event> global = ds2.global();
        global.print().setParallelism(4);
        env.execute();
    }
}
