package bigdata.app;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Administrator
 * @Date 2022/7/3 18:09
 * @Version 1.0
 * Desc:
 *
 *  fink 程序剖析：
 *  1. 获取执行环境
 *  2. 加载和创建基础数据
 *  3. 指定数据转换
 *  4. 指定计算结果输出
 *  5. 触发程序执行
 */
public class DataStreamDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT,"8081");
        //获取执行环境
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
       // StreamExecutionEnvironment hadoop102 = StreamExecutionEnvironment.createRemoteEnvironment("hadoop102", 10003, "");
        //StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取基础数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("G:\\MyFlinkLearn\\input\\words.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1l));
                }

            }
        });
       flatMap.writeAsText("G:\\MyFlinkLearn\\Flink-DataStream\\datas\\output");

        JobExecutionResult execute = env.execute();
        long netRuntime = execute.getJobExecutionResult().getNetRuntime();
        Object accumulatorResult = execute.getAccumulatorResult("");
        System.out.println(netRuntime);
    }
}
