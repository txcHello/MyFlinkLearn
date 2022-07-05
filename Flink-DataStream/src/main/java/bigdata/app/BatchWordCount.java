package bigdata.app;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.CompletableFuture;

/**
 * @Author Administrator
 * @Date 2022/7/3 6:05
 * @Version 1.0
 * Desc:
 *  批处理
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {

        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> wordSource = env.readTextFile("G:\\MyFlinkLearn\\input\\words.txt");

        FlatMapOperator<String, Tuple2<String, Integer>> wordTupleDs = wordSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(",");

                for (String s1 : split) {
                    collector.collect(Tuple2.of(s1, 1));
                }
            }
        });

        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = wordTupleDs.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.aggregate(Aggregations.SUM,1);
        sum.print();

    env.execute();


    }

}
