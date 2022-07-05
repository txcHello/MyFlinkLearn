package bigdata.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * @Author Administrator
 * @Date 2022/7/4 1:01
 * @Version 1.0
 * Desc:
 */
public class SourceOfKafkaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkasource  = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092")
                // 订阅topic  提供三种  订阅topic 方式
                // 1.Topic 列表，订阅 Topic 列表中所有 Partition 的消息：
                // .setTopics("topic-a", "topic-b");
                // 2.  正则表达式匹配，订阅与正则表达式所匹配的 Topic 下的所有 Partition：
                //  KafkaSource.builder().setTopicPattern("topic.*");
                // 3.  指定分区Partition 列表，订阅指定的 Partition：
                /*
                final HashSet<TopicPartition> partitionSet = new HashSet<>(Arrays.asList(
                   new TopicPartition("topic-a", 0),    // Partition 0 of topic "topic-a"
                   new TopicPartition("topic-b", 5)));  // Partition 5 of topic "topic-b"
                   KafkaSource.builder().setPartitions(partitionSet);
                 */
                .setTopics("ods_base_db_m")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafka_source = env.fromSource(kafkasource, WatermarkStrategy.noWatermarks(), "kafka source");
        kafka_source.print();

        env.execute();
    }
}
