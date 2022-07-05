package bigdata.source;

/**
 * @Author Administrator
 * @Date 2022/7/4 0:44
 * @Version 1.0
 * Desc:
 *  https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/sources/
 *  一个数据 source 包括三个核心组件：分片（Splits）、分片枚举器（SplitEnumerator） 以及 源阅读器（SourceReader）。
 *
 *  分片（Split） 是对一部分 source 数据的包装，如一个文件或者日志分区。分片是 source 进行任务分配和数据并行读取的基本粒度。
 *
 * 源阅读器（SourceReader） 会请求分片并进行处理，例如读取分片所表示的文件或日志分区。SourceReader 在 TaskManagers 上的 SourceOperators 并行运行，并产生并行的事件流/记录流。
 *
 * 分片枚举器（SplitEnumerator） 会生成分片并将它们分配给 SourceReader。该组件在 JobManager 上以单并行度运行，负责对未分配的分片进行维护，并以均衡的方式将其分配给 reader。
 */
public class DataSourceDemo {
    public static void main(String[] args) {

    }
}
