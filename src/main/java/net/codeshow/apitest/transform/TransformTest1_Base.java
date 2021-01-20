package net.codeshow.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2021/1/20
 */
public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/cuiguangsong/my_files/workspace/java/FlinkStudy/src/main/resources/sensor.txt");
//        1.map,把String转换成对应的字符串长度输出
        DataStream<Integer> mapStream = inputStream.map((MapFunction<String, Integer>) s -> s.length());

//        2.按照逗号分割字段
        DataStream<String> flatMapStream = inputStream.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
            String[] fields = s.split(",");
            for (String field : fields) {
                collector.collect(field);
            }
        });

        //3.filter,筛选sensor_1开头的id对应的数据
        DataStream<String> filterStream = inputStream.filter((FilterFunction<String>) s -> s.startsWith("sensor_1"));

//        打印输出
        mapStream.print("mapStream");
        flatMapStream.print("flatMapStream");
        filterStream.print("filterStream");

        env.execute();


    }
}
