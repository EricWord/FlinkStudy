package net.codeshow.apitest.transform;

import net.codeshow.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2021/1/20
 */
public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/cuiguangsong/my_files/workspace/java/FlinkStudy/src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) s -> {
            String[] fields = s.split(",");

            return new SensorReading(fields[0], Long.valueOf(fields[1]), new Double(fields[2]));
        });

//        分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");


//        reduce 聚合，取最大的温度值，以及当前最新的时间戳
        keyedStream.reduce((ReduceFunction<SensorReading>) (s1, s2) -> new SensorReading(s1.getId(),s2.getTimestamp(),Math.max(s1.getTemperature(),s2.getTemperature())));
        env.execute();
    }
}
