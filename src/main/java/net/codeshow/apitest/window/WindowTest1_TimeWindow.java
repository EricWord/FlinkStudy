package net.codeshow.apitest.window;

import net.codeshow.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2021/1/21
 */
public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/cuiguangsong/my_files/workspace/java/FlinkStudy/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) s -> {
            String[] fields = s.split(",");

            return new SensorReading(fields[0], Long.valueOf(fields[1]), new Double(fields[2]));
        });
//        开窗测试
        dataStream.keyBy("id")
//                .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
//                .timeWindow(Time.seconds(15));
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
        .reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                return null;
            }
        });

        env.execute();
    }
}
