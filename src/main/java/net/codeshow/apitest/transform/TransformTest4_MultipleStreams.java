package net.codeshow.apitest.transform;

import net.codeshow.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2021/1/20
 */
public class TransformTest4_MultipleStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/cuiguangsong/my_files/workspace/java/FlinkStudy/src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) s -> {
            String[] fields = s.split(",");

            return new SensorReading(fields[0], Long.valueOf(fields[1]), new Double(fields[2]));
        });

//        分流操作，按照温度值，30度为界，分为两条流
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading s) {
                return (s.getTemperature() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

//        拣选
        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        highTempStream.print("highTempStream");
        lowTempStream.print("lowTempStream");

//        2.合流 connect,将高温流转换成二元组类型，与低温流连接合并之后输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map((MapFunction<SensorReading, Tuple2<String, Double>>) s -> new Tuple2<>(s.getId(), s.getTemperature()));

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowTempStream);
        DataStream<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });
        resultStream.print("resultStream");

        env.execute();
    }
}
