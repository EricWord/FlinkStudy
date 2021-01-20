package net.codeshow.apitest.source;

import net.codeshow.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2021/1/20
 */
public class SourceTest1_Collection {

    public static void main(String[] args) throws Exception {
//        创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        从集合中读取数据
        List<SensorReading> dataList = Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        );
        DataStream<SensorReading> dataStream = env.fromCollection(dataList);

        DataStream<Integer> dataStream2 = env.fromElements(1, 2, 4, 67, 189);

        dataStream.print("dataStream");
        dataStream2.print("dataStream2");

        //执行
        env.execute();
    }
}
