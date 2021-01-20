package net.codeshow.apitest.sink;

import net.codeshow.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2021/1/20
 */
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("/Users/cuiguangsong/my_files/workspace/java/FlinkStudy/src/main/resources/sensor.txt");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        //从kafka读取数据
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));
        DataStream<String> dataStream = inputStream.map((MapFunction<String, String>) s -> {
            String[] fields = s.split(",");

            return new SensorReading(fields[0], Long.valueOf(fields[1]), new Double(fields[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer011<String>("localhost:9092", "sinktest",new SimpleStringSchema()));

        env.execute();

    }
}
