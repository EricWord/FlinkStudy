package net.codeshow.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2021/1/20
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从文件读取数据
        DataStream<String> dataStream = env.readTextFile("/Users/cuiguangsong/my_files/workspace/java/FlinkStudy/src/main/resources/sensor.txt");
        //打印输出
        dataStream.print();

        env.execute();


    }
}
