package net.codeshow.apitest.sink;

import net.codeshow.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2021/1/20
 */
public class SinkTest2_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/cuiguangsong/my_files/workspace/java/FlinkStudy/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) s -> {
            String[] fields = s.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), new Double(fields[2]));
        });

//        定义jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();
        dataStream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute();
    }

    //    自定义RedisMapper
    public static class MyRedisMapper implements RedisMapper<SensorReading> {

        //        定义保存数据到redis的命令
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading data) {
            return data.getId();
        }

        @Override
        public String getValueFromData(SensorReading data) {
            return data.getTemperature().toString();
        }
    }
}
