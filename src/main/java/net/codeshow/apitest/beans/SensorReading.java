package net.codeshow.apitest.beans;


/**
 * @Description
 * @Author eric
 * @Version V1.0.0
 * @Date 2021/1/20
 */

//传感器温度读数的数据类型
public class SensorReading {

    private String id;
    private Long timestamp;
    private Double temperature;

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public SensorReading() {

    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
