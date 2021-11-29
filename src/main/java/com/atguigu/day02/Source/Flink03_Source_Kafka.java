package com.atguigu.day02.Source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author lds
 * @date 2021-11-24  22:59
 */
public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 从kafka获取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "bigdata");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));
        kafkaStream.setParallelism(5);
        kafkaStream.print();


        env.execute();
    }
}
