package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author lds
 * @date 2021-11-25  1:23
 */
public class Flink06_TransForm_Max {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //TODO 获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });


        //TODO 按照相同id聚合
        KeyedStream<WaterSensor, Object> keyedStream = singleOutputStreamOperator.keyBy(r -> r.getId());
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.max("vc");
        SingleOutputStreamOperator<WaterSensor> maxByResult = keyedStream.maxBy("vc", true);
        result.print("max:");
        maxByResult.print("maxBy:");

        //5 Max聚合操作,求vc最大值

        env.execute();
    }
}
