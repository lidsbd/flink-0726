package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lds
 * @date 2021-11-25  1:23
 */
public class Flink07_TransForm_Reduce {
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

        keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            //value1,指的是计算出来的最大值
            //value2 指的是当前的数据
            //reduce方法第一条数据,不进入此方法,后面数据来一条调用一次
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("reduce.....");
                return new WaterSensor(value1.getId(), value1.getTs(), Math.max(value1.getVc(), value2.getVc()));
            }
        }).print();

        //5 Max聚合操作,求vc最大值

        env.execute();
    }
}
