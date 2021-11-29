package com.atguigu.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lds
 * @date 2021-11-25  1:23
 */
public class Flink03_TransForm_Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        //TODO 获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input");
        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(s);
                }
            }
        }).setParallelism(2);
        DataStream<String> shuffle = flatMap.shuffle();
        flatMap.print("原始数据").setParallelism(2);
        shuffle.print("shuffle").setParallelism(2);
        env.execute();
    }
}
