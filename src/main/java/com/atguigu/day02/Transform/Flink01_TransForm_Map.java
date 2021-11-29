package com.atguigu.day02.Transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lds
 * @date 2021-11-25  0:22
 */
public class Flink01_TransForm_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        //TODO 从流中获取数据
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5);
        streamSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                System.out.println("================");
                return integer*2;
            }
        }).print();
        env.execute();
    }
}
