package com.atguigu.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lds
 * @date 2021-11-25  1:23
 */
public class Flink01_TransForm_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 获取数据
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4);

        //过滤偶数
        streamSource.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {

                return value%2==0;
            }
        }).print();

        env.execute();
    }
}
