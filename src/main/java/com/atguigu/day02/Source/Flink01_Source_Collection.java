package com.atguigu.day02.Source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author lds
 * @date 2021-11-24  22:45
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置全局并行度为1
        env.setParallelism(1);
        //TODO 3.从集合中获取数据 集合中获取数据并行度只能为1
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        DataStreamSource<Integer> streamSource = env.fromCollection(list);
        streamSource.print();
        env.execute();
    }
}
