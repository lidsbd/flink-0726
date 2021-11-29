package com.atguigu.day02.Transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author lds
 * @date 2021-11-25  0:22
 */
public class Flink02_TransForm_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 从文件中获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");
        //使用flatMap方法对每一行数据按照空格切分,获取每个单词
        SingleOutputStreamOperator<String> flatMapStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });

        flatMapStream.print();
        env.execute();
    }
}
