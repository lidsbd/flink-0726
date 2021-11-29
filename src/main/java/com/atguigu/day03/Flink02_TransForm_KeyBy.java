package com.atguigu.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lds
 * @date 2021-11-25  1:23
 */
public class Flink02_TransForm_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(6);
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
        });//.setParallelism(6);
        KeyedStream<String, String> keyedStream = flatMap.keyBy(r -> r);
        DataStreamSink<String> keyBy = keyedStream.print("keyBy");
        flatMap.print("flatMap");//.setParallelism(6);
        env.execute();
    }
}
