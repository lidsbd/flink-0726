package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lds
 * @date 2021-11-19  19:32
 */
public class Flink02_Stream_Bounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //将全局并行度设置为1
        env.setParallelism(1);
        //2.从文件读取数据==>有界
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");
        //3按照空格将单词,单独提取出来
        SingleOutputStreamOperator<String> wordStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        //4.将每个单词组成Tuple2元组(word,1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = wordStream.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
        //5.将相同单词聚合在一起
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneStream.keyBy(0);
        //6.聚合计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        result.print();
        //
        env.execute();
    }
}
