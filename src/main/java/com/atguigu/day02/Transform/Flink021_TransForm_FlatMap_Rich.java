package com.atguigu.day02.Transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lds
 * @date 2021-11-25  0:22
 */
public class Flink021_TransForm_FlatMap_Rich {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // env.setParallelism(8);
        //TODO 获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input");
        //DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9998);
        //使用flatMap方法对每一行数据按照空格切分,获取每个单词
        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap(new MyRichFlatMap());
        flatMap.print();
        env.execute();
    }

    //自定义一个类继承富函数抽象类
    public static class MyRichFlatMap extends RichFlatMapFunction<String,String> {

        @Override
        public void flatMap(String value, Collector out) throws Exception {
            String[] split = value.split(" ");
            System.out.println(this.getRuntimeContext().getTaskNameWithSubtasks());
            for (String s : split) {
                out.collect(s);
            }
        }

        /**
         * 默认生命周期,调用时间,最先调用,每个并行度调用一次
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open............");
        }

        /**
         * 默认生命周期,调用时间,最后调用,每个并行度调用一次(读文件时,每个并行度调用2次)==>因为:确保文件内容读完,保证文件读完
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("close............");

        }
    }
}
