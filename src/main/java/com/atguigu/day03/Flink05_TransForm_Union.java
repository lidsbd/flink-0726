package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author lds
 * @date 2021-11-25  1:23
 */
public class Flink05_TransForm_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //TODO 获取数据
        DataStreamSource<Integer> dataStreamSource1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> dataStreamSource2 = env.fromElements(1, 2, 3, 4, 5,6,7);
        DataStreamSource<Integer> dataStreamSource3 = env.fromElements(9,8,7,1, 2, 3, 4, 5);
//        DataStreamSource<String> stringDataStreamSource = env.fromElements("a", "b", "c", "d", "e");
        //TODO Union 流数据类型必需一样 疑问:数据先后,union后的流是怎样的
        DataStream<Integer> union = dataStreamSource1.union(dataStreamSource2, dataStreamSource3, dataStreamSource1);
        SingleOutputStreamOperator<Integer> process = union.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                out.collect(value + 2);
            }
        });
        process.print();


        env.execute();
    }
}
