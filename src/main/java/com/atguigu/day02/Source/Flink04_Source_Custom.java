package com.atguigu.day02.Source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author lds
 * @date 2021-11-24  23:49
 */
public class Flink04_Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 自定义Source
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource());
        streamSource.print();
        env.execute();
    }

    public static class MySource implements SourceFunction<WaterSensor>{
        private Random random=new Random();
        private boolean isRunning=true;
        /**
         * 将想发送的数据写到run方法中
         * @param ctx SourceContext source上下文对象
         * @throws Exception
         */
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning){

                ctx.collect(new WaterSensor("sensor"+random.nextInt(100),System.currentTimeMillis(),random.nextInt(100)));
            }
        }

        /**
         *终止while循环 系统内部调佣
         */
        @Override
        public void cancel() {
            isRunning=false;
        }
    }
}
