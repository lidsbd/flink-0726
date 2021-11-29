import org.apache.flink.streaming.api.scala._

/**
  * @author lds
  * @date 2021-11-19  20:38
  */
object Object {
  def main(args: Array[String]): Unit = {
    //1.流的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2
    env.socketTextStream("hadoop102", 9999)
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(_._1)
      .sum(1)
      .print()
    env.execute()
  }
}
