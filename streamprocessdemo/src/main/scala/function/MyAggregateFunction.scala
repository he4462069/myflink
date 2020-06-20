package function

import bean.SensorReading
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

class MyAggregateFunction extends AggregateFunction[SensorReading,(String,Double,Int),(String,Double)]{

  //创建一个累加器
  override def createAccumulator(): (String,Double, Int) = ("",0.0,0)
  //向累加器中累加数据
  override def add(in: SensorReading, acc: (String,Double, Int)): (String,Double, Int) = (in.id,in.temperature+acc._2,acc._3+1)

  //merge各个累加器中的数据
  override def merge(acc: (String,Double, Int), acc1: (String,Double, Int)): (String,Double, Int) = (acc._1,acc1._2+acc._2,acc1._3+acc._3)
  //获取聚合结果
  override def getResult(acc: (String,Double, Int)):(String, Double) = (acc._1,acc._2/acc._3)
}

object MyAggregateFunction{
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[SensorReading] = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\sqlDemoCsv.txt")
      .map { data =>
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      }

    val value: DataStream[(String, Double)] = inputStream.keyBy("id").timeWindow(Time.milliseconds(50)).aggregate(new MyAggregateFunction)
    value.print()
    env.execute("")
  }
}
