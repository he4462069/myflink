package source

import bean.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.immutable
import scala.util.Random

class MySource extends SourceFunction[SensorReading]{

  //数据源是否在正常运行
  var isRunning:Boolean= true;

  val rand = new Random()


  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    var curTemp= 1 to 10 map (x => (s"sensor_$x", 65 + rand.nextGaussian() * 20))
    while (isRunning) {
      curTemp = curTemp.map(x =>(x._1,x._2+rand.nextGaussian()))

      val curTime = System.currentTimeMillis()

      curTemp.foreach{t =>
        ctx.collect(SensorReading(t._1,curTime,t._2))
      }

      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
