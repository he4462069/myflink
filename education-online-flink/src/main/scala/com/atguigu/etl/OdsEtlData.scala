package com.atguigu.etl

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.util.Collector


object OdsEtlData {

  val GROUP_ID ="group.id"
  val RETRIES = "retries"
  val TOPIC = "topic"
  val BOOTSTRAP_SERVERS = "bootstrap.servers"

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置checkpoint
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig

    env.enableCheckpointing(60000)
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.setCheckpointTimeout(10000) // 设置checkpoint超时时间
    checkpointConfig.setMinPauseBetweenCheckpoints(30000) //设置checkpoint的最小间隔时间
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) //cancel的时候是否保存checkpoint

    val backend: StateBackend = new RocksDBStateBackend("hdfs://mycluster/flink/checkpoint")
    env.setStateBackend(backend)

    //重启3次间隔10秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,new Time(10)))

    import scala.collection.JavaConverters._
    val topicList: util.List[String] = params.get(TOPIC).split(",").toBuffer.asJava

    val consumerProps = new Properties()
    consumerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS))
    consumerProps.setProperty(GROUP_ID,params.get(GROUP_ID))

    //还可以反序列化为json类型，使用 JSONKeyValueDeserializationSchema
    val kafkaEventSource: FlinkKafkaConsumer010[TopicAndValue] = new FlinkKafkaConsumer010[TopicAndValue](topicList, new TopicAndValueDeserialization, consumerProps)
    kafkaEventSource.setStartFromEarliest()

    val dataStream: DataStream[TopicAndValue] = env.addSource[TopicAndValue](kafkaEventSource).filter { item =>
      //过滤出非json数据
      val value: JSONObject = ParseJsonData.getJsonData(item.value)
      value.isInstanceOf[JSONObject]
    }
    //将datastream拆分成两份，维度表写到hbase，事实表写到第二层kafka
    val outputTag: OutputTag[TopicAndValue] = new OutputTag[TopicAndValue]("HBaseSinkStream")
    val result: DataStream[TopicAndValue] = dataStream.process(new ProcessFunction[TopicAndValue, TopicAndValue] {
      override def processElement(value: TopicAndValue, ctx: ProcessFunction[TopicAndValue, TopicAndValue]#Context, out: Collector[TopicAndValue]): Unit = {
        value.topic match {
          case "based" | "basewebsite" | "membervip" => ctx.output(outputTag, value)
          case _ => out.collect(value)
        }
      }
    })
    //测输出流得到需要写入hbase 的数据
    result.getSideOutput(outputTag).addSink(new DwdHBaseSink)

    result.addSink(new FlinkKafkaProducer010[TopicAndValue](BOOTSTRAP_SERVERS, "",new DwdKafkaProducerSerializationSchema ,consumerProps))

    env.execute()


  }
}


