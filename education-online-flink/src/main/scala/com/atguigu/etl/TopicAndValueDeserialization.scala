package com.atguigu.etl

import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class TopicAndValueDeserialization extends KafkaDeserializationSchema[TopicAndValue]{
  //是否是流的最后一条数据
  override def isEndOfStream(t: TopicAndValue): Boolean = false

  //反序列化为需要的数据类型
  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): TopicAndValue =
                                TopicAndValue(consumerRecord.topic(),new String(consumerRecord.value(),"utf-8"))

  //告诉flink数据类型
  override def getProducedType: TypeInformation[TopicAndValue] = TypeInformation.of(new TypeHint[TopicAndValue] {})
}
