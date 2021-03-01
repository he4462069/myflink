package com.atguigu.etl

import com.alibaba.fastjson.{JSON, JSONObject}

object ParseJsonData {
  def getJsonData(value: String): JSONObject = {
    try {
      JSON.parseObject(value)
    } catch {
      case e: Exception => null
    }
  }

}
