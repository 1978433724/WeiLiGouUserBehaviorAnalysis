package com.foo

import org.apache.flink.cep.scala.{CEP}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCEP {
  def main(args: Array[String]): Unit = {
    //1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //显示地定义Time的类型,默认是Pro
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置并行度
    env.setParallelism(1)

    val loginStream = env.fromCollection(List(
      LoginEvent(4, "192.168.0.4", "fail", 1558430820),
      LoginEvent(4, "192.168.0.5", "success", 1558430821),
      LoginEvent(4, "192.168.0.6", "fail", 1558430821),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(1, "192.168.10.3", "fail", 1558430845) ,
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime*1000)
      .keyBy(_.userId)

    //定义一个pattern
    val loginFailPattern =Pattern.begin[LoginEvent]("begin").where(_.eventType=="fail")
      .next("next").where(_.eventType=="fail")
      .within(Time.seconds(2))

    //在keyBy之后的流匹配中定义的pattern stream
    val patternStream = CEP.pattern(loginStream,loginFailPattern)


    import scala.collection.Map
    //再从pattern stream当中获取匹配的事件流,select方法传入一个pattern select function ,
    //当检测到定义好的模式序列时就会调用
    //String指的是上面的begin,next,Iterabele指的是里面可迭代的事件
    val loginFailDataStream = patternStream.select(
      //数据都保存在pattern里面了
      (pattern: Map[String,Iterable[LoginEvent]])=>{
    ///如果没有默认值是null
        val begin = pattern.getOrElse("begin",null).iterator.next()
        val next = pattern.getOrElse("next",null).iterator.next()
        (next.userId,begin.ip,next.eventType)

      }
    )
    loginFailDataStream.print()
    env.execute("Login Fail Detect Job")
  }
}
